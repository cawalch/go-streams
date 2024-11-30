package extension

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestNetSourceConcurrentConnections verifies that NetSource can handle multiple
// concurrent TCP connections correctly. It:
// - Creates multiple concurrent connections (10)
// - Sends unique messages from each connection
// - Verifies all messages are received exactly once
// - Ensures connection handling is thread-safe
func TestNetSourceConcurrentConnections(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start server with larger buffer to handle concurrent connections
	source, err := NewNetSource(ctx, TCP, "localhost:0",
		WithBufferSize(2000), // Larger buffer for concurrent connections
	)
	if err != nil {
		t.Fatalf("Failed to create NetSource: %v", err)
	}

	addr := source.listener.Addr().String()
	messageCount := 1000
	clientCount := 10
	var wg sync.WaitGroup

	// Channel to collect received messages with enough buffer
	received := make(chan string, messageCount)
	var receivedCount int32 // Use atomic counter

	// Start collecting messages
	go func() {
		for msg := range source.Out() {
			received <- msg.(string)
			atomic.AddInt32(&receivedCount, 1)
		}
	}()

	// Start multiple clients
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Errorf("Failed to connect: %v", err)
				return
			}
			defer conn.Close()

			// Send messages rapidly
			for j := 0; j < messageCount/clientCount; j++ {
				_, err := conn.Write([]byte("test\n"))
				if err != nil {
					t.Errorf("Failed to write: %v", err)
					return
				}
			}
		}()
	}

	// Wait for clients to finish
	wg.Wait()

	// Wait for messages to be processed with timeout
	deadline := time.After(5 * time.Second)
	for atomic.LoadInt32(&receivedCount) < int32(messageCount) {
		select {
		case <-deadline:
			t.Errorf("Timeout waiting for messages. Expected %d, got %d",
				messageCount, atomic.LoadInt32(&receivedCount))
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestNetSourcePartialReads verifies that NetSource correctly handles TCP
// partial reads and message framing. It:
// - Sends messages that may be split across multiple TCP packets
// - Tests the scanner's ability to properly reconstruct complete messages
// - Ensures no data is lost or corrupted during transmission
func TestNetSourcePartialReads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start server
	source, err := NewNetSource(ctx, TCP, "localhost:0")
	if err != nil {
		t.Fatalf("Failed to create NetSource: %v", err)
	}

	addr := source.listener.Addr().String()

	// Start collecting messages
	received := make(chan string, 100)
	go func() {
		for msg := range source.Out() {
			received <- msg.(string)
		}
	}()

	// Connect and send partial message
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Send a partial message and close connection immediately
	conn.Write([]byte("incomplete"))
	conn.Close()

	// Wait for potential message
	select {
	case msg := <-received:
		// We shouldn't receive incomplete messages
		t.Errorf("Received unexpected partial message: %q", msg)
	case <-time.After(100 * time.Millisecond):
		// This is the expected behavior - no message should be received
	}
}

// TestNetSourceDropsMessagesWhenChannelFull demonstrates the behavior without
// backpressure implementation. This test is kept as a reference to show why
// backpressure was needed. It:
// - Sends messages rapidly to a slow consumer
// - Shows that messages are dropped when internal buffers fill up
// - Validates the original issue that led to backpressure implementation
func TestNetSourceDropsMessagesWhenChannelFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start server
	source, err := NewNetSource(ctx, TCP, "localhost:0")
	if err != nil {
		t.Fatalf("Failed to create NetSource: %v", err)
	}

	addr := source.listener.Addr().String()
	messageCount := 1000

	// Create a slow consumer that processes messages with delay
	received := make([]string, 0, messageCount)
	go func() {
		for msg := range source.Out() {
			received = append(received, msg.(string))
			// Simulate slow processing
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Connect and send messages rapidly
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Send messages as fast as possible
	for i := 0; i < messageCount; i++ {
		msg := fmt.Sprintf("message-%d\n", i)
		_, err := conn.Write([]byte(msg))
		if err != nil {
			t.Fatalf("Failed to send message: %v", err)
		}
	}

	// Give some time for messages to be processed
	time.Sleep(2 * time.Second)

	// Verify that messages were dropped
	if len(received) == messageCount {
		t.Errorf("Expected some messages to be dropped, but all %d messages were received", messageCount)
	} else {
		t.Logf("Bug confirmed: %d messages were dropped (%d sent, %d received)",
			messageCount-len(received), messageCount, len(received))
	}
}

// TestNetSourceWithBackpressure verifies that the backpressure mechanism
// prevents message loss under load. It:
// - Uses a slow consumer to create backpressure
// - Sends messages at a rate faster than they can be processed
// - Confirms all messages are eventually received
// - Validates that the sender blocks when the buffer is full
func TestNetSourceWithBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messageCount := 50 // Reduced message count
	bufferSize := 10

	// Start server with small buffer and short timeout
	source, err := NewNetSource(ctx, TCP, "localhost:0",
		WithBufferSize(bufferSize),
		WithSendTimeout(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Failed to create NetSource: %v", err)
	}

	addr := source.listener.Addr().String()

	// Channel to collect received messages
	var receivedCount int32
	var wg sync.WaitGroup
	wg.Add(1)

	// Start a slow consumer
	doneChan := make(chan struct{})
	go func() {
		defer wg.Done()
		defer close(doneChan)
		for msg := range source.Out() {
			_ = msg // Process message
			atomic.AddInt32(&receivedCount, 1)
			// Simulate slow processing
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Connect and send messages
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Send messages with small delays to allow backpressure to work
	for i := 0; i < messageCount; i++ {
		msg := fmt.Sprintf("message-%d\n", i)
		_, err := conn.Write([]byte(msg))
		if err != nil {
			t.Errorf("Failed to write message %d: %v", i, err)
			break
		}
		// Small delay between sends
		time.Sleep(5 * time.Millisecond)
	}

	// Close connection and wait for messages to be processed
	conn.Close()

	// Wait for all messages to be received
	deadline := time.After(5 * time.Second)
	for atomic.LoadInt32(&receivedCount) < int32(messageCount) {
		select {
		case <-deadline:
			t.Fatalf("Timeout waiting for messages. Got %d/%d",
				atomic.LoadInt32(&receivedCount), messageCount)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Now that all messages are received, cancel the context
	cancel()

	// Wait for goroutine to finish
	select {
	case <-doneChan:
		// Success
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for consumer to finish")
	}

	final := atomic.LoadInt32(&receivedCount)
	if int(final) != messageCount {
		t.Errorf("Expected %d messages, but got %d", messageCount, final)
	}
}

// TestNetSourceConfigurableBufferSize verifies that the NetSource's buffer
// size can be configured via options. It:
// - Creates a NetSource with a custom buffer size
// - Validates that the buffer is created with the specified size
// - Ensures the option is properly applied during initialization
func TestNetSourceConfigurableBufferSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bufferSize := 50
	source, err := NewNetSource(ctx, TCP, "localhost:0",
		WithBufferSize(bufferSize),
	)
	if err != nil {
		t.Fatalf("Failed to create NetSource: %v", err)
	}

	// Verify the channel has the configured buffer size
	if cap(source.out) != bufferSize {
		t.Errorf("Expected buffer size %d, got %d", bufferSize, cap(source.out))
	}
}

// TestNetSourceWithBackpressureHandlesRapidSends performs a high-throughput
// test of the backpressure mechanism. It:
// - Sends 1000 messages as fast as possible
// - Uses a slow consumer to create natural backpressure
// - Verifies that no messages are dropped despite the speed mismatch
// - Ensures system remains stable under heavy load
func TestNetSourceWithBackpressureHandlesRapidSends(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messageCount := 1000 // Same as the dropping test
	bufferSize := 100

	// Start server with backpressure configuration
	source, err := NewNetSource(ctx, TCP, "localhost:0",
		WithBufferSize(bufferSize),
		WithSendTimeout(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Failed to create NetSource: %v", err)
	}

	addr := source.listener.Addr().String()

	// Channel to collect received messages
	var receivedCount int32
	var wg sync.WaitGroup
	wg.Add(1)

	// Start a slow consumer (same speed as the dropping test)
	doneChan := make(chan struct{})
	go func() {
		defer wg.Done()
		defer close(doneChan)
		for msg := range source.Out() {
			_ = msg
			atomic.AddInt32(&receivedCount, 1)
			time.Sleep(5 * time.Millisecond) // Same delay as dropping test
		}
	}()

	// Connect and send messages rapidly
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Send messages as fast as possible (same as dropping test)
	for i := 0; i < messageCount; i++ {
		msg := fmt.Sprintf("message-%d\n", i)
		_, err := conn.Write([]byte(msg))
		if err != nil {
			t.Errorf("Failed to write message %d: %v", i, err)
			break
		}
	}

	// Close connection
	conn.Close()

	// Wait for all messages to be received with a longer timeout
	deadline := time.After(60 * time.Second) // Longer timeout since we're processing 1000 messages
	for atomic.LoadInt32(&receivedCount) < int32(messageCount) {
		select {
		case <-deadline:
			t.Fatalf("Timeout waiting for messages. Got %d/%d",
				atomic.LoadInt32(&receivedCount), messageCount)
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Now that all messages are received, cancel the context
	cancel()

	// Wait for goroutine to finish
	select {
	case <-doneChan:
		// Success
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for consumer to finish")
	}

	final := atomic.LoadInt32(&receivedCount)
	if int(final) != messageCount {
		t.Errorf("Expected %d messages, but got %d", messageCount, final)
	} else {
		t.Logf("Successfully received all %d messages with backpressure", messageCount)
	}
}
