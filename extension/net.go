package extension

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"time"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// ConnType represents a connection type.
type ConnType string

const (
	// TCP connection type
	TCP ConnType = "tcp"

	// UDP connection type
	UDP ConnType = "udp"
)

// NetSource represents an inbound network socket connector.
type NetSource struct {
	ctx         context.Context
	conn        net.Conn
	listener    net.Listener
	connType    ConnType
	out         chan any
	bufferSize  int
	sendTimeout time.Duration
}

var _ streams.Source = (*NetSource)(nil)

// NetSourceOption represents an option for configuring NetSource
type NetSourceOption func(*NetSource)

// WithBufferSize sets the buffer size for the output channel
func WithBufferSize(size int) NetSourceOption {
	return func(ns *NetSource) {
		ns.bufferSize = size
	}
}

// WithSendTimeout sets the timeout for sending messages to the output channel
func WithSendTimeout(timeout time.Duration) NetSourceOption {
	return func(ns *NetSource) {
		ns.sendTimeout = timeout
	}
}

// NewNetSource returns a new NetSource connector.
func NewNetSource(ctx context.Context, connType ConnType, address string, opts ...NetSourceOption) (*NetSource, error) {
	var err error
	var conn net.Conn
	var listener net.Listener

	source := &NetSource{
		ctx:         ctx,
		connType:    connType,
		bufferSize:  1000,         // default buffer size
		sendTimeout: time.Second,   // default timeout
	}

	// Apply options
	for _, opt := range opts {
		opt(source)
	}

	source.out = make(chan any, source.bufferSize)

	switch connType {
	case TCP:
		addr, _ := net.ResolveTCPAddr(string(connType), address)
		listener, err = net.ListenTCP(string(connType), addr)
		if err != nil {
			return nil, err
		}
		go acceptConnections(listener, source)
	case UDP:
		addr, _ := net.ResolveUDPAddr(string(connType), address)
		conn, err = net.ListenUDP(string(connType), addr)
		if err != nil {
			return nil, err
		}
		go handleConnection(conn, source)
	default:
		return nil, errors.New("invalid connection type")
	}

	source.conn = conn
	source.listener = listener

	go source.listenCtx()
	return source, nil
}

func (ns *NetSource) listenCtx() {
	<-ns.ctx.Done()

	if ns.conn != nil {
		ns.conn.Close()
	}

	if ns.listener != nil {
		ns.listener.Close()
	}

	close(ns.out)
}

// acceptConnections accepts new TCP connections.
func acceptConnections(listener net.Listener, source *NetSource) {
	for {
		// accept a new connection
		conn, err := listener.Accept()
		if err != nil {
			// Don't log if the listener was closed normally
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("listener.Accept() failed with: %s", err)
			}
			return
		}

		// handle the new connection
		go handleConnection(conn, source)
	}
}

// handleConnection handles new connections.
func handleConnection(conn net.Conn, source *NetSource) {
	log.Printf("NetSource connected on: %v", conn.LocalAddr())
	reader := bufio.NewReader(conn)
	buffer := make([]byte, 0, 4096)

	for {
		// Read into buffer
		bufferBytes, err := reader.ReadBytes('\n')
		if err != nil {
			if len(buffer) > 0 {
				// Don't send partial messages
				log.Printf("Discarding partial message of length %d", len(buffer))
			}
			if err != io.EOF {
				log.Printf("handleConnection failed with: %s", err)
			}
			break
		}

		// Append to buffer
		buffer = append(buffer, bufferBytes...)

		// Only send complete messages (ending in newline)
		if len(buffer) > 0 && buffer[len(buffer)-1] == '\n' {
			msg := string(buffer)
			
			// Keep trying to send until successful or context is done
			for {
				select {
				case source.out <- msg:
					// Message sent successfully
					buffer = buffer[:0] // Reset buffer after successful send
					goto nextMessage
				case <-time.After(source.sendTimeout):
					log.Printf("Channel send timed out after %v, retrying...", source.sendTimeout)
					// Continue the retry loop
				case <-source.ctx.Done():
					return
				}
			}
		nextMessage:
			continue
		}
	}

	log.Printf("Closing the NetSource connection %v", conn)
	conn.Close()
}

// Via streams data to a specified operator and returns it.
func (ns *NetSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(ns, operator)
	return operator
}

// Out returns the output channel of the NetSource connector.
func (ns *NetSource) Out() <-chan any {
	return ns.out
}

// NetSink represents an outbound network socket connector.
type NetSink struct {
	conn     net.Conn
	connType ConnType
	in       chan any
}

var _ streams.Sink = (*NetSink)(nil)

// NewNetSink returns a new NetSink connector.
func NewNetSink(connType ConnType, address string) (*NetSink, error) {
	var err error
	var conn net.Conn

	conn, err = net.DialTimeout(string(connType), address, time.Second*10)
	if err != nil {
		return nil, err
	}

	sink := &NetSink{
		conn:     conn,
		connType: connType,
		in:       make(chan any),
	}

	go sink.init()
	return sink, nil
}

// init starts the stream processing loop
func (ns *NetSink) init() {
	log.Printf("NetSink connected on: %v", ns.conn.LocalAddr())
	writer := bufio.NewWriter(ns.conn)

	for msg := range ns.in {
		switch m := msg.(type) {
		case string:
			_, err := writer.WriteString(m)
			if err == nil {
				writer.Flush()
			}
		default:
			log.Printf("NetSink unsupported message type: %T", m)
		}
	}

	log.Printf("Closing the NetSink connection %v", ns.conn)
	ns.conn.Close()
}

// In returns the input channel of the NetSink connector.
func (ns *NetSink) In() chan<- any {
	return ns.in
}
