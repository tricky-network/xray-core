package grpc

import (
	"context"
	"io"
	stdnet "net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/grpc/encoding"
	"github.com/xtls/xray-core/transport/internet/tls"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
)

// closeCachedClients closes and forgets the gRPC clients Dial cached for
// dest.
func closeCachedClients(dest net.Destination) {
	globalDialerAccess.Lock()
	defer globalDialerAccess.Unlock()
	for key, client := range globalDialerMap {
		if key.Destination == dest {
			_ = client.Close()
			delete(globalDialerMap, key)
		}
	}
}

// The gRPC dialer completes the uTLS handshake itself, under gRPC's connect
// context, and reports its failure as a dial error. Left to gRPC's first
// write, the same failure would surface while writing the client preface.
func TestDialCompletesUTLSHandshake(t *testing.T) {
	listener, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// The peer is no TLS server: it answers the ClientHello in plain HTTP.
	// Every accepted connection is closed by its handler, and the test ends
	// only after the accept loop and all handlers have returned.
	var handlers sync.WaitGroup
	acceptDone := make(chan struct{})
	t.Cleanup(func() {
		_ = listener.Close()
		<-acceptDone
		handlers.Wait()
	})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				if _, err := conn.Read(make([]byte, 512)); err != nil {
					return
				}
				_, _ = io.WriteString(conn, "HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n")
			}()
		}
	}()

	dest := net.TCPDestination(net.LocalHostIP, net.Port(listener.Addr().(*stdnet.TCPAddr).Port))
	t.Cleanup(func() { closeCachedClients(dest) })
	streamSettings := &internet.MemoryStreamConfig{
		ProtocolName:     protocolName,
		ProtocolSettings: &Config{ServiceName: "not-tls"},
		SecurityType:     "tls",
		SecuritySettings: &tls.Config{ServerName: "localhost", Fingerprint: "chrome"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = Dial(ctx, dest, streamSettings)
	if err == nil {
		t.Fatal("dial through a peer that does not speak TLS succeeded")
	}
	if !strings.Contains(err.Error(), "Error while dialing") || !strings.Contains(err.Error(), "first record does not look like a TLS handshake") {
		t.Fatalf("the TLS failure was not reported by the dialer: %v", err)
	}
}

// A static stream window turns gRPC's dynamic window off, and without its
// own option the connection window then stays at 64 KiB: every stream of the
// connection shares 64 KiB per round trip. The dialer opens the connection
// window to the dynamic window's ceiling, so the stream window alone bounds
// what a slow reader leaves buffered.
func TestDialOpensConnectionWindowForStaticStreamWindow(t *testing.T) {
	const streamWindow = 1 << 20
	listener, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	type windows struct {
		stream, connIncrement uint32
		err                   error
	}
	seen := make(chan windows, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			seen <- windows{err: err}
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		var got windows
		got.err = readClientWindows(conn, &got.stream, &got.connIncrement)
		seen <- got
	}()
	t.Cleanup(func() { _ = listener.Close() })

	dest := net.TCPDestination(net.LocalHostIP, net.Port(listener.Addr().(*stdnet.TCPAddr).Port))
	t.Cleanup(func() { closeCachedClients(dest) })
	streamSettings := &internet.MemoryStreamConfig{
		ProtocolName:     protocolName,
		ProtocolSettings: &Config{ServiceName: "svc", InitialWindowsSize: streamWindow},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dialDone := make(chan struct{})
	go func() {
		defer close(dialDone)
		if conn, err := Dial(ctx, dest, streamSettings); err == nil {
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() { cancel(); <-dialDone })

	got := <-seen
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.stream != streamWindow {
		t.Fatalf("stream window %d, want %d", got.stream, streamWindow)
	}
	if want := uint32(connWindowCeiling - 65535); got.connIncrement != want {
		t.Fatalf("connection window increment %d, want %d", got.connIncrement, want)
	}
}

// readClientWindows reads a gRPC client's HTTP/2 preface up to its first
// HEADERS frame and returns the INITIAL_WINDOW_SIZE it announced and the
// increment of its connection-level WINDOW_UPDATE.
func readClientWindows(conn stdnet.Conn, stream, connIncrement *uint32) error {
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(conn, preface); err != nil {
		return err
	}
	framer := http2.NewFramer(conn, conn)
	if err := framer.WriteSettings(); err != nil {
		return err
	}
	for {
		frame, err := framer.ReadFrame()
		if err != nil {
			return err
		}
		switch f := frame.(type) {
		case *http2.SettingsFrame:
			if f.IsAck() {
				continue
			}
			if v, ok := f.Value(http2.SettingInitialWindowSize); ok {
				*stream = v
			}
			if err := framer.WriteSettingsAck(); err != nil {
				return err
			}
		case *http2.WindowUpdateFrame:
			if f.StreamID == 0 {
				*connIncrement += f.Increment
			}
		case *http2.HeadersFrame:
			return nil
		}
	}
}

type holdStreamServer struct {
	encoding.UnimplementedGRPCServiceServer
	started   chan struct{}
	cancelled chan struct{}
}

func (s *holdStreamServer) Tun(stream encoding.GRPCService_TunServer) error {
	close(s.started)
	<-stream.Context().Done()
	close(s.cancelled)
	return nil
}

func (s *holdStreamServer) TunMulti(stream encoding.GRPCService_TunMultiServer) error {
	close(s.started)
	<-stream.Context().Done()
	close(s.cancelled)
	return nil
}

func countGRPCStreamWatchers() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "google.golang.org/grpc.newClientStreamWithParams.func4")
}

// Closing a dialed gRPC connection must cancel its stream context even when
// the caller's Dial context stays alive and Read is never drained to EOF;
// otherwise grpc-go's stream watcher goroutine and receive buffer leak until
// the cached ClientConn shuts down.
func TestDialCloseCancelsStream(t *testing.T) {
	for _, tc := range []struct {
		name      string
		multiMode bool
	}{
		{name: "tun", multiMode: false},
		{name: "tun-multi", multiMode: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := stdnet.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := &holdStreamServer{
				started:   make(chan struct{}),
				cancelled: make(chan struct{}),
			}
			grpcServer := grpc.NewServer()
			encoding.RegisterGRPCServiceServerX(grpcServer, srv, "svc", "Tun", "TunMulti")
			serveDone := make(chan struct{})
			go func() {
				defer close(serveDone)
				_ = grpcServer.Serve(listener)
			}()
			t.Cleanup(func() {
				grpcServer.Stop()
				_ = listener.Close()
				<-serveDone
			})

			dest := net.TCPDestination(net.LocalHostIP, net.Port(listener.Addr().(*stdnet.TCPAddr).Port))
			t.Cleanup(func() { closeCachedClients(dest) })
			streamSettings := &internet.MemoryStreamConfig{
				ProtocolName:     protocolName,
				ProtocolSettings: &Config{ServiceName: "svc", MultiMode: tc.multiMode},
			}

			baseline := countGRPCStreamWatchers()
			conn, err := Dial(context.Background(), dest, streamSettings)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-srv.started:
			case <-time.After(5 * time.Second):
				t.Fatal("server did not receive stream")
			}

			if err := conn.Close(); err != nil {
				t.Fatalf("conn.Close: %v", err)
			}

			select {
			case <-srv.cancelled:
			case <-time.After(5 * time.Second):
				t.Fatal("closing dialed conn did not cancel server stream")
			}

			deadline := time.Now().Add(5 * time.Second)
			for countGRPCStreamWatchers() > baseline {
				if time.Now().After(deadline) {
					t.Fatalf("gRPC stream watcher goroutine leaked: got %d, baseline %d", countGRPCStreamWatchers(), baseline)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}
