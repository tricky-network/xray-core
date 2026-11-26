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
