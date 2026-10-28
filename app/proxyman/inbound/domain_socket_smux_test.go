package inbound_test

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/app/proxyman/inbound"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/singmux"
	"github.com/xtls/xray-core/core"
	featureinbound "github.com/xtls/xray-core/features/inbound"
	"github.com/xtls/xray-core/proxy/dokodemo"
)

// An SMUX inbound behind a REALITY fallback listens on a Unix socket, so its
// H2MUX frame size must reach the Unix socket worker too: without it every
// h2mux stream of a Go client holds a 512 KiB upload buffer.
func TestDomainSocketWorkerReceivesH2MuxOptions(t *testing.T) {
	server, err := core.New(&core.Config{App: []*serial.TypedMessage{
		serial.ToTypedMessage(&dispatcher.Config{}),
		serial.ToTypedMessage(&proxyman.InboundConfig{}),
		serial.ToTypedMessage(&proxyman.OutboundConfig{}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })

	err = core.AddInboundHandler(server, &core.InboundHandlerConfig{
		Tag: "smux-unix",
		ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
			Listen:       net.NewIPOrDomain(net.DomainAddress("@xray-smux-options-test")),
			SmuxSettings: &proxyman.SmuxConfig{H2MuxMaxReadFrameSize: 16384},
		}),
		ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
			RewriteAddress:  net.NewIPOrDomain(net.LocalHostIP),
			RewritePort:     7,
			AllowedNetworks: []net.Network{net.Network_UNIX},
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	manager := server.GetFeature(featureinbound.ManagerType()).(featureinbound.Manager)
	handler, err := manager.GetHandler(context.Background(), "smux-unix")
	if err != nil {
		t.Fatal(err)
	}

	options := inbound.DomainSocketH2MuxOptions(handler.(*inbound.AlwaysOnInboundHandler))
	if len(options) != 1 {
		t.Fatalf("Unix socket workers = %d, want 1", len(options))
	}
	if want := (singmux.H2MuxOptions{MaxReadFrameSize: 16384}); options[0] != want {
		t.Fatalf("Unix socket worker H2MUX options = %#v, want %#v", options[0], want)
	}
}
