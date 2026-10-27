// SPDX-License-Identifier: MPL-2.0

package singmux

import (
	"io"
	"net"
	"testing"

	"golang.org/x/net/http2"
)

func TestH2MuxWindowConnClampsInitialWindow(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	go func() {
		defer client.Close()
		_, _ = io.WriteString(client, http2.ClientPreface)
		framer := http2.NewFramer(client, nil)
		_ = framer.WriteSettings(
			http2.Setting{ID: http2.SettingEnablePush, Val: 0},
			http2.Setting{ID: http2.SettingInitialWindowSize, Val: 4 << 20},
			http2.Setting{ID: http2.SettingMaxHeaderListSize, Val: 10 << 20},
		)
		_ = framer.WriteWindowUpdate(0, 1<<30)
	}()

	conn := newH2MuxWindowConn(server, 512<<10)
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(conn, preface); err != nil || string(preface) != http2.ClientPreface {
		t.Fatalf("preface = %q, %v", preface, err)
	}
	framer := http2.NewFramer(nil, conn)
	frame, err := framer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	settings := frame.(*http2.SettingsFrame)
	want := map[http2.SettingID]uint32{
		http2.SettingEnablePush:        0,
		http2.SettingInitialWindowSize: 512 << 10,
		http2.SettingMaxHeaderListSize: 10 << 20,
	}
	for id, val := range want {
		if got, ok := settings.Value(id); !ok || got != val {
			t.Errorf("setting %v = %d, %v; want %d", id, got, ok, val)
		}
	}
	frame, err = framer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if update := frame.(*http2.WindowUpdateFrame); update.Increment != 1<<30 {
		t.Errorf("connection window update = %d, want it untouched", update.Increment)
	}
}

func TestH2MuxWindowConnDisabled(t *testing.T) {
	_, server := net.Pipe()
	defer server.Close()
	if conn := newH2MuxWindowConn(server, 0); conn != server {
		t.Fatal("window 0 must keep the carrier as is")
	}
}
