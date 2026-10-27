// SPDX-License-Identifier: MPL-2.0

package singmux

import (
	"encoding/binary"
	"io"
	"net"

	"github.com/xtls/xray-core/common/platform"
	"golang.org/x/net/http2"
)

// H2MuxStreamWindowDefault caps how many unacknowledged bytes one H2MUX
// stream may have in flight towards the client.
const H2MuxStreamWindowDefault = 1 << 20

// h2MuxStreamWindow comes from XRAY_SINGMUX_H2MUX_WINDOW; 0 keeps the window
// the client advertises. Go clients advertise 4 MiB per stream and credit it
// back only as the application reads, so a slow reader buffers up to the full
// window in client memory: enough to get an iOS network extension killed.
var h2MuxStreamWindow = readH2MuxStreamWindow()

func readH2MuxStreamWindow() uint32 {
	window := platform.NewEnvFlag("xray.singmux.h2mux.window").GetValueAsInt(H2MuxStreamWindowDefault)
	if window <= 0 {
		return 0
	}
	return uint32(window)
}

// h2MuxWindowConn lowers SETTINGS_INITIAL_WINDOW_SIZE in the client's first
// SETTINGS frame before the HTTP/2 server sees it. The server then sends at
// most window bytes per stream ahead of the client's WINDOW_UPDATEs, while the
// client keeps its own larger receive window and never notices.
type h2MuxWindowConn struct {
	net.Conn
	window  uint32
	started bool
	pending []byte
}

func newH2MuxWindowConn(conn net.Conn, window uint32) net.Conn {
	if window == 0 {
		return conn
	}
	return &h2MuxWindowConn{Conn: conn, window: window}
}

func (c *h2MuxWindowConn) Read(payload []byte) (int, error) {
	if !c.started {
		c.started = true
		if err := c.readSettings(); err != nil {
			return 0, err
		}
	}
	if len(c.pending) > 0 {
		n := copy(payload, c.pending)
		c.pending = c.pending[n:]
		return n, nil
	}
	return c.Conn.Read(payload)
}

// readSettings buffers the client preface and the SETTINGS frame that must
// follow it (RFC 9113 section 3.4). Anything else is passed through untouched
// for the server to reject.
func (c *h2MuxWindowConn) readSettings() error {
	const headerLen = 9
	head := make([]byte, len(http2.ClientPreface)+headerLen)
	if _, err := io.ReadFull(c.Conn, head); err != nil {
		return err
	}
	c.pending = head
	header := head[len(http2.ClientPreface):]
	length := int(header[0])<<16 | int(header[1])<<8 | int(header[2])
	if http2.FrameType(header[3]) != http2.FrameSettings || length > int(H2MuxFrameSizeMin) {
		return nil
	}
	frame := make([]byte, len(head)+length)
	copy(frame, head)
	if _, err := io.ReadFull(c.Conn, frame[len(head):]); err != nil {
		return err
	}
	for i := len(head); i+6 <= len(frame); i += 6 {
		id := http2.SettingID(binary.BigEndian.Uint16(frame[i:]))
		if id == http2.SettingInitialWindowSize && binary.BigEndian.Uint32(frame[i+2:]) > c.window {
			binary.BigEndian.PutUint32(frame[i+2:], c.window)
		}
	}
	c.pending = frame
	return nil
}
