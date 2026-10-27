package freedom

import (
	"bytes"
	"testing"
)

func TestFragmentWriterSplitClientHello(t *testing.T) {
	payload := bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7}, 60)
	hello := append([]byte{22, 3, 1, byte(len(payload) >> 8), byte(len(payload))}, payload...)
	tail := []byte("after hello")
	var out bytes.Buffer
	w := &FragmentWriter{fragment: &Fragment{PacketsFrom: 0, PacketsTo: 1, LengthMin: 1, LengthMax: 1}, writer: &out}

	// The hello arrives in three writes, the last one carrying data that follows it.
	for _, part := range [][]byte{hello[:3], hello[3:200], append(append([]byte(nil), hello[200:]...), tail...)} {
		if n, err := w.Write(part); err != nil || n != len(part) {
			t.Fatalf("Write(%d bytes) = %d, %v", len(part), n, err)
		}
	}

	var got []byte
	rest := out.Bytes()
	for records := 0; len(rest) >= 5 && rest[0] == 22; records++ {
		size := int(rest[3])<<8 | int(rest[4])
		if size != 1 {
			t.Fatalf("record %d carries %d bytes, want 1", records, size)
		}
		got = append(got, rest[5:5+size]...)
		rest = rest[5+size:]
	}
	if !bytes.Equal(got, payload) || !bytes.Equal(rest, tail) {
		t.Fatalf("reassembled %d payload bytes, tail %q", len(got), rest)
	}
}
