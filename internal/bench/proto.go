package bench

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	cmdUpload   byte = 'U'
	cmdDownload byte = 'D'
	cmdProbe    byte = 'P'
	cmdReply    byte = 'R'

	tcpHeaderSize = 16
	udpPacketSize = 17
)

// tcpHeader is the first frame on a TCP bench stream.
// Magic[4] Cmd[1] Reserved[3] DurationMs[4] Payload[4]
type tcpHeader struct {
	Cmd        byte
	DurationMs uint32
	Payload    uint32
}

func writeTCPHeader(w io.Writer, h tcpHeader) error {
	var buf [tcpHeaderSize]byte
	copy(buf[:4], Magic)
	buf[4] = h.Cmd
	binary.BigEndian.PutUint32(buf[8:12], h.DurationMs)
	binary.BigEndian.PutUint32(buf[12:16], h.Payload)
	_, err := w.Write(buf[:])
	return err
}

func readTCPHeader(r io.Reader) (tcpHeader, error) {
	var buf [tcpHeaderSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return tcpHeader{}, err
	}
	if string(buf[:4]) != Magic {
		return tcpHeader{}, fmt.Errorf("bad magic")
	}
	cmd := buf[4]
	if cmd != cmdUpload && cmd != cmdDownload {
		return tcpHeader{}, fmt.Errorf("unknown cmd %q", cmd)
	}
	return tcpHeader{
		Cmd:        cmd,
		DurationMs: binary.BigEndian.Uint32(buf[8:12]),
		Payload:    binary.BigEndian.Uint32(buf[12:16]),
	}, nil
}

// udpPacket: Magic[4] Type[1] Seq[4] SendUnixNano[8]
func encodeUDP(kind byte, seq uint32, sendNano int64) []byte {
	buf := make([]byte, udpPacketSize)
	copy(buf[:4], Magic)
	buf[4] = kind
	binary.BigEndian.PutUint32(buf[5:9], seq)
	binary.BigEndian.PutUint64(buf[9:17], uint64(sendNano))
	return buf
}

func decodeUDP(b []byte) (kind byte, seq uint32, sendNano int64, err error) {
	if len(b) < udpPacketSize {
		return 0, 0, 0, fmt.Errorf("short udp packet")
	}
	if string(b[:4]) != Magic {
		return 0, 0, 0, fmt.Errorf("bad magic")
	}
	kind = b[4]
	seq = binary.BigEndian.Uint32(b[5:9])
	sendNano = int64(binary.BigEndian.Uint64(b[9:17]))
	return kind, seq, sendNano, nil
}
