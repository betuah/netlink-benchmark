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
	// Stream handshake / CBR data (UDP).
	cmdStreamHello byte = 'H'
	cmdStreamAck   byte = 'A'
	cmdStreamData  byte = 'S'
	cmdStreamEcho  byte = 'T'
	// Path MTU probes (UDP).
	cmdMTUProbe byte = 'M'
	cmdMTUReply byte = 'N'

	tcpHeaderSize    = 16
	udpPacketSize    = 17
	streamHelloSize  = 21
	mtuHeaderSize    = 13
	maxUDPReadBuffer = 65535
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

// streamHello: Magic[4] 'H' DurationMs[4] PacketSize[4] BitrateBps[8]
func encodeStreamHello(durationMs, packetSize uint32, bitrateBps uint64) []byte {
	buf := make([]byte, streamHelloSize)
	copy(buf[:4], Magic)
	buf[4] = cmdStreamHello
	binary.BigEndian.PutUint32(buf[5:9], durationMs)
	binary.BigEndian.PutUint32(buf[9:13], packetSize)
	binary.BigEndian.PutUint64(buf[13:21], bitrateBps)
	return buf
}

func decodeStreamHello(b []byte) (durationMs, packetSize uint32, bitrateBps uint64, err error) {
	if len(b) < streamHelloSize {
		return 0, 0, 0, fmt.Errorf("short stream hello")
	}
	if string(b[:4]) != Magic || b[4] != cmdStreamHello {
		return 0, 0, 0, fmt.Errorf("not a stream hello")
	}
	durationMs = binary.BigEndian.Uint32(b[5:9])
	packetSize = binary.BigEndian.Uint32(b[9:13])
	bitrateBps = binary.BigEndian.Uint64(b[13:21])
	return durationMs, packetSize, bitrateBps, nil
}

func encodeStreamAck() []byte {
	buf := make([]byte, 5)
	copy(buf[:4], Magic)
	buf[4] = cmdStreamAck
	return buf
}

// encodeStreamData builds a CBR datagram of exactly size bytes (header + pad).
func encodeStreamData(seq uint32, sendNano int64, size int) []byte {
	if size < udpPacketSize {
		size = udpPacketSize
	}
	buf := make([]byte, size)
	copy(buf[:4], Magic)
	buf[4] = cmdStreamData
	binary.BigEndian.PutUint32(buf[5:9], seq)
	binary.BigEndian.PutUint64(buf[9:17], uint64(sendNano))
	return buf
}

// mtuPacket: Magic[4] 'M'/'N' Seq[4] TotalLen[4] + optional pad to TotalLen
func encodeMTU(kind byte, seq uint32, totalLen int) []byte {
	if totalLen < mtuHeaderSize {
		totalLen = mtuHeaderSize
	}
	buf := make([]byte, totalLen)
	copy(buf[:4], Magic)
	buf[4] = kind
	binary.BigEndian.PutUint32(buf[5:9], seq)
	binary.BigEndian.PutUint32(buf[9:13], uint32(totalLen))
	return buf
}

func decodeMTU(b []byte) (kind byte, seq uint32, totalLen uint32, err error) {
	if len(b) < mtuHeaderSize {
		return 0, 0, 0, fmt.Errorf("short mtu packet")
	}
	if string(b[:4]) != Magic {
		return 0, 0, 0, fmt.Errorf("bad magic")
	}
	kind = b[4]
	seq = binary.BigEndian.Uint32(b[5:9])
	totalLen = binary.BigEndian.Uint32(b[9:13])
	return kind, seq, totalLen, nil
}
