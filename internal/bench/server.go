package bench

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Server accepts TCP bulk streams and UDP echo probes on the same address.
type Server struct {
	Addr string
	// Logf, when set, gets one line per finished TCP stream and per new UDP
	// probe source. It is never called per packet.
	Logf func(format string, args ...any)

	ln   net.Listener
	pc   net.PacketConn
	once sync.Once
}

// Listen binds TCP and UDP on addr (for example ":9201").
// When the TCP port is 0, UDP uses the same assigned port.
func (s *Server) Listen() error {
	if s.Addr == "" {
		s.Addr = fmt.Sprintf(":%d", DefaultPort)
	}
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	udpAddr := ln.Addr().String()
	if host, port, splitErr := net.SplitHostPort(udpAddr); splitErr == nil {
		if host == "::" || host == "" {
			udpAddr = net.JoinHostPort("0.0.0.0", port)
		}
	}
	pc, err := net.ListenPacket("udp", udpAddr)
	if err != nil {
		_ = ln.Close()
		return err
	}
	s.ln = ln
	s.pc = pc
	s.Addr = ln.Addr().String()
	return nil
}

// Close stops the listeners.
func (s *Server) Close() error {
	var err error
	s.once.Do(func() {
		if s.ln != nil {
			err = s.ln.Close()
		}
		if s.pc != nil {
			if e := s.pc.Close(); e != nil && err == nil {
				err = e
			}
		}
	})
	return err
}

// Serve runs until ctx is done or a listener fails fatally.
func (s *Server) Serve(ctx context.Context) error {
	if s.ln == nil || s.pc == nil {
		return fmt.Errorf("server not listening")
	}
	errCh := make(chan error, 2)
	go func() { errCh <- s.serveTCP(ctx) }()
	go func() { errCh <- s.serveUDP(ctx) }()

	select {
	case <-ctx.Done():
		_ = s.Close()
		<-errCh
		<-errCh
		return nil
	case err := <-errCh:
		_ = s.Close()
		<-errCh
		if err == nil || isClosed(err) || ctx.Err() != nil {
			return nil
		}
		return err
	}
}

func (s *Server) serveTCP(ctx context.Context) error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if isClosed(err) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		go s.handleTCP(ctx, conn)
	}
}

func (s *Server) handleTCP(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Minute))
	h, err := readTCPHeader(conn)
	if err != nil {
		return
	}
	payload := int(h.Payload)
	if payload <= 0 || payload > 4*1024*1024 {
		payload = DefaultPayload
	}
	dur := time.Duration(h.DurationMs) * time.Millisecond
	if dur <= 0 {
		dur = 10 * time.Second
	}
	buf := make([]byte, payload)
	start := time.Now()
	var n int64
	phase := DirUpload
	switch h.Cmd {
	case cmdUpload:
		deadline := time.Now().Add(dur + 5*time.Second)
		_ = conn.SetReadDeadline(deadline)
		n, _ = io.Copy(io.Discard, conn)
	case cmdDownload:
		phase = DirDownload
		_ = conn.SetWriteDeadline(time.Now().Add(dur + 5*time.Second))
		end := time.Now().Add(dur)
		for time.Now().Before(end) && ctx.Err() == nil {
			nw, err := conn.Write(buf)
			n += int64(nw)
			if err != nil {
				break
			}
		}
	}
	if s.Logf != nil {
		el := time.Since(start).Seconds()
		mbps := 0.0
		if el > 0 {
			mbps = Mbps(float64(n) / el)
		}
		s.Logf("session remote=%s phase=%s bytes=%d elapsed=%.2fs mbps=%.2f",
			conn.RemoteAddr(), phase, n, el, mbps)
	}
}

const udpSessionIdle = 10 * time.Second

func (s *Server) serveUDP(ctx context.Context) error {
	buf := make([]byte, maxUDPReadBuffer)
	var seen map[string]time.Time
	if s.Logf != nil {
		seen = make(map[string]time.Time)
	}
	for {
		_ = s.pc.SetReadDeadline(time.Now().Add(time.Second))
		n, addr, err := s.pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || isClosed(err) {
				return nil
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
		if n < 5 || string(buf[:4]) != Magic {
			continue
		}
		switch buf[4] {
		case cmdProbe:
			kind, seq, sendNano, err := decodeUDP(buf[:n])
			if err != nil || kind != cmdProbe {
				continue
			}
			reply := encodeUDP(cmdReply, seq, sendNano)
			_, _ = s.pc.WriteTo(reply, addr)
			if seen != nil {
				s.noteUDP(seen, addr.String(), "latency")
			}
		case cmdStreamHello:
			_, _ = s.pc.WriteTo(encodeStreamAck(), addr)
			if seen != nil {
				s.noteUDP(seen, addr.String(), "stream")
			}
		case cmdStreamData:
			kind, seq, sendNano, err := decodeUDP(buf[:n])
			if err != nil || kind != cmdStreamData {
				continue
			}
			// Echo header only so high CBR does not double the path load.
			_, _ = s.pc.WriteTo(encodeUDP(cmdStreamEcho, seq, sendNano), addr)
		case cmdMTUProbe:
			kind, seq, totalLen, err := decodeMTU(buf[:n])
			if err != nil || kind != cmdMTUProbe {
				continue
			}
			if int(totalLen) != n && totalLen != 0 {
				// Still reply if the datagram arrived intact enough to decode.
			}
			_, _ = s.pc.WriteTo(encodeMTU(cmdMTUReply, seq, mtuHeaderSize), addr)
			if seen != nil {
				s.noteUDP(seen, addr.String(), "mtu")
			}
		}
	}
}

func (s *Server) noteUDP(seen map[string]time.Time, key, phase string) {
	now := time.Now()
	last, ok := seen[key+"|"+phase]
	seen[key+"|"+phase] = now
	if ok && now.Sub(last) < udpSessionIdle {
		return
	}
	s.Logf("session remote=%s phase=%s udp started", key, phase)
	if len(seen) > 1024 {
		for k, t := range seen {
			if now.Sub(t) >= udpSessionIdle {
				delete(seen, k)
			}
		}
	}
}

func isClosed(err error) bool {
	if err == nil {
		return false
	}
	if err == net.ErrClosed || err == io.EOF {
		return true
	}
	return false
}
