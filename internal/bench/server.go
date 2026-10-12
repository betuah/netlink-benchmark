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
	switch h.Cmd {
	case cmdUpload:
		deadline := time.Now().Add(dur + 5*time.Second)
		_ = conn.SetReadDeadline(deadline)
		_, _ = io.Copy(io.Discard, conn)
	case cmdDownload:
		_ = conn.SetWriteDeadline(time.Now().Add(dur + 5*time.Second))
		end := time.Now().Add(dur)
		for time.Now().Before(end) {
			if ctx.Err() != nil {
				return
			}
			if _, err := conn.Write(buf); err != nil {
				return
			}
		}
	}
}

func (s *Server) serveUDP(ctx context.Context) error {
	buf := make([]byte, 64)
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
		kind, seq, sendNano, err := decodeUDP(buf[:n])
		if err != nil || kind != cmdProbe {
			continue
		}
		reply := encodeUDP(cmdReply, seq, sendNano)
		_, _ = s.pc.WriteTo(reply, addr)
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
