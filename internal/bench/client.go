package bench

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Run executes UDP latency probes then TCP throughput against target.
// Target may be "host" or "host:port"; the default port is DefaultPort.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if err := cfg.Normalize(); err != nil {
		return Result{}, err
	}
	addr, err := resolveTarget(cfg.Target)
	if err != nil {
		return Result{}, err
	}
	started := time.Now()
	res := Result{
		Target:    addr,
		Duration:  cfg.Duration.Seconds(),
		StartedAt: started,
	}

	lat, err := probeUDP(ctx, addr, cfg)
	if err != nil {
		return Result{}, fmt.Errorf("udp probes: %w", err)
	}
	res.Latency = lat

	switch cfg.Direction {
	case DirUpload:
		up, err := tcpBulk(ctx, addr, cfg, cmdUpload)
		if err != nil {
			return Result{}, fmt.Errorf("upload: %w", err)
		}
		res.Upload = up
	case DirDownload:
		dn, err := tcpBulk(ctx, addr, cfg, cmdDownload)
		if err != nil {
			return Result{}, fmt.Errorf("download: %w", err)
		}
		res.Download = dn
	case DirBoth:
		up, err := tcpBulk(ctx, addr, cfg, cmdUpload)
		if err != nil {
			return Result{}, fmt.Errorf("upload: %w", err)
		}
		res.Upload = up
		dn, err := tcpBulk(ctx, addr, cfg, cmdDownload)
		if err != nil {
			return Result{}, fmt.Errorf("download: %w", err)
		}
		res.Download = dn
	}
	res.FinishedAt = time.Now()
	return res, nil
}

func resolveTarget(target string) (string, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		// bare IP or hostname
		host = target
		port = fmt.Sprintf("%d", DefaultPort)
	}
	if host == "" {
		return "", fmt.Errorf("empty host")
	}
	return net.JoinHostPort(host, port), nil
}

func probeUDP(ctx context.Context, addr string, cfg Config) (Latency, error) {
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return Latency{}, err
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return Latency{}, err
	}
	defer conn.Close()

	type reply struct {
		seq uint32
		rtt time.Duration
		ok  bool
	}
	replies := make(chan reply, cfg.Probes)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 64)
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			default:
			}
			_ = conn.SetReadDeadline(time.Now().Add(cfg.ProbeTimeout))
			n, err := conn.Read(buf)
			if err != nil {
				continue
			}
			kind, seq, sendNano, err := decodeUDP(buf[:n])
			if err != nil || kind != cmdReply {
				continue
			}
			rtt := time.Since(time.Unix(0, sendNano))
			if rtt < 0 {
				rtt = 0
			}
			select {
			case replies <- reply{seq: seq, rtt: rtt, ok: true}:
			default:
			}
		}
	}()

	sentAt := make([]time.Time, cfg.Probes)
	for i := 0; i < cfg.Probes; i++ {
		if err := ctx.Err(); err != nil {
			close(done)
			wg.Wait()
			return Latency{}, err
		}
		now := time.Now()
		sentAt[i] = now
		pkt := encodeUDP(cmdProbe, uint32(i), now.UnixNano())
		if _, err := conn.Write(pkt); err != nil {
			close(done)
			wg.Wait()
			return Latency{}, err
		}
		if i+1 < cfg.Probes {
			select {
			case <-ctx.Done():
				close(done)
				wg.Wait()
				return Latency{}, ctx.Err()
			case <-time.After(cfg.ProbeInterval):
			}
		}
	}

	deadline := time.After(cfg.ProbeTimeout)
	got := make(map[uint32]time.Duration, cfg.Probes)
collect:
	for len(got) < cfg.Probes {
		select {
		case <-ctx.Done():
			break collect
		case <-deadline:
			break collect
		case r := <-replies:
			if r.ok {
				if _, exists := got[r.seq]; !exists && int(r.seq) < cfg.Probes {
					got[r.seq] = r.rtt
				}
			}
		}
	}
	close(done)
	wg.Wait()

	rtts := make([]time.Duration, 0, len(got))
	for i := 0; i < cfg.Probes; i++ {
		if rtt, ok := got[uint32(i)]; ok {
			rtts = append(rtts, rtt)
		}
	}
	return latencyFromRTTs(cfg.Probes, len(rtts), rtts), nil
}

func tcpBulk(ctx context.Context, addr string, cfg Config, cmd byte) (*Throughput, error) {
	var total atomic.Uint64
	var wg sync.WaitGroup
	errCh := make(chan error, cfg.Streams)
	start := time.Now()
	for i := 0; i < cfg.Streams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := tcpStream(ctx, addr, cfg, cmd)
			total.Add(n)
			if err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return nil, err
		}
	}
	elapsed := time.Since(start)
	dir := DirUpload
	if cmd == cmdDownload {
		dir = DirDownload
	}
	return throughputOf(dir, total.Load(), elapsed, cfg.Streams), nil
}

func tcpStream(ctx context.Context, addr string, cfg Config, cmd byte) (uint64, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	h := tcpHeader{
		Cmd:        cmd,
		DurationMs: uint32(cfg.Duration / time.Millisecond),
		Payload:    uint32(cfg.Payload),
	}
	if err := writeTCPHeader(conn, h); err != nil {
		return 0, err
	}

	buf := make([]byte, cfg.Payload)
	end := time.Now().Add(cfg.Duration)
	var n uint64
	switch cmd {
	case cmdUpload:
		_ = conn.SetWriteDeadline(end.Add(5 * time.Second))
		for time.Now().Before(end) {
			if ctx.Err() != nil {
				return n, ctx.Err()
			}
			nw, err := conn.Write(buf)
			n += uint64(nw)
			if err != nil {
				if time.Now().After(end) {
					return n, nil
				}
				return n, err
			}
		}
	case cmdDownload:
		_ = conn.SetReadDeadline(end.Add(5 * time.Second))
		for time.Now().Before(end) {
			if ctx.Err() != nil {
				return n, ctx.Err()
			}
			nr, err := conn.Read(buf)
			n += uint64(nr)
			if err != nil {
				if err == io.EOF || time.Now().After(end) {
					return n, nil
				}
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					return n, nil
				}
				return n, err
			}
		}
	default:
		return 0, fmt.Errorf("unknown cmd")
	}
	return n, nil
}
