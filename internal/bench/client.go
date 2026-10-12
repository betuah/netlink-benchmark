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

// Run executes the configured mode against target.
// Target may be "host" or "host:port"; the default port is DefaultPort.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if err := cfg.Normalize(); err != nil {
		return Result{}, err
	}
	addr, err := resolveTarget(cfg.Target)
	if err != nil {
		return Result{}, err
	}
	switch cfg.Mode {
	case ModeStream:
		return runStream(ctx, addr, cfg)
	case ModeMTU:
		return runMTU(ctx, addr, cfg)
	default:
		return runThroughput(ctx, addr, cfg)
	}
}

func runThroughput(ctx context.Context, addr string, cfg Config) (Result, error) {
	started := time.Now()
	res := Result{
		Mode:      ModeThroughput,
		Target:    addr,
		Duration:  cfg.Duration.Seconds(),
		Direction: cfg.Direction,
		Streams:   cfg.Streams,
		StartedAt: started,
	}

	cfg.Progress.setBidir(cfg.Direction == DirBidir)
	cfg.Progress.beginLatency(cfg.Probes, cfg.ProbeTimeout)
	lat, err := probeUDP(ctx, addr, cfg)
	if err != nil {
		return Result{}, fmt.Errorf("udp probes: %w", err)
	}
	cfg.Progress.endLatency()
	res.Latency = lat

	// Each group runs its directions concurrently; groups run in order.
	var groups [][]Direction
	switch cfg.Direction {
	case DirUpload:
		groups = [][]Direction{{DirUpload}}
	case DirDownload:
		groups = [][]Direction{{DirDownload}}
	case DirBoth:
		groups = [][]Direction{{DirUpload}, {DirDownload}}
	case DirBidir:
		groups = [][]Direction{{DirUpload, DirDownload}}
	}
	for _, dirs := range groups {
		ts, err := tcpBulk(ctx, addr, cfg, dirs...)
		if err != nil {
			return Result{}, err
		}
		for _, t := range ts {
			if t.Direction == DirUpload {
				res.Upload = t
			} else {
				res.Download = t
			}
		}
	}
	if res.Upload != nil && res.Download != nil && cfg.Direction == DirBidir {
		res.Total = totalOf(res.Upload, res.Download)
	}
	cfg.Progress.done()
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
	prog := cfg.Progress
	replies := make(chan reply, cfg.Probes)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 64)
		var recent []time.Duration
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
			if prog.probeRecv(seq, rtt) {
				if len(recent) == rttWindow {
					copy(recent, recent[1:])
					recent = recent[:rttWindow-1]
				}
				recent = append(recent, rtt)
				if j, ok := jitterMs(recent); ok {
					prog.setJitter(j)
				}
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
		prog.probeSent(i, now)
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

// tcpBulk runs cfg.Streams connections for every dir at once and returns one
// Throughput per dir, in order. All connections are dialed before any header
// is sent, so every direction starts its window at the same instant.
func tcpBulk(ctx context.Context, addr string, cfg Config, dirs ...Direction) ([]*Throughput, error) {
	conns := make([]net.Conn, len(dirs)*cfg.Streams)
	dialErrs := make([]error, len(conns))
	var dialed sync.WaitGroup
	for i := range conns {
		dialed.Add(1)
		go func() {
			defer dialed.Done()
			var d net.Dialer
			conns[i], dialErrs[i] = d.DialContext(ctx, "tcp", addr)
		}()
	}
	dialed.Wait()
	for i, err := range dialErrs {
		if err != nil {
			for _, c := range conns {
				if c != nil {
					_ = c.Close()
				}
			}
			return nil, fmt.Errorf("%s: %w", dirs[i/cfg.Streams], err)
		}
	}

	lives := make([]*atomic.Uint64, len(dirs))
	for i, dir := range dirs {
		lives[i] = cfg.Progress.beginTCP(dir, cfg.Duration, cfg.Streams)
	}
	if len(dirs) > 1 {
		cfg.Progress.setPhase(PhaseBidir)
	} else {
		cfg.Progress.setPhase(phaseOf(dirs[0]))
	}

	start := time.Now()
	out := make([]*Throughput, len(dirs))
	errCh := make(chan error, len(conns))
	var all sync.WaitGroup
	for i, dir := range dirs {
		cmd := cmdUpload
		if dir == DirDownload {
			cmd = cmdDownload
		}
		var total atomic.Uint64
		var streams sync.WaitGroup
		for _, conn := range conns[i*cfg.Streams : (i+1)*cfg.Streams] {
			streams.Add(1)
			go func() {
				defer streams.Done()
				n, err := tcpStream(ctx, conn, cfg, cmd, lives[i])
				total.Add(n)
				if err != nil {
					errCh <- fmt.Errorf("%s: %w", dir, err)
				}
			}()
		}
		all.Add(1)
		go func() {
			defer all.Done()
			streams.Wait()
			end := time.Now()
			cfg.Progress.endTCP(dir)
			out[i] = throughputOf(dir, total.Load(), start, end, cfg.Streams)
		}()
	}
	all.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		return nil, err
	}
	return out, nil
}

// tcpStream runs one bulk stream on conn and closes it. live, when non-nil,
// is bumped per syscall so a renderer can sample it; the returned count is
// what the result uses.
func tcpStream(ctx context.Context, conn net.Conn, cfg Config, cmd byte, live *atomic.Uint64) (uint64, error) {
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
			if live != nil {
				live.Add(uint64(nw))
			}
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
			if live != nil {
				live.Add(uint64(nr))
			}
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
