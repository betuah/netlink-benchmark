package bench

import (
	"context"
	"fmt"
	"math"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type streamEcho struct {
	seq  uint32
	rtt  time.Duration
	recv time.Time
}

func runStream(ctx context.Context, addr string, cfg Config) (Result, error) {
	started := time.Now()
	res := Result{
		Mode:      ModeStream,
		Target:    addr,
		Duration:  cfg.Duration.Seconds(),
		StartedAt: started,
	}
	cfg.Progress.beginStream(cfg.Duration, cfg.Bitrate, cfg.Packet)

	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return Result{}, err
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return Result{}, err
	}
	defer conn.Close()

	if err := streamHandshake(ctx, conn, cfg); err != nil {
		return Result{}, err
	}

	echoes := make(chan streamEcho, 4096)
	var dupes atomic.Int64
	seen := sync.Map{}
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
			_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			n, err := conn.Read(buf)
			if err != nil {
				continue
			}
			kind, seq, sendNano, err := decodeUDP(buf[:n])
			if err != nil || kind != cmdStreamEcho {
				continue
			}
			if _, loaded := seen.LoadOrStore(seq, true); loaded {
				dupes.Add(1)
				continue
			}
			rtt := time.Since(time.Unix(0, sendNano))
			if rtt < 0 {
				rtt = 0
			}
			now := time.Now()
			cfg.Progress.streamRecv(seq, rtt)
			if len(recent) == rttWindow {
				copy(recent, recent[1:])
				recent = recent[:rttWindow-1]
			}
			recent = append(recent, rtt)
			if j, ok := jitterMs(recent); ok {
				cfg.Progress.setStreamJitter(j)
			}
			select {
			case echoes <- streamEcho{seq: seq, rtt: rtt, recv: now}:
			default:
			}
		}
	}()

	packetBits := int64(cfg.Packet) * 8
	interval := time.Duration(float64(time.Second) * float64(packetBits) / float64(cfg.Bitrate))
	if interval < time.Microsecond {
		interval = time.Microsecond
	}

	end := time.Now().Add(cfg.Duration)
	next := time.Now()
	var seq uint32
	sendTimes := make([]time.Time, 0, int(cfg.Duration/interval)+8)
	for time.Now().Before(end) {
		if err := ctx.Err(); err != nil {
			close(done)
			wg.Wait()
			return Result{}, err
		}
		now := time.Now()
		pkt := encodeStreamData(seq, now.UnixNano(), cfg.Packet)
		if _, err := conn.Write(pkt); err != nil {
			close(done)
			wg.Wait()
			return Result{}, fmt.Errorf("stream send: %w", err)
		}
		sendTimes = append(sendTimes, now)
		cfg.Progress.streamSent(seq, now)
		seq++
		next = next.Add(interval)
		if sleep := time.Until(next); sleep > 0 {
			select {
			case <-ctx.Done():
				close(done)
				wg.Wait()
				return Result{}, ctx.Err()
			case <-time.After(sleep):
			}
		}
	}
	sent := int(seq)

	got := make(map[uint32]streamEcho, sent)
	drainUntil := time.Now().Add(DefaultStreamDrain)
drain:
	for time.Now().Before(drainUntil) {
		select {
		case <-ctx.Done():
			break drain
		case e := <-echoes:
			if _, ok := got[e.seq]; !ok && int(e.seq) < sent {
				got[e.seq] = e
			}
		case <-time.After(20 * time.Millisecond):
		}
	}
	close(done)
	wg.Wait()
collect:
	for {
		select {
		case e := <-echoes:
			if _, ok := got[e.seq]; !ok && int(e.seq) < sent {
				got[e.seq] = e
			}
		default:
			break collect
		}
	}

	st := buildStream(cfg, sent, sendTimes, got, int(dupes.Load()))
	cfg.Progress.endStream()
	cfg.Progress.done()
	res.Stream = &st
	res.FinishedAt = time.Now()
	return res, nil
}

func streamHandshake(ctx context.Context, conn *net.UDPConn, cfg Config) error {
	hello := encodeStreamHello(uint32(cfg.Duration/time.Millisecond), uint32(cfg.Packet), uint64(cfg.Bitrate))
	if _, err := conn.Write(hello); err != nil {
		return err
	}
	buf := make([]byte, 64)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, err := conn.Read(buf)
		if err != nil {
			continue
		}
		if n >= 5 && string(buf[:4]) == Magic && buf[4] == cmdStreamAck {
			return nil
		}
	}
	return fmt.Errorf("stream handshake failed: peer does not support -mode stream (upgrade npbench server)")
}

func buildStream(cfg Config, sent int, sendTimes []time.Time, got map[uint32]streamEcho, dupes int) Stream {
	lateThresh := time.Duration(cfg.LateMs) * time.Millisecond
	recv := make([]bool, sent)
	arrivals := make([]streamEcho, 0, len(got))
	for seq, e := range got {
		if int(seq) < sent {
			recv[seq] = true
		}
		arrivals = append(arrivals, e)
	}
	sort.Slice(arrivals, func(i, j int) bool {
		return arrivals[i].recv.Before(arrivals[j].recv)
	})

	var (
		rtts       []time.Duration
		late       int
		outOfOrder int
		lastSeq    = -1
		lastRecv   time.Time
		maxGap     time.Duration
	)
	for _, e := range arrivals {
		rtts = append(rtts, e.rtt)
		if e.rtt > lateThresh {
			late++
		}
		if lastSeq >= 0 && int(e.seq) < lastSeq {
			outOfOrder++
		}
		lastSeq = int(e.seq)
		if lastRecv.IsZero() {
			lastRecv = e.recv
			continue
		}
		if gap := e.recv.Sub(lastRecv); gap > maxGap {
			maxGap = gap
		}
		lastRecv = e.recv
	}

	lost := 0
	maxBurst := 0
	maxBurstMs := 0.0
	burst := 0
	burstStart := -1
	finishBurst := func() {
		if burst <= maxBurst {
			return
		}
		maxBurst = burst
		if burstStart < 0 || burstStart >= len(sendTimes) {
			return
		}
		endIdx := burstStart + burst - 1
		if endIdx >= len(sendTimes) {
			endIdx = len(sendTimes) - 1
		}
		if endIdx >= burstStart {
			maxBurstMs = sendTimes[endIdx].Sub(sendTimes[burstStart]).Seconds() * 1000
		}
	}
	for i := 0; i < sent; i++ {
		if recv[i] {
			finishBurst()
			burst = 0
			burstStart = -1
			continue
		}
		lost++
		if burst == 0 {
			burstStart = i
		}
		burst++
	}
	finishBurst()

	lossPct := 0.0
	if sent > 0 {
		lossPct = float64(lost) / float64(sent) * 100
	}
	latePct := 0.0
	if len(got) > 0 {
		latePct = float64(late) / float64(len(got)) * 100
	}
	j := 0.0
	if jj, ok := jitterMs(rtts); ok {
		j = math.Round(jj*1000) / 1000
	}

	var rttMin, rttMax, rttAvg float64
	if len(rtts) > 0 {
		min, max, sum := rtts[0], rtts[0], time.Duration(0)
		for _, r := range rtts {
			if r < min {
				min = r
			}
			if r > max {
				max = r
			}
			sum += r
		}
		rttMin = float64(min) / float64(time.Millisecond)
		rttMax = float64(max) / float64(time.Millisecond)
		rttAvg = float64(sum) / float64(len(rtts)) / float64(time.Millisecond)
	}

	elapsed := cfg.Duration.Seconds()
	achieved := 0.0
	if elapsed > 0 {
		achieved = float64(len(got)*cfg.Packet*8) / elapsed
	}

	st := Stream{
		BitrateBps:      cfg.Bitrate,
		PacketSize:      cfg.Packet,
		DurationSec:     elapsed,
		LateMs:          cfg.LateMs,
		PacketsSent:     sent,
		PacketsRecv:     len(got),
		PacketLossPct:   math.Round(lossPct*1000) / 1000,
		AchievedBitrate: math.Round(achieved),
		JitterMs:        j,
		LatePackets:     late,
		LatePct:         math.Round(latePct*1000) / 1000,
		OutOfOrder:      outOfOrder,
		Duplicates:      dupes,
		MaxBurstPackets: maxBurst,
		MaxBurstMs:      math.Round(maxBurstMs*1000) / 1000,
		MaxGapMs:        math.Round(float64(maxGap)/float64(time.Millisecond)*1000) / 1000,
		RTTMinMs:        math.Round(rttMin*1000) / 1000,
		RTTAvgMs:        math.Round(rttAvg*1000) / 1000,
		RTTMaxMs:        math.Round(rttMax*1000) / 1000,
		PerSecond:       streamPerSecond(cfg, sent, sendTimes, got, lateThresh),
	}
	st.Verdict = StreamVerdictOf(st.PacketLossPct, st.JitterMs, st.LatePct, st.MaxBurstPackets)
	return st
}

func streamPerSecond(cfg Config, sent int, sendTimes []time.Time, got map[uint32]streamEcho, lateThresh time.Duration) []StreamSecond {
	if sent == 0 || len(sendTimes) == 0 {
		return nil
	}
	start := sendTimes[0]
	secs := int(math.Ceil(cfg.Duration.Seconds()))
	if secs < 1 {
		secs = 1
	}
	type bucket struct {
		sent, recv, late int
		rtts             []time.Duration
	}
	buckets := make([]bucket, secs)
	for i, at := range sendTimes {
		s := int(at.Sub(start).Seconds())
		if s < 0 {
			s = 0
		}
		if s >= secs {
			s = secs - 1
		}
		buckets[s].sent++
		if e, ok := got[uint32(i)]; ok {
			buckets[s].recv++
			buckets[s].rtts = append(buckets[s].rtts, e.rtt)
			if e.rtt > lateThresh {
				buckets[s].late++
			}
		}
	}
	out := make([]StreamSecond, 0, secs)
	for i, b := range buckets {
		if b.sent == 0 {
			continue
		}
		lost := b.sent - b.recv
		if lost < 0 {
			lost = 0
		}
		lossPct := float64(lost) / float64(b.sent) * 100
		j := 0.0
		if jj, ok := jitterMs(b.rtts); ok {
			j = math.Round(jj*1000) / 1000
		}
		out = append(out, StreamSecond{
			Second:   i,
			Sent:     b.sent,
			Recv:     b.recv,
			Lost:     lost,
			LossPct:  math.Round(lossPct*1000) / 1000,
			JitterMs: j,
			Late:     b.late,
		})
	}
	return out
}
