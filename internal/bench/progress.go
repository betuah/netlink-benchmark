package bench

import (
	"math"
	"sync/atomic"
	"time"
)

// Phase is the client step that a Progress is reporting.
type Phase int32

const (
	PhaseIdle Phase = iota
	PhaseLatency
	PhaseUpload
	PhaseDownload
	PhaseDone
	// PhaseBidir is upload and download running at the same time.
	PhaseBidir
	PhaseStream
	PhaseMTU
)

func (p Phase) String() string {
	switch p {
	case PhaseLatency:
		return "latency"
	case PhaseUpload:
		return "upload"
	case PhaseDownload:
		return "download"
	case PhaseBidir:
		return "bidir"
	case PhaseStream:
		return "stream"
	case PhaseMTU:
		return "mtu"
	case PhaseDone:
		return "done"
	}
	return "idle"
}

// Progress exposes live counters of a Run. The measuring goroutines only
// touch atomics; a renderer calls Snapshot from its own goroutine.
// The zero value is ready to use, and a nil *Progress records nothing.
type Progress struct {
	phase  atomic.Int32
	bidir  atomic.Bool
	lat    latencyCounters
	up     tcpCounters
	down   tcpCounters
	stream streamCounters
	mtu    mtuCounters
}

type span struct {
	start atomic.Int64 // unix nano, 0 = not started
	end   atomic.Int64 // unix nano, 0 = still running
}

func (s *span) begin()  { s.start.Store(time.Now().UnixNano()) }
func (s *span) finish() { s.end.Store(time.Now().UnixNano()) }

func (s *span) read(now time.Time) (started, finished bool, elapsed time.Duration) {
	st := s.start.Load()
	if st == 0 {
		return false, false, 0
	}
	if en := s.end.Load(); en != 0 {
		return true, true, time.Duration(en - st)
	}
	return true, false, time.Duration(now.UnixNano() - st)
}

type latencyCounters struct {
	span
	track     atomic.Pointer[probeTrack]
	sent      atomic.Int64
	recv      atomic.Int64
	lastRTT   atomic.Int64
	jitter    atomic.Uint64 // math.Float64bits of jitter ms
	hasJitter atomic.Bool
}

type probeTrack struct {
	timeout time.Duration
	sentAt  []atomic.Int64
	recvd   []atomic.Bool
}

type tcpCounters struct {
	span
	planned atomic.Int64
	streams atomic.Int32
	bytes   atomic.Uint64
}

type streamCounters struct {
	span
	planned   atomic.Int64
	bitrate   atomic.Int64
	packet    atomic.Int32
	sent      atomic.Int64
	recv      atomic.Int64
	lastRTT   atomic.Int64
	jitter    atomic.Uint64
	hasJitter atomic.Bool
	bytes     atomic.Uint64
}

type mtuCounters struct {
	span
	trying atomic.Int32
	ok     atomic.Int32
	hi     atomic.Int32
	lo     atomic.Int32
}

func (p *Progress) beginLatency(total int, timeout time.Duration) {
	if p == nil {
		return
	}
	p.lat.track.Store(&probeTrack{
		timeout: timeout,
		sentAt:  make([]atomic.Int64, total),
		recvd:   make([]atomic.Bool, total),
	})
	p.lat.begin()
	p.phase.Store(int32(PhaseLatency))
}

func (p *Progress) probeSent(seq int, at time.Time) {
	if p == nil {
		return
	}
	if t := p.lat.track.Load(); t != nil && seq < len(t.sentAt) {
		t.sentAt[seq].Store(at.UnixNano())
	}
	p.lat.sent.Add(1)
}

// probeRecv records a reply and reports whether seq was new.
func (p *Progress) probeRecv(seq uint32, rtt time.Duration) bool {
	if p == nil {
		return false
	}
	t := p.lat.track.Load()
	if t == nil || int(seq) >= len(t.recvd) || t.recvd[seq].Swap(true) {
		return false
	}
	p.lat.lastRTT.Store(int64(rtt))
	p.lat.recv.Add(1)
	return true
}

func (p *Progress) setJitter(ms float64) {
	if p == nil {
		return
	}
	p.lat.jitter.Store(math.Float64bits(ms))
	p.lat.hasJitter.Store(true)
}

func (p *Progress) endLatency() {
	if p == nil {
		return
	}
	p.lat.finish()
}

func (p *Progress) tcp(dir Direction) *tcpCounters {
	if dir == DirDownload {
		return &p.down
	}
	return &p.up
}

func phaseOf(dir Direction) Phase {
	if dir == DirDownload {
		return PhaseDownload
	}
	return PhaseUpload
}

func (p *Progress) setBidir(on bool) {
	if p == nil {
		return
	}
	p.bidir.Store(on)
}

func (p *Progress) setPhase(ph Phase) {
	if p == nil {
		return
	}
	p.phase.Store(int32(ph))
}

// beginTCP starts the counters of one bulk direction and returns its shared
// byte counter, or nil when progress is off. The caller sets the phase.
func (p *Progress) beginTCP(dir Direction, planned time.Duration, streams int) *atomic.Uint64 {
	if p == nil {
		return nil
	}
	c := p.tcp(dir)
	c.planned.Store(int64(planned))
	c.streams.Store(int32(streams))
	c.begin()
	return &c.bytes
}

func (p *Progress) endTCP(dir Direction) {
	if p == nil {
		return
	}
	p.tcp(dir).finish()
}

func (p *Progress) beginStream(planned time.Duration, bitrate int64, packet int) {
	if p == nil {
		return
	}
	p.stream.planned.Store(int64(planned))
	p.stream.bitrate.Store(bitrate)
	p.stream.packet.Store(int32(packet))
	p.stream.begin()
	p.phase.Store(int32(PhaseStream))
}

func (p *Progress) streamSent(seq uint32, at time.Time) {
	if p == nil {
		return
	}
	_ = seq
	_ = at
	p.stream.sent.Add(1)
	p.stream.bytes.Add(uint64(p.stream.packet.Load()))
}

func (p *Progress) streamRecv(seq uint32, rtt time.Duration) {
	if p == nil {
		return
	}
	_ = seq
	p.stream.recv.Add(1)
	p.stream.lastRTT.Store(int64(rtt))
}

func (p *Progress) setStreamJitter(ms float64) {
	if p == nil {
		return
	}
	p.stream.jitter.Store(math.Float64bits(ms))
	p.stream.hasJitter.Store(true)
}

func (p *Progress) endStream() {
	if p == nil {
		return
	}
	p.stream.finish()
}

func (p *Progress) beginMTU(lo, hi int) {
	if p == nil {
		return
	}
	p.mtu.lo.Store(int32(lo))
	p.mtu.hi.Store(int32(hi))
	p.mtu.begin()
	p.phase.Store(int32(PhaseMTU))
}

func (p *Progress) mtuTrying(size, ok, hi int) {
	if p == nil {
		return
	}
	p.mtu.trying.Store(int32(size))
	p.mtu.ok.Store(int32(ok))
	p.mtu.hi.Store(int32(hi))
}

func (p *Progress) endMTU() {
	if p == nil {
		return
	}
	p.mtu.finish()
}

func (p *Progress) done() {
	if p == nil {
		return
	}
	p.phase.Store(int32(PhaseDone))
}

// ProgressSnapshot is a point-in-time copy of Progress.
type ProgressSnapshot struct {
	Phase Phase
	// Bidir is true for the whole run when upload and download overlap.
	Bidir    bool
	Latency  LatencyProgress
	Upload   TCPProgress
	Download TCPProgress
	Stream   StreamProgress
	MTU      MTUProgress
}

// LatencyProgress is the UDP probe phase so far.
type LatencyProgress struct {
	Started  bool
	Finished bool
	Elapsed  time.Duration
	Total    int
	Sent     int
	Recv     int
	// Settled counts probes that got a reply or outlived ProbeTimeout;
	// Lost is the subset without a reply. In-flight probes are in neither.
	Settled   int
	Lost      int
	LastRTT   time.Duration
	JitterMs  float64
	HasJitter bool
}

// LossPct is the running packet loss over settled probes.
func (l LatencyProgress) LossPct() float64 {
	if l.Settled == 0 {
		return 0
	}
	return float64(l.Lost) / float64(l.Settled) * 100
}

// TCPProgress is one bulk direction so far.
type TCPProgress struct {
	Started  bool
	Finished bool
	Elapsed  time.Duration
	Planned  time.Duration
	Streams  int
	Bytes    uint64
}

// StreamProgress is the CBR stream phase so far.
type StreamProgress struct {
	Started   bool
	Finished  bool
	Elapsed   time.Duration
	Planned   time.Duration
	Bitrate   int64
	Packet    int
	Sent      int
	Recv      int
	Bytes     uint64
	LastRTT   time.Duration
	JitterMs  float64
	HasJitter bool
}

// LossPct is instantaneous loss over packets whose echoes have returned or
// are still in flight (in-flight counted as not yet lost).
func (s StreamProgress) LossPct() float64 {
	if s.Sent == 0 {
		return 0
	}
	lost := s.Sent - s.Recv
	if lost < 0 {
		lost = 0
	}
	return float64(lost) / float64(s.Sent) * 100
}

// AchievedBps is the receive-side bitrate so far.
func (s StreamProgress) AchievedBps() float64 {
	if s.Elapsed <= 0 {
		return 0
	}
	return float64(s.Recv*s.Packet*8) / s.Elapsed.Seconds()
}

// MTUProgress is the path MTU discovery phase so far.
type MTUProgress struct {
	Started  bool
	Finished bool
	Elapsed  time.Duration
	Trying   int
	OK       int
	Hi       int
	Lo       int
}

// Snapshot reads all counters without blocking the measurement.
func (p *Progress) Snapshot() ProgressSnapshot {
	if p == nil {
		return ProgressSnapshot{}
	}
	now := time.Now()
	s := ProgressSnapshot{Phase: Phase(p.phase.Load())}

	l := &s.Latency
	l.Started, l.Finished, l.Elapsed = p.lat.read(now)
	l.Sent = int(p.lat.sent.Load())
	l.Recv = int(p.lat.recv.Load())
	l.LastRTT = time.Duration(p.lat.lastRTT.Load())
	if p.lat.hasJitter.Load() {
		l.HasJitter = true
		l.JitterMs = math.Float64frombits(p.lat.jitter.Load())
	}
	if t := p.lat.track.Load(); t != nil {
		l.Total = len(t.sentAt)
		if l.Finished {
			l.Settled = l.Sent
			l.Lost = max(l.Sent-l.Recv, 0)
		} else {
			cutoff := now.Add(-t.timeout).UnixNano()
			for i := range t.sentAt {
				at := t.sentAt[i].Load()
				if at == 0 {
					continue
				}
				switch {
				case t.recvd[i].Load():
					l.Settled++
				case at < cutoff:
					l.Settled++
					l.Lost++
				}
			}
		}
	}

	s.Upload = p.up.snapshot(now)
	s.Download = p.down.snapshot(now)
	s.Bidir = p.bidir.Load()

	st := &s.Stream
	st.Started, st.Finished, st.Elapsed = p.stream.read(now)
	st.Planned = time.Duration(p.stream.planned.Load())
	st.Bitrate = p.stream.bitrate.Load()
	st.Packet = int(p.stream.packet.Load())
	st.Sent = int(p.stream.sent.Load())
	st.Recv = int(p.stream.recv.Load())
	st.Bytes = p.stream.bytes.Load()
	st.LastRTT = time.Duration(p.stream.lastRTT.Load())
	if p.stream.hasJitter.Load() {
		st.HasJitter = true
		st.JitterMs = math.Float64frombits(p.stream.jitter.Load())
	}

	m := &s.MTU
	m.Started, m.Finished, m.Elapsed = p.mtu.read(now)
	m.Trying = int(p.mtu.trying.Load())
	m.OK = int(p.mtu.ok.Load())
	m.Hi = int(p.mtu.hi.Load())
	m.Lo = int(p.mtu.lo.Load())
	return s
}

func (c *tcpCounters) snapshot(now time.Time) TCPProgress {
	var t TCPProgress
	t.Started, t.Finished, t.Elapsed = c.read(now)
	t.Planned = time.Duration(c.planned.Load())
	t.Streams = int(c.streams.Load())
	t.Bytes = c.bytes.Load()
	return t
}
