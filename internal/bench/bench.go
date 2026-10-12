// Package bench measures overlay throughput and UDP RTT/loss/jitter between
// two agents. All sockets are user-space over the tunnel; nothing runs on the
// WireGuard packet path.
package bench

import (
	"fmt"
	"math"
	"time"
)

const (
	// DefaultPort is the TCP/UDP listen port for npbench.
	DefaultPort = 9201
	// Magic identifies the npbench wire framing.
	Magic = "NPB1"
	// DefaultPayload is the TCP write chunk size.
	DefaultPayload = 64 * 1024
	// DefaultProbes is how many UDP echo samples the client sends.
	DefaultProbes = 50
	// DefaultProbeInterval spaces UDP probes so the path is not flooded.
	DefaultProbeInterval = 20 * time.Millisecond
	// DefaultProbeTimeout is the wait for one UDP reply.
	DefaultProbeTimeout = 500 * time.Millisecond
)

// Direction selects which TCP bulk phases run.
type Direction string

const (
	DirUpload   Direction = "upload"
	DirDownload Direction = "download"
	DirBoth     Direction = "both"
)

// Config is a client run.
type Config struct {
	Target        string
	Duration      time.Duration
	Streams       int
	Payload       int
	Direction     Direction
	Probes        int
	ProbeInterval time.Duration
	ProbeTimeout  time.Duration
}

// Normalize fills defaults and rejects empty targets.
func (c *Config) Normalize() error {
	if c.Target == "" {
		return fmt.Errorf("target is required")
	}
	if c.Duration <= 0 {
		c.Duration = 10 * time.Second
	}
	if c.Streams <= 0 {
		c.Streams = 1
	}
	if c.Payload <= 0 {
		c.Payload = DefaultPayload
	}
	if c.Direction == "" {
		c.Direction = DirBoth
	}
	switch c.Direction {
	case DirUpload, DirDownload, DirBoth:
	default:
		return fmt.Errorf("direction must be upload, download, or both")
	}
	if c.Probes <= 0 {
		c.Probes = DefaultProbes
	}
	if c.ProbeInterval <= 0 {
		c.ProbeInterval = DefaultProbeInterval
	}
	if c.ProbeTimeout <= 0 {
		c.ProbeTimeout = DefaultProbeTimeout
	}
	return nil
}

// Latency holds UDP echo statistics.
type Latency struct {
	ProbesSent    int      `json:"probes_sent"`
	ProbesRecv    int      `json:"probes_recv"`
	PacketLossPct float64  `json:"packet_loss_pct"`
	SuccessRate   float64  `json:"success_rate"`
	RTTMinMs      float64  `json:"rtt_min_ms"`
	RTTAvgMs      float64  `json:"rtt_avg_ms"`
	RTTMaxMs      float64  `json:"rtt_max_ms"`
	JitterMs      *float64 `json:"jitter_ms,omitempty"`
}

// Throughput is one TCP bulk direction. Rates use *_bps.
// Mbps helpers are for the human summary only.
type Throughput struct {
	Direction     Direction `json:"direction"`
	Bytes         uint64    `json:"bytes"`
	Elapsed       float64   `json:"elapsed_sec"`
	ThroughputBps float64   `json:"throughput_bps"`
	Streams       int       `json:"streams"`
}

// Result is the full client report.
type Result struct {
	Target     string      `json:"target"`
	Duration   float64     `json:"duration_sec"`
	Latency    Latency     `json:"latency"`
	Upload     *Throughput `json:"upload,omitempty"`
	Download   *Throughput `json:"download,omitempty"`
	StartedAt  time.Time   `json:"started_at"`
	FinishedAt time.Time   `json:"finished_at"`
}

// Mbps converts bytes per second to megabits per second.
func Mbps(bps float64) float64 {
	return bps * 8 / 1e6
}

// FormatSummary prints a stable human-readable report.
func FormatSummary(r Result) string {
	var b []byte
	appendf := func(format string, args ...any) {
		b = append(b, fmt.Sprintf(format, args...)...)
	}
	appendf("npbench target=%s duration=%.1fs\n", r.Target, r.Duration)
	lat := r.Latency
	appendf("latency probes_sent=%d probes_recv=%d packet_loss_pct=%.2f success_rate=%.2f\n",
		lat.ProbesSent, lat.ProbesRecv, lat.PacketLossPct, lat.SuccessRate)
	if lat.ProbesRecv > 0 {
		appendf("rtt_ms min=%.3f avg=%.3f max=%.3f", lat.RTTMinMs, lat.RTTAvgMs, lat.RTTMaxMs)
		if lat.JitterMs != nil {
			appendf(" jitter_ms=%.3f", *lat.JitterMs)
		}
		appendf("\n")
	}
	if r.Upload != nil {
		appendf("upload bytes=%d elapsed_sec=%.3f throughput_bps=%.0f throughput_mbps=%.2f streams=%d\n",
			r.Upload.Bytes, r.Upload.Elapsed, r.Upload.ThroughputBps, Mbps(r.Upload.ThroughputBps), r.Upload.Streams)
	}
	if r.Download != nil {
		appendf("download bytes=%d elapsed_sec=%.3f throughput_bps=%.0f throughput_mbps=%.2f streams=%d\n",
			r.Download.Bytes, r.Download.Elapsed, r.Download.ThroughputBps, Mbps(r.Download.ThroughputBps), r.Download.Streams)
	}
	return string(b)
}

func latencyFromRTTs(sent, recv int, rtts []time.Duration) Latency {
	lat := Latency{ProbesSent: sent, ProbesRecv: recv}
	if sent > 0 {
		lat.SuccessRate = float64(recv) / float64(sent)
		lat.PacketLossPct = (1 - lat.SuccessRate) * 100
	}
	if len(rtts) == 0 {
		return lat
	}
	min := rtts[0]
	max := rtts[0]
	var sum time.Duration
	for _, rtt := range rtts {
		if rtt < min {
			min = rtt
		}
		if rtt > max {
			max = rtt
		}
		sum += rtt
	}
	lat.RTTMinMs = float64(min) / float64(time.Millisecond)
	lat.RTTMaxMs = float64(max) / float64(time.Millisecond)
	lat.RTTAvgMs = float64(sum) / float64(len(rtts)) / float64(time.Millisecond)
	if j, ok := jitterMs(rtts); ok {
		// Round lightly so JSON stays readable; RFC 3550 value is unchanged in spirit.
		j = math.Round(j*1000) / 1000
		lat.JitterMs = &j
	}
	return lat
}

func throughputOf(dir Direction, bytes uint64, elapsed time.Duration, streams int) *Throughput {
	sec := elapsed.Seconds()
	bps := 0.0
	if sec > 0 {
		bps = float64(bytes) / sec
	}
	return &Throughput{
		Direction:     dir,
		Bytes:         bytes,
		Elapsed:       sec,
		ThroughputBps: bps,
		Streams:       streams,
	}
}
