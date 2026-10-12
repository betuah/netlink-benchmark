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

	// DefaultStreamPacket is 7×188 MPEG-TS (typical RTP/UDP video chunk).
	DefaultStreamPacket = 1316
	// DefaultBitrate is 5 Mbit/s CBR for stream mode.
	DefaultBitrate int64 = 5_000_000
	// DefaultLateMs counts a stream echo slower than this as late for playback.
	DefaultLateMs = 150
	// DefaultStreamDrain waits for late echoes after the send window ends.
	DefaultStreamDrain = 500 * time.Millisecond

	// Typical WireGuard overlay MTU and relay-shrunk MTU (IPv4).
	OverlayMTU      = 1420
	OverlayRelayMTU = 1414
)

// Mode selects which benchmark the client runs.
type Mode string

const (
	// ModeThroughput is UDP latency probes then TCP bulk (default).
	ModeThroughput Mode = "throughput"
	// ModeStream sends constant-bitrate UDP (video/RTSP-like stress).
	ModeStream Mode = "stream"
	// ModeMTU discovers the path MTU over the npbench UDP path.
	ModeMTU Mode = "mtu"
)

// Direction selects which TCP bulk phases run.
type Direction string

const (
	DirUpload   Direction = "upload"
	DirDownload Direction = "download"
	// DirBoth runs upload, then download.
	DirBoth Direction = "both"
	// DirBidir runs upload and download at the same time.
	DirBidir Direction = "bidir"
)

// StreamVerdict is the final stability label for ModeStream.
type StreamVerdict string

const (
	VerdictStable     StreamVerdict = "stable"
	VerdictDegraded   StreamVerdict = "degraded"
	VerdictUnsuitable StreamVerdict = "unsuitable"
)

// Config is a client run.
type Config struct {
	Mode          Mode
	Target        string
	Duration      time.Duration
	Streams       int
	Payload       int
	Direction     Direction
	Probes        int
	ProbeInterval time.Duration
	ProbeTimeout  time.Duration
	// Bitrate is the target CBR in bits/s for ModeStream.
	Bitrate int64
	// Packet is the UDP datagram size for ModeStream (bytes).
	Packet int
	// LateMs marks stream echoes slower than this as late (playback budget).
	LateMs int
	// Progress, when set, receives live counters while Run measures.
	Progress *Progress
}

// Normalize fills defaults and rejects empty targets.
func (c *Config) Normalize() error {
	if c.Target == "" {
		return fmt.Errorf("target is required")
	}
	if c.Mode == "" {
		c.Mode = ModeThroughput
	}
	switch c.Mode {
	case ModeThroughput, ModeStream, ModeMTU:
	default:
		return fmt.Errorf("mode must be throughput, stream, or mtu")
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
	case DirUpload, DirDownload, DirBoth, DirBidir:
	default:
		return fmt.Errorf("direction must be upload, download, both, or bidir")
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
	if c.Bitrate <= 0 {
		c.Bitrate = DefaultBitrate
	}
	if c.Packet <= 0 {
		c.Packet = DefaultStreamPacket
	}
	if c.Packet < udpPacketSize {
		return fmt.Errorf("packet size must be at least %d", udpPacketSize)
	}
	if c.Packet > maxUDPReadBuffer {
		return fmt.Errorf("packet size must be at most %d", maxUDPReadBuffer)
	}
	if c.LateMs <= 0 {
		c.LateMs = DefaultLateMs
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
	StartedAt     time.Time `json:"started_at,omitzero"`
	FinishedAt    time.Time `json:"finished_at,omitzero"`
}

// StreamSecond is one second of the CBR stream (for progress appendix / JSON).
type StreamSecond struct {
	Second   int     `json:"second"`
	Sent     int     `json:"sent"`
	Recv     int     `json:"recv"`
	Lost     int     `json:"lost"`
	LossPct  float64 `json:"loss_pct"`
	JitterMs float64 `json:"jitter_ms"`
	Late     int     `json:"late"`
}

// Stream holds ModeStream stability metrics.
type Stream struct {
	BitrateBps      int64          `json:"bitrate_bps"`
	PacketSize      int            `json:"packet_size"`
	DurationSec     float64        `json:"duration_sec"`
	LateMs          int            `json:"late_ms"`
	PacketsSent     int            `json:"packets_sent"`
	PacketsRecv     int            `json:"packets_recv"`
	PacketLossPct   float64        `json:"packet_loss_pct"`
	AchievedBitrate float64        `json:"achieved_bitrate_bps"`
	JitterMs        float64        `json:"jitter_ms"`
	LatePackets     int            `json:"late_packets"`
	LatePct         float64        `json:"late_pct"`
	OutOfOrder      int            `json:"out_of_order"`
	Duplicates      int            `json:"duplicates"`
	MaxBurstPackets int            `json:"max_burst_packets"`
	MaxBurstMs      float64        `json:"max_burst_ms"`
	MaxGapMs        float64        `json:"max_gap_ms"`
	RTTMinMs        float64        `json:"rtt_min_ms"`
	RTTAvgMs        float64        `json:"rtt_avg_ms"`
	RTTMaxMs        float64        `json:"rtt_max_ms"`
	Verdict         StreamVerdict  `json:"verdict"`
	PerSecond       []StreamSecond `json:"per_second,omitempty"`
}

// MTU holds ModeMTU discovery results.
type MTU struct {
	// UDPPayload is the largest UDP datagram (npbench payload) that got a reply.
	UDPPayload int `json:"udp_payload"`
	// IPEstimate is UDPPayload + 28 (IPv4) or +48 (IPv6); see IPVersion.
	IPEstimate int    `json:"ip_mtu_estimate"`
	IPVersion  string `json:"ip_version"`
	// DFEnabled is true when the OS accepted a Don't-Fragment sockopt.
	DFEnabled bool `json:"df_enabled"`
	// SoftProbe is true when probes ran without DF (less definitive).
	SoftProbe bool `json:"soft_probe"`
	// RecommendedOverlay is the usual WireGuard tunnel MTU (1420).
	RecommendedOverlay int `json:"recommended_overlay_mtu"`
	// RecommendedRelay is overlay MTU when a peer uses the relay (1414).
	RecommendedRelay int `json:"recommended_relay_mtu"`
	// FragmentationRisk is true when IPEstimate is below RecommendedOverlay.
	FragmentationRisk bool   `json:"fragmentation_risk"`
	Note              string `json:"note,omitempty"`
}

// Result is the full client report.
type Result struct {
	Mode      Mode        `json:"mode"`
	Target    string      `json:"target"`
	Duration  float64     `json:"duration_sec,omitempty"`
	Direction Direction   `json:"direction,omitempty"`
	Streams   int         `json:"streams,omitempty"`
	Latency   Latency     `json:"latency,omitempty"`
	Upload    *Throughput `json:"upload,omitempty"`
	Download  *Throughput `json:"download,omitempty"`
	// Total is set only for DirBidir: bytes and rates of both directions
	// summed over their shared window; Streams counts all connections.
	Total      *Throughput `json:"total,omitempty"`
	Stream     *Stream     `json:"stream,omitempty"`
	MTU        *MTU        `json:"mtu,omitempty"`
	StartedAt  time.Time   `json:"started_at"`
	FinishedAt time.Time   `json:"finished_at"`
}

// Mbps converts bytes per second to megabits per second.
func Mbps(bps float64) float64 {
	return bps * 8 / 1e6
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

func throughputOf(dir Direction, bytes uint64, start, end time.Time, streams int) *Throughput {
	sec := end.Sub(start).Seconds()
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
		StartedAt:     start,
		FinishedAt:    end,
	}
}

func totalOf(up, down *Throughput) *Throughput {
	start, end := up.StartedAt, up.FinishedAt
	if down.StartedAt.Before(start) {
		start = down.StartedAt
	}
	if down.FinishedAt.After(end) {
		end = down.FinishedAt
	}
	return &Throughput{
		Direction:     DirBidir,
		Bytes:         up.Bytes + down.Bytes,
		Elapsed:       end.Sub(start).Seconds(),
		ThroughputBps: up.ThroughputBps + down.ThroughputBps,
		Streams:       up.Streams + down.Streams,
		StartedAt:     start,
		FinishedAt:    end,
	}
}

// StreamVerdictOf applies the documented stability thresholds.
//
//	stable:     loss ≤ 0.5%, jitter ≤ 20 ms, late ≤ 1%, max burst ≤ 3 packets
//	degraded:   loss ≤ 2%,   jitter ≤ 50 ms, late ≤ 5%, max burst ≤ 10 packets
//	unsuitable: otherwise
func StreamVerdictOf(lossPct, jitterMs, latePct float64, maxBurstPackets int) StreamVerdict {
	if lossPct <= 0.5 && jitterMs <= 20 && latePct <= 1 && maxBurstPackets <= 3 {
		return VerdictStable
	}
	if lossPct <= 2 && jitterMs <= 50 && latePct <= 5 && maxBurstPackets <= 10 {
		return VerdictDegraded
	}
	return VerdictUnsuitable
}
