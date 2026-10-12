package bench_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/beesar/net-peer-benchmark/internal/bench"
)

func TestMbps(t *testing.T) {
	// 125_000_000 B/s = 1 Gbps = 1000 Mbps
	if got := bench.Mbps(125_000_000); got != 1000 {
		t.Fatalf("Mbps = %v, want 1000", got)
	}
}

func TestParseBitrate(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"5000000", 5_000_000},
		{"5M", 5_000_000},
		{"5m", 5_000_000},
		{"5Mbps", 5_000_000},
		{"5000k", 5_000_000},
		{"5000K", 5_000_000},
		{"1G", 1_000_000_000},
		{"2.5M", 2_500_000},
	}
	for _, c := range cases {
		got, err := bench.ParseBitrate(c.in)
		if err != nil || got != c.want {
			t.Errorf("ParseBitrate(%q) = %d, %v; want %d", c.in, got, err, c.want)
		}
	}
	for _, bad := range []string{"", "x", "0", "-1M", "5T"} {
		if _, err := bench.ParseBitrate(bad); err == nil {
			t.Errorf("ParseBitrate(%q) expected error", bad)
		}
	}
}

func TestStreamVerdictOf(t *testing.T) {
	cases := []struct {
		loss, jitter, late float64
		burst              int
		want               bench.StreamVerdict
	}{
		{0, 1, 0, 0, bench.VerdictStable},
		{0.5, 20, 1, 3, bench.VerdictStable},
		{1.0, 25, 2, 5, bench.VerdictDegraded},
		{2.0, 50, 5, 10, bench.VerdictDegraded},
		{2.1, 10, 0, 0, bench.VerdictUnsuitable},
		{0, 51, 0, 0, bench.VerdictUnsuitable},
		{0, 0, 6, 0, bench.VerdictUnsuitable},
		{0, 0, 0, 11, bench.VerdictUnsuitable},
	}
	for _, c := range cases {
		got := bench.StreamVerdictOf(c.loss, c.jitter, c.late, c.burst)
		if got != c.want {
			t.Errorf("StreamVerdictOf(%v,%v,%v,%d) = %s, want %s",
				c.loss, c.jitter, c.late, c.burst, got, c.want)
		}
	}
}

func TestParseMode(t *testing.T) {
	cases := []struct {
		in   string
		want bench.Mode
	}{
		{"", bench.ModeAll},
		{"all", bench.ModeAll},
		{"throughput", bench.ModeThroughput},
		{"tcp", bench.ModeThroughput},
		{"stream", bench.ModeStream},
		{"mtu", bench.ModeMTU},
	}
	for _, c := range cases {
		got, err := bench.ParseMode(c.in)
		if err != nil || got != c.want {
			t.Errorf("ParseMode(%q) = %q, %v; want %q", c.in, got, err, c.want)
		}
	}
	if _, err := bench.ParseMode("laser"); err == nil {
		t.Fatal("expected bad mode error")
	}
}

func TestConfigNormalizeModes(t *testing.T) {
	for _, m := range []bench.Mode{bench.ModeAll, bench.ModeThroughput, bench.ModeStream, bench.ModeMTU} {
		c := bench.Config{Target: "x", Mode: m}
		if err := c.Normalize(); err != nil {
			t.Fatalf("mode %q: %v", m, err)
		}
	}
	bad := bench.Config{Target: "x", Mode: "laser"}
	if err := bad.Normalize(); err == nil {
		t.Fatal("expected bad mode error")
	}
	tiny := bench.Config{Target: "x", Mode: bench.ModeStream, Packet: 8}
	if err := tiny.Normalize(); err == nil {
		t.Fatal("expected tiny packet error")
	}
}

func TestConfigNormalize(t *testing.T) {
	cfg := bench.Config{Target: "100.64.0.1"}
	if err := cfg.Normalize(); err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != bench.ModeAll {
		t.Fatalf("default mode = %q, want all", cfg.Mode)
	}
	if cfg.Duration != 10*time.Second || cfg.Streams != 1 || cfg.Direction != bench.DirBoth {
		t.Fatalf("defaults = %+v", cfg)
	}
	bad := bench.Config{Target: "x", Direction: "sideways"}
	if err := bad.Normalize(); err == nil {
		t.Fatal("expected bad direction error")
	}
	empty := bench.Config{}
	if err := empty.Normalize(); err == nil {
		t.Fatal("expected empty target error")
	}
	for _, d := range []bench.Direction{bench.DirUpload, bench.DirDownload, bench.DirBoth, bench.DirBidir} {
		c := bench.Config{Target: "x", Direction: d}
		if err := c.Normalize(); err != nil {
			t.Fatalf("direction %q: %v", d, err)
		}
	}
}

func TestBidirResultJSON(t *testing.T) {
	start := time.Unix(1_800_000_000, 0).UTC()
	up := &bench.Throughput{Direction: bench.DirUpload, Bytes: 10, Elapsed: 1, ThroughputBps: 10,
		Streams: 2, StartedAt: start, FinishedAt: start.Add(time.Second)}
	r := bench.Result{
		Target: "100.64.0.1:9201", Direction: bench.DirBidir, Streams: 2,
		Upload: up,
		Total:  &bench.Throughput{Direction: bench.DirBidir, Bytes: 30, Elapsed: 1, ThroughputBps: 30, Streams: 4},
	}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["direction"] != "bidir" {
		t.Fatalf("direction = %v", m["direction"])
	}
	total, ok := m["total"].(map[string]any)
	if !ok || total["bytes"] != float64(30) || total["streams"] != float64(4) {
		t.Fatalf("total = %v", m["total"])
	}
	if _, ok := total["started_at"]; ok {
		t.Fatal("zero started_at should be omitted")
	}
	if m["upload"].(map[string]any)["started_at"] != "2027-01-15T08:00:00Z" {
		t.Fatalf("upload started_at = %v", m["upload"])
	}

	r.Direction, r.Total = bench.DirBoth, nil
	raw, _ = json.Marshal(r)
	var seq map[string]any
	if err := json.Unmarshal(raw, &seq); err != nil || seq["total"] != nil {
		t.Fatalf("total must be omitted outside bidir: %s", raw)
	}
}

func TestResultJSONUsesTrafficFieldNames(t *testing.T) {
	j := 1.5
	r := bench.Result{
		Target:    "100.64.0.1:9201",
		Duration:  10,
		Direction: bench.DirUpload,
		Streams:   1,
		Latency: bench.Latency{
			ProbesSent: 10, ProbesRecv: 9,
			PacketLossPct: 10, SuccessRate: 0.9,
			RTTMinMs: 1, RTTAvgMs: 2, RTTMaxMs: 3,
			JitterMs: &j,
		},
		Upload: &bench.Throughput{
			Direction: bench.DirUpload, Bytes: 1_250_000,
			Elapsed: 1, ThroughputBps: 1_250_000, Streams: 1,
		},
	}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	up := m["upload"].(map[string]any)
	if _, ok := up["throughput_bps"]; !ok {
		t.Fatal("upload missing throughput_bps")
	}
	lat := m["latency"].(map[string]any)
	for _, k := range []string{"jitter_ms", "packet_loss_pct", "success_rate", "rtt_avg_ms"} {
		if _, ok := lat[k]; !ok {
			t.Fatalf("latency missing %s", k)
		}
	}
	if m["direction"] != "upload" || m["streams"] != float64(1) {
		t.Fatalf("direction/streams = %v/%v", m["direction"], m["streams"])
	}
}

func TestLoopbackRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &bench.Server{Addr: "127.0.0.1:0"}
	if err := s.Listen(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	errCh := make(chan error, 1)
	go func() { errCh <- s.Serve(ctx) }()

	prog := &bench.Progress{}
	res, err := bench.Run(ctx, bench.Config{
		Mode:          bench.ModeThroughput,
		Progress:      prog,
		Target:        s.Addr,
		Duration:      200 * time.Millisecond,
		Streams:       2,
		Payload:       32 * 1024,
		Direction:     bench.DirBoth,
		Probes:        20,
		ProbeInterval: 5 * time.Millisecond,
		ProbeTimeout:  300 * time.Millisecond,
	})
	cancel()
	<-errCh
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != bench.ModeThroughput {
		t.Fatalf("mode = %q", res.Mode)
	}
	if res.Latency.ProbesSent != 20 {
		t.Fatalf("probes_sent = %d", res.Latency.ProbesSent)
	}
	if res.Latency.ProbesRecv < 10 {
		t.Fatalf("probes_recv = %d, want at least 10 on loopback", res.Latency.ProbesRecv)
	}
	if res.Upload == nil || res.Upload.Bytes == 0 || res.Upload.ThroughputBps <= 0 {
		t.Fatalf("upload = %+v", res.Upload)
	}
	if res.Download == nil || res.Download.Bytes == 0 || res.Download.ThroughputBps <= 0 {
		t.Fatalf("download = %+v", res.Download)
	}
	if res.Upload.Streams != 2 || res.Download.Streams != 2 {
		t.Fatalf("streams upload=%d download=%d", res.Upload.Streams, res.Download.Streams)
	}
	if res.Latency.JitterMs == nil && res.Latency.ProbesRecv >= 2 {
		t.Fatal("expected jitter_ms after multiple RTT samples")
	}

	snap := prog.Snapshot()
	if snap.Phase != bench.PhaseDone {
		t.Fatalf("phase = %v, want done", snap.Phase)
	}
	if !snap.Latency.Finished || snap.Latency.Sent != 20 || snap.Latency.Total != 20 {
		t.Fatalf("latency progress = %+v", snap.Latency)
	}
	if snap.Latency.Lost != res.Latency.ProbesSent-snap.Latency.Recv {
		t.Fatalf("lost = %d, sent=%d recv=%d", snap.Latency.Lost, snap.Latency.Sent, snap.Latency.Recv)
	}
	if !snap.Upload.Finished || snap.Upload.Bytes != res.Upload.Bytes || snap.Upload.Streams != 2 {
		t.Fatalf("upload progress = %+v, result bytes=%d", snap.Upload, res.Upload.Bytes)
	}
	if !snap.Download.Finished || snap.Download.Bytes != res.Download.Bytes {
		t.Fatalf("download progress = %+v, result bytes=%d", snap.Download, res.Download.Bytes)
	}
}

func TestNilProgressSnapshot(t *testing.T) {
	var p *bench.Progress
	if s := p.Snapshot(); s.Phase != bench.PhaseIdle {
		t.Fatalf("nil snapshot phase = %v", s.Phase)
	}
}
