package bench_test

import (
	"context"
	"encoding/json"
	"strings"
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

func TestConfigNormalize(t *testing.T) {
	cfg := bench.Config{Target: "100.64.0.1"}
	if err := cfg.Normalize(); err != nil {
		t.Fatal(err)
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
}

func TestFormatSummaryUsesTrafficFieldNames(t *testing.T) {
	j := 1.5
	r := bench.Result{
		Target:   "100.64.0.1:9201",
		Duration: 10,
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
	out := bench.FormatSummary(r)
	for _, want := range []string{
		"throughput_bps=", "throughput_mbps=", "jitter_ms=", "packet_loss_pct=",
		"rtt_ms min=", "success_rate=",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("summary missing %q:\n%s", want, out)
		}
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
	if _, ok := lat["jitter_ms"]; !ok {
		t.Fatal("latency missing jitter_ms")
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

	res, err := bench.Run(ctx, bench.Config{
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
}
