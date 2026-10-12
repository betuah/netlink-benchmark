package report_test

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/beesar/net-peer-benchmark/internal/bench"
	"github.com/beesar/net-peer-benchmark/internal/report"
)

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{0, "0 B"},
		{1023, "1023 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{1024*1024 - 1, "1.00 MiB"},
		{10 * 1024 * 1024, "10.00 MiB"},
		{1_250_000, "1.19 MiB"},
		{5 * 1024 * 1024 * 1024, "5.00 GiB"},
		{3 << 40, "3.00 TiB"},
		{^uint64(0), "16.00 EiB"},
	}
	for _, c := range cases {
		if got := report.HumanBytes(c.in); got != c.want {
			t.Errorf("HumanBytes(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func sampleResult() bench.Result {
	j := 0.042
	start := time.Date(2026, 10, 12, 15, 4, 5, 0, time.FixedZone("WIB", 7*3600))
	return bench.Result{
		Mode:      bench.ModeThroughput,
		Target:    "100.64.0.1:9201",
		Duration:  10,
		Direction: bench.DirBoth,
		Streams:   2,
		Latency: bench.Latency{
			ProbesSent: 50, ProbesRecv: 49,
			PacketLossPct: 2, SuccessRate: 0.98,
			RTTMinMs: 0.5, RTTAvgMs: 1.25, RTTMaxMs: 3.125,
			JitterMs: &j,
		},
		Upload: &bench.Throughput{
			Direction: bench.DirUpload, Bytes: 125 * 1024 * 1024,
			Elapsed: 10.001, ThroughputBps: 13_105_000, Streams: 2,
		},
		Download: &bench.Throughput{
			Direction: bench.DirDownload, Bytes: 2_500_000_000,
			Elapsed: 10.0005, ThroughputBps: 250_000_000, Streams: 2,
		},
		StartedAt:  start,
		FinishedAt: start.Add(21500 * time.Millisecond),
	}
}

func TestFormatGolden(t *testing.T) {
	rule := strings.Repeat("─", 60)
	want := strings.Join([]string{
		"npbench report",
		rule,
		"  Mode               throughput",
		"  Target             100.64.0.1:9201",
		"  Direction          both",
		"  Streams            2",
		"  Duration           10s per direction",
		"  Started            2026-10-12 15:04:05 +07:00",
		"  Total time         21.5s",
		"",
		"Latency (UDP)",
		rule,
		"  Probes sent        50",
		"  Probes received    49",
		"  Packet loss        2.00 %",
		"  Success rate       98.00 %",
		"  RTT min/avg/max    0.500 / 1.250 / 3.125 ms",
		"  Jitter (RFC 3550)  0.042 ms",
		"",
		"Throughput (TCP)",
		rule,
		"  Direction           Bytes      Elapsed       Throughput",
		"  upload         125.00 MiB     10.001 s      104.84 Mbps",
		"  download         2.33 GiB     10.001 s     2000.00 Mbps",
		"",
	}, "\n")
	got := report.Format(sampleResult(), report.Options{})
	if got != want {
		t.Fatalf("report mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
	for i, line := range strings.Split(got, "\n") {
		if n := utf8.RuneCountInString(line); n > 80 {
			t.Errorf("line %d is %d columns: %q", i, n, line)
		}
	}
	if strings.Contains(got, "\x1b[") {
		t.Fatal("color disabled but output has ANSI escapes")
	}
}

func TestFormatOnlyDownloadAndNoReplies(t *testing.T) {
	r := sampleResult()
	r.Direction = bench.DirDownload
	r.Upload = nil
	r.Latency = bench.Latency{ProbesSent: 50, PacketLossPct: 100}
	got := report.Format(r, report.Options{})
	if strings.Contains(got, "  upload ") {
		t.Fatalf("unexpected upload row:\n%s", got)
	}
	for _, want := range []string{
		"  download         2.33 GiB",
		"  RTT min/avg/max    n/a",
		"  Jitter (RFC 3550)  n/a",
		"  Packet loss        100.00 %",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}

func TestFormatColor(t *testing.T) {
	r := sampleResult()
	got := report.Format(r, report.Options{Color: true})
	// 2% loss is past the yellow band; 0.042 ms jitter is green.
	if !strings.Contains(got, "\x1b[31m2.00 %\x1b[0m") {
		t.Fatalf("loss not red:\n%q", got)
	}
	if !strings.Contains(got, "\x1b[32m0.042 ms\x1b[0m") {
		t.Fatalf("jitter not green:\n%q", got)
	}
	r.Latency.PacketLossPct = 0
	if got := report.Format(r, report.Options{Color: true}); !strings.Contains(got, "\x1b[32m0.00 %") {
		t.Fatalf("zero loss not green:\n%q", got)
	}
}

func TestProgressLine(t *testing.T) {
	lat := bench.ProgressSnapshot{
		Phase: bench.PhaseLatency,
		Latency: bench.LatencyProgress{
			Started: true, Total: 50, Sent: 23, Recv: 22,
			Settled: 22, Lost: 0, LastRTT: 1250 * time.Microsecond,
			JitterMs: 0.031, HasJitter: true,
		},
	}
	if got, want := report.ProgressLine(lat, report.Rates{}),
		"latency   sent 23/50  recv 22  loss 0.0%  rtt 1.25 ms  jitter 0.03 ms"; got != want {
		t.Fatalf("latency line\n got %q\nwant %q", got, want)
	}

	up := bench.ProgressSnapshot{
		Phase: bench.PhaseUpload,
		Upload: bench.TCPProgress{
			Started: true, Elapsed: 2 * time.Second, Planned: 10 * time.Second,
			Streams: 1, Bytes: 250_000_000,
		},
	}
	got := report.ProgressLine(up, report.Rates{Upload: 150_000_000, Download: 1})
	want := "upload    2.0s/10.0s  238.42 MiB  now 1200.0 Mbps  avg 1000.0 Mbps"
	if got != want {
		t.Fatalf("upload line\n got %q\nwant %q", got, want)
	}

	big := up
	big.Phase = bench.PhaseDownload
	big.Download = bench.TCPProgress{
		Started: true, Elapsed: 100 * time.Second, Planned: 100 * time.Second,
		Bytes: 999 << 30,
	}
	if n := utf8.RuneCountInString("⠋ " + report.ProgressLine(big, report.Rates{Download: 12e9})); n > 80 {
		t.Fatalf("worst-case progress line is %d columns", n)
	}
}

func TestBidirProgressLine(t *testing.T) {
	s := bench.ProgressSnapshot{
		Phase: bench.PhaseBidir,
		Bidir: true,
		Upload: bench.TCPProgress{
			Started: true, Elapsed: 1200 * time.Millisecond, Planned: 10 * time.Second,
			Streams: 2, Bytes: 150_000_000,
		},
		Download: bench.TCPProgress{
			Started: true, Elapsed: 1200 * time.Millisecond, Planned: 10 * time.Second,
			Streams: 2, Bytes: 4_500_000_000,
		},
	}
	got := report.ProgressLine(s, report.Rates{Upload: 120_000_000, Download: 3_500_000_000})
	want := "⇅ bidir  1.2s/10.0s  up now 960.0 avg 1000  down now 28000 avg 30000 Mbps"
	if got != want {
		t.Fatalf("bidir line\n got %q\nwant %q", got, want)
	}

	s.Upload.Elapsed, s.Upload.Planned = 100*time.Second, 100*time.Second
	s.Download.Elapsed, s.Download.Planned = 100*time.Second, 100*time.Second
	s.Upload.Bytes, s.Download.Bytes = 999<<30, 999<<30
	line := "⠋ " + report.ProgressLine(s, report.Rates{Upload: 12e9, Download: 12e9})
	if n := utf8.RuneCountInString(line); n > 80 {
		t.Fatalf("worst-case bidir line is %d columns: %q", n, line)
	}
	if !strings.HasSuffix(line, " Mbps") {
		t.Fatalf("bidir line lost its unit: %q", line)
	}
}

func TestBidirPhaseLines(t *testing.T) {
	tcp := func(bytes uint64) bench.TCPProgress {
		return bench.TCPProgress{
			Started: true, Finished: true, Elapsed: 3 * time.Second,
			Planned: 3 * time.Second, Streams: 2, Bytes: bytes,
		}
	}
	s := bench.ProgressSnapshot{Bidir: true, Upload: tcp(375_000_000), Download: tcp(750_000_000)}
	checks := map[string]string{
		report.PhaseStartLine(s, bench.PhaseBidir): "bidir start (3.0s, 2 streams per direction, simultaneous)",
		report.PhaseEndLine(s, bench.PhaseBidir):   "bidir done  3.0s  up 357.63 MiB avg 1000.0 Mbps  down 715.26 MiB avg 2000.0 Mbps",
	}
	for got, want := range checks {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func TestFormatBidirGolden(t *testing.T) {
	r := sampleResult()
	r.Direction = bench.DirBidir
	r.FinishedAt = r.StartedAt.Add(11500 * time.Millisecond)
	r.Total = &bench.Throughput{
		Direction: bench.DirBidir, Bytes: r.Upload.Bytes + r.Download.Bytes,
		Elapsed: 10.001, ThroughputBps: r.Upload.ThroughputBps + r.Download.ThroughputBps, Streams: 4,
	}
	rule := strings.Repeat("─", 60)
	want := strings.Join([]string{
		"npbench report",
		rule,
		"  Mode               throughput",
		"  Target             100.64.0.1:9201",
		"  Direction          bidir (simultaneous)",
		"  Streams            2 per direction (4 total)",
		"  Duration           10s per direction",
		"  Started            2026-10-12 15:04:05 +07:00",
		"  Total time         11.5s",
		"",
		"Latency (UDP)",
		rule,
		"  Probes sent        50",
		"  Probes received    49",
		"  Packet loss        2.00 %",
		"  Success rate       98.00 %",
		"  RTT min/avg/max    0.500 / 1.250 / 3.125 ms",
		"  Jitter (RFC 3550)  0.042 ms",
		"",
		"Throughput (TCP)",
		rule,
		"  Direction           Bytes      Elapsed       Throughput",
		"  upload         125.00 MiB     10.001 s      104.84 Mbps",
		"  download         2.33 GiB     10.001 s     2000.00 Mbps",
		"  total            2.45 GiB     10.001 s     2104.84 Mbps",
		"",
	}, "\n")
	got := report.Format(r, report.Options{})
	if got != want {
		t.Fatalf("report mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
	for i, line := range strings.Split(got, "\n") {
		if n := utf8.RuneCountInString(line); n > 80 {
			t.Errorf("line %d is %d columns: %q", i, n, line)
		}
	}
}

func TestFormatStream(t *testing.T) {
	start := time.Date(2026, 10, 12, 15, 4, 5, 0, time.FixedZone("WIB", 7*3600))
	r := bench.Result{
		Mode:   bench.ModeStream,
		Target: "100.64.0.1:9201",
		Stream: &bench.Stream{
			BitrateBps: 5_000_000, PacketSize: 1316, DurationSec: 30, LateMs: 150,
			PacketsSent: 1000, PacketsRecv: 998, PacketLossPct: 0.2,
			AchievedBitrate: 4_980_000, JitterMs: 1.5,
			LatePackets: 2, LatePct: 0.2, MaxBurstPackets: 1, MaxBurstMs: 2,
			MaxGapMs: 5, RTTMinMs: 0.5, RTTAvgMs: 1.0, RTTMaxMs: 3.0,
			Verdict: bench.VerdictStable,
			PerSecond: []bench.StreamSecond{
				{Second: 0, Sent: 500, Recv: 499, Lost: 1, LossPct: 0.2, JitterMs: 1.2, Late: 1},
			},
		},
		StartedAt: start, FinishedAt: start.Add(31 * time.Second),
	}
	got := report.Format(r, report.Options{})
	for _, want := range []string{
		"  Mode               stream",
		"Stream Stability (UDP CBR)",
		"  Packet loss        0.200 %",
		"  Verdict            stable",
		"Per-second (appendix)",
		" 0.2",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}

func TestFormatMTU(t *testing.T) {
	r := bench.Result{
		Mode:   bench.ModeMTU,
		Target: "127.0.0.1:9201",
		MTU: &bench.MTU{
			UDPPayload: 1472, IPEstimate: 1500, IPVersion: "ipv4",
			DFEnabled: true, RecommendedOverlay: 1420, RecommendedRelay: 1414,
		},
	}
	got := report.Format(r, report.Options{})
	for _, want := range []string{
		"  Mode               mtu",
		"Path MTU Discovery (UDP)",
		"  UDP payload MTU    1472 bytes",
		"  Est. IP MTU        1500 bytes",
		"Fragment risk",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}

func TestStreamProgressLine(t *testing.T) {
	s := bench.ProgressSnapshot{
		Phase: bench.PhaseStream,
		Stream: bench.StreamProgress{
			Started: true, Elapsed: 12*time.Second + 300*time.Millisecond,
			Planned: 30 * time.Second, Bitrate: 5_000_000, Packet: 1316,
			Sent: 5840, Recv: 5835, HasJitter: true, JitterMs: 2.3,
		},
	}
	got := report.ProgressLine(s, report.Rates{})
	if !strings.Contains(got, "stream") || !strings.Contains(got, "sent 5840") || !strings.Contains(got, "loss") {
		t.Fatalf("bad stream line: %q", got)
	}
}

func TestPhaseLines(t *testing.T) {
	s := bench.ProgressSnapshot{
		Latency: bench.LatencyProgress{
			Started: true, Finished: true, Total: 50, Sent: 50, Recv: 49,
			Settled: 50, Lost: 1, JitterMs: 0.5, HasJitter: true,
		},
		Upload: bench.TCPProgress{
			Started: true, Finished: true, Elapsed: 3 * time.Second,
			Planned: 3 * time.Second, Streams: 1, Bytes: 375_000_000,
		},
	}
	checks := map[string]string{
		report.PhaseStartLine(s, bench.PhaseLatency): "latency start (50 UDP probes)",
		report.PhaseEndLine(s, bench.PhaseLatency):   "latency done  sent 50  recv 49  loss 2.0%  jitter 0.50 ms",
		report.PhaseStartLine(s, bench.PhaseUpload):  "upload start (3.0s, 1 stream)",
		report.PhaseEndLine(s, bench.PhaseUpload):    "upload done  3.0s  357.63 MiB  avg 1000.0 Mbps",
	}
	for got, want := range checks {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
