// Package report renders npbench results for humans: the final report on
// stdout and the live progress line on stderr.
package report

import (
	"fmt"
	"strings"
	"time"

	"github.com/beesar/net-peer-benchmark/internal/bench"
)

// Options controls the final report.
type Options struct {
	// Color wraps loss/jitter values and titles in ANSI colors.
	Color bool
}

const (
	ruleWidth  = 60
	timeLayout = "2006-01-02 15:04:05 -07:00"

	ansiReset  = "\x1b[0m"
	ansiBold   = "\x1b[1m"
	ansiGreen  = "\x1b[32m"
	ansiYellow = "\x1b[33m"
	ansiRed    = "\x1b[31m"
)

// Loss and jitter thresholds for color: below the first is green, below the
// second is yellow, anything else is red.
const (
	lossGoodPct  = 0.0001
	lossWarnPct  = 2
	jitterGoodMs = 5
	jitterWarnMs = 30
)

// Format returns the multi-line report for r, at most 80 columns wide.
func Format(r bench.Result, opt Options) string {
	switch r.Mode {
	case bench.ModeAll:
		return formatAll(r, opt)
	case bench.ModeStream:
		return formatStream(r, opt)
	case bench.ModeMTU:
		return formatMTU(r, opt)
	default:
		return formatThroughput(r, opt)
	}
}

func formatAll(r bench.Result, opt Options) string {
	var b strings.Builder
	_, section, kv := painters(&b, opt)

	section("npbench report")
	kv("Mode", "all")
	kv("Target", r.Target)
	if !r.StartedAt.IsZero() {
		kv("Started", r.StartedAt.Format(timeLayout))
	}
	if !r.FinishedAt.IsZero() && !r.StartedAt.IsZero() {
		kv("Total time", seconds(r.FinishedAt.Sub(r.StartedAt)))
	}

	tp := formatThroughput(bench.Result{
		Mode:       bench.ModeThroughput,
		Target:     r.Target,
		Duration:   r.Duration,
		Direction:  r.Direction,
		Streams:    r.Streams,
		Latency:    r.Latency,
		Upload:     r.Upload,
		Download:   r.Download,
		Total:      r.Total,
		StartedAt:  r.StartedAt,
		FinishedAt: r.FinishedAt,
	}, opt)
	// Drop the per-mode header block; keep Latency / Throughput sections.
	if i := strings.Index(tp, "\n\n"); i >= 0 {
		b.WriteString(tp[i:])
	} else {
		b.WriteString("\n")
		b.WriteString(tp)
	}

	if r.Stream != nil {
		st := formatStream(bench.Result{
			Mode: bench.ModeStream, Target: r.Target, Stream: r.Stream,
			StartedAt: r.StartedAt, FinishedAt: r.FinishedAt,
		}, opt)
		if i := strings.Index(st, "\n\n"); i >= 0 {
			b.WriteString(st[i:])
		}
	}
	if r.MTU != nil {
		mt := formatMTU(bench.Result{
			Mode: bench.ModeMTU, Target: r.Target, MTU: r.MTU,
			StartedAt: r.StartedAt, FinishedAt: r.FinishedAt,
		}, opt)
		if i := strings.Index(mt, "\n\n"); i >= 0 {
			b.WriteString(mt[i:])
		}
	}
	return b.String()
}

func formatThroughput(r bench.Result, opt Options) string {
	var b strings.Builder
	paint, section, kv := painters(&b, opt)

	section("npbench report")
	kv("Mode", string(orMode(r.Mode, bench.ModeThroughput)))
	kv("Target", r.Target)
	if r.Direction == bench.DirBidir {
		n := streamsOf(r)
		kv("Direction", "bidir (simultaneous)")
		kv("Streams", fmt.Sprintf("%d per direction (%d total)", n, 2*n))
	} else {
		kv("Direction", string(r.Direction))
		kv("Streams", fmt.Sprintf("%d", streamsOf(r)))
	}
	kv("Duration", fmt.Sprintf("%s per direction", time.Duration(r.Duration*float64(time.Second))))
	if !r.StartedAt.IsZero() {
		kv("Started", r.StartedAt.Format(timeLayout))
	}
	if !r.FinishedAt.IsZero() && !r.StartedAt.IsZero() {
		kv("Total time", seconds(r.FinishedAt.Sub(r.StartedAt)))
	}

	lat := r.Latency
	b.WriteString("\n")
	section("Latency (UDP)")
	kv("Probes sent", fmt.Sprintf("%d", lat.ProbesSent))
	kv("Probes received", fmt.Sprintf("%d", lat.ProbesRecv))
	kv("Packet loss", paint(lossColor(lat.PacketLossPct), fmt.Sprintf("%.2f %%", lat.PacketLossPct)))
	kv("Success rate", fmt.Sprintf("%.2f %%", lat.SuccessRate*100))
	if lat.ProbesRecv > 0 {
		kv("RTT min/avg/max", fmt.Sprintf("%.3f / %.3f / %.3f ms", lat.RTTMinMs, lat.RTTAvgMs, lat.RTTMaxMs))
	} else {
		kv("RTT min/avg/max", "n/a")
	}
	if lat.JitterMs != nil {
		kv("Jitter (RFC 3550)", paint(jitterColor(*lat.JitterMs), fmt.Sprintf("%.3f ms", *lat.JitterMs)))
	} else {
		kv("Jitter (RFC 3550)", "n/a")
	}

	if r.Upload != nil || r.Download != nil {
		b.WriteString("\n")
		section("Throughput (TCP)")
		row := "  %-10s %14s %12s %16s\n"
		fmt.Fprintf(&b, row, "Direction", "Bytes", "Elapsed", "Throughput")
		for _, t := range []*bench.Throughput{r.Upload, r.Download, r.Total} {
			if t == nil {
				continue
			}
			name := string(t.Direction)
			if t == r.Total {
				name = "total"
			}
			fmt.Fprintf(&b, row, name, HumanBytes(t.Bytes),
				fmt.Sprintf("%.3f s", t.Elapsed),
				fmt.Sprintf("%.2f Mbps", bench.Mbps(t.ThroughputBps)))
		}
	}
	return b.String()
}

func formatStream(r bench.Result, opt Options) string {
	var b strings.Builder
	paint, section, kv := painters(&b, opt)
	st := r.Stream
	if st == nil {
		st = &bench.Stream{}
	}

	section("npbench report")
	kv("Mode", "stream")
	kv("Target", r.Target)
	kv("Bitrate", fmt.Sprintf("%s (%d bps)", bench.FormatBitrate(st.BitrateBps), st.BitrateBps))
	kv("Packet size", fmt.Sprintf("%d bytes", st.PacketSize))
	kv("Duration", time.Duration(st.DurationSec*float64(time.Second)).String())
	kv("Late threshold", fmt.Sprintf("%d ms (echo RTT)", st.LateMs))
	if !r.StartedAt.IsZero() {
		kv("Started", r.StartedAt.Format(timeLayout))
	}
	if !r.FinishedAt.IsZero() && !r.StartedAt.IsZero() {
		kv("Total time", seconds(r.FinishedAt.Sub(r.StartedAt)))
	}

	b.WriteString("\n")
	section("Stream Stability (UDP CBR)")
	kv("Packets sent", fmt.Sprintf("%d", st.PacketsSent))
	kv("Packets received", fmt.Sprintf("%d", st.PacketsRecv))
	kv("Packet loss", paint(lossColor(st.PacketLossPct), fmt.Sprintf("%.3f %%", st.PacketLossPct)))
	kv("Achieved bitrate", fmt.Sprintf("%.2f Mbps", st.AchievedBitrate/1e6))
	kv("Jitter (RFC 3550)", paint(jitterColor(st.JitterMs), fmt.Sprintf("%.3f ms", st.JitterMs)))
	kv("Late packets", fmt.Sprintf("%d (%.3f %%)", st.LatePackets, st.LatePct))
	kv("Out of order", fmt.Sprintf("%d", st.OutOfOrder))
	kv("Duplicates", fmt.Sprintf("%d", st.Duplicates))
	kv("Max loss burst", fmt.Sprintf("%d packets / %.1f ms", st.MaxBurstPackets, st.MaxBurstMs))
	kv("Max recv gap", fmt.Sprintf("%.1f ms", st.MaxGapMs))
	if st.PacketsRecv > 0 {
		kv("Echo RTT min/avg/max", fmt.Sprintf("%.3f / %.3f / %.3f ms", st.RTTMinMs, st.RTTAvgMs, st.RTTMaxMs))
	}
	kv("Verdict", paint(verdictColor(st.Verdict), string(st.Verdict)))

	if len(st.PerSecond) > 0 {
		b.WriteString("\n")
		section("Per-second (appendix)")
		row := "  %6s %6s %6s %7s %8s %5s\n"
		fmt.Fprintf(&b, row, "Sec", "Sent", "Recv", "Loss%", "Jitter", "Late")
		for _, s := range st.PerSecond {
			fmt.Fprintf(&b, row,
				fmt.Sprintf("%d", s.Second),
				fmt.Sprintf("%d", s.Sent),
				fmt.Sprintf("%d", s.Recv),
				fmt.Sprintf("%.1f", s.LossPct),
				fmt.Sprintf("%.2f", s.JitterMs),
				fmt.Sprintf("%d", s.Late))
		}
	}
	return b.String()
}

func formatMTU(r bench.Result, opt Options) string {
	var b strings.Builder
	paint, section, kv := painters(&b, opt)
	m := r.MTU
	if m == nil {
		m = &bench.MTU{}
	}

	section("npbench report")
	kv("Mode", "mtu")
	kv("Target", r.Target)
	if !r.StartedAt.IsZero() {
		kv("Started", r.StartedAt.Format(timeLayout))
	}
	if !r.FinishedAt.IsZero() && !r.StartedAt.IsZero() {
		kv("Total time", seconds(r.FinishedAt.Sub(r.StartedAt)))
	}

	b.WriteString("\n")
	section("Path MTU Discovery (UDP)")
	kv("UDP payload MTU", fmt.Sprintf("%d bytes", m.UDPPayload))
	kv("Est. IP MTU", fmt.Sprintf("%d bytes (%s + UDP)", m.IPEstimate, m.IPVersion))
	kv("DF enabled", fmt.Sprintf("%v", m.DFEnabled))
	if m.SoftProbe {
		kv("Soft probe", "yes (no DF confirmation)")
	}
	kv("Overlay expect", fmt.Sprintf("%d (direct) / %d (relay)", m.RecommendedOverlay, m.RecommendedRelay))
	risk := "no"
	if m.FragmentationRisk {
		risk = paint(ansiRed, "yes — below overlay MTU")
	} else {
		risk = paint(ansiGreen, "no")
	}
	kv("Fragment risk", risk)
	if m.Note != "" {
		kv("Note", m.Note)
	}
	return b.String()
}

func painters(b *strings.Builder, opt Options) (
	paint func(code, s string) string,
	section func(title string),
	kv func(k, v string),
) {
	paint = func(code, s string) string {
		if !opt.Color || code == "" {
			return s
		}
		return code + s + ansiReset
	}
	rule := strings.Repeat("─", ruleWidth)
	section = func(title string) {
		fmt.Fprintf(b, "%s\n%s\n", paint(ansiBold, title), rule)
	}
	kv = func(k, v string) {
		fmt.Fprintf(b, "  %-18s %s\n", k, v)
	}
	return paint, section, kv
}

func orMode(m, def bench.Mode) bench.Mode {
	if m == "" {
		return def
	}
	return m
}

func verdictColor(v bench.StreamVerdict) string {
	switch v {
	case bench.VerdictStable:
		return ansiGreen
	case bench.VerdictDegraded:
		return ansiYellow
	default:
		return ansiRed
	}
}

func streamsOf(r bench.Result) int {
	if r.Streams > 0 {
		return r.Streams
	}
	for _, t := range []*bench.Throughput{r.Upload, r.Download} {
		if t != nil {
			return t.Streams
		}
	}
	return 0
}

func lossColor(pct float64) string {
	switch {
	case pct < lossGoodPct:
		return ansiGreen
	case pct < lossWarnPct:
		return ansiYellow
	}
	return ansiRed
}

func jitterColor(ms float64) string {
	switch {
	case ms < jitterGoodMs:
		return ansiGreen
	case ms < jitterWarnMs:
		return ansiYellow
	}
	return ansiRed
}
