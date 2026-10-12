package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/beesar/net-peer-benchmark/internal/bench"
	"github.com/beesar/net-peer-benchmark/internal/report"
)

func TestBidirRunsBothDirectionsAtOnce(t *testing.T) {
	s, logs := startServer(t)
	const dur = 400 * time.Millisecond
	prog := &bench.Progress{}
	out := &syncBuffer{}
	live := report.StartLive(out, prog, report.LiveOptions{TTY: false, Interval: 20 * time.Millisecond})
	res, err := bench.Run(context.Background(), bench.Config{
		Target:        s.Addr,
		Duration:      dur,
		Streams:       2,
		Payload:       32 * 1024,
		Direction:     bench.DirBidir,
		Probes:        10,
		ProbeInterval: 5 * time.Millisecond,
		ProbeTimeout:  200 * time.Millisecond,
		Progress:      prog,
	})
	live.Stop()
	if err != nil {
		t.Fatal(err)
	}

	up, down := res.Upload, res.Download
	if up == nil || down == nil || res.Total == nil {
		t.Fatalf("upload=%+v download=%+v total=%+v", up, down, res.Total)
	}
	if up.Bytes == 0 || down.Bytes == 0 {
		t.Fatalf("bytes upload=%d download=%d", up.Bytes, down.Bytes)
	}
	if up.Streams != 2 || down.Streams != 2 || res.Total.Streams != 4 {
		t.Fatalf("streams upload=%d download=%d total=%d", up.Streams, down.Streams, res.Total.Streams)
	}
	if res.Total.Bytes != up.Bytes+down.Bytes {
		t.Fatalf("total bytes %d != %d + %d", res.Total.Bytes, up.Bytes, down.Bytes)
	}

	// The windows must overlap for most of the run, not just touch.
	overlapStart := maxTime(up.StartedAt, down.StartedAt)
	overlapEnd := minTime(up.FinishedAt, down.FinishedAt)
	if overlap := overlapEnd.Sub(overlapStart); overlap < dur*8/10 {
		t.Fatalf("overlap %v, want >= %v (upload %v..%v, download %v..%v)", overlap, dur*8/10,
			up.StartedAt, up.FinishedAt, down.StartedAt, down.FinishedAt)
	}
	if res.Latency.ProbesSent != 10 || !up.StartedAt.After(res.StartedAt) {
		t.Fatalf("latency phase must run first: probes=%d started=%v upload=%v",
			res.Latency.ProbesSent, res.StartedAt, up.StartedAt)
	}

	text := out.String()
	order := []string{
		"npbench: latency done",
		"npbench: bidir start (0.4s, 2 streams per direction, simultaneous)",
		"npbench: bidir done",
	}
	pos := 0
	for _, want := range order {
		i := strings.Index(text[pos:], want)
		if i < 0 {
			t.Fatalf("missing %q after offset %d in:\n%s", want, pos, text)
		}
		pos += i + len(want)
	}
	if strings.Contains(text, "upload start") || strings.Contains(text, "download start") {
		t.Fatalf("bidir must not print per-direction phase lines:\n%s", text)
	}

	want := map[string]int{"phase=upload": 2, "phase=download": 2}
	deadline := time.Now().Add(3 * time.Second)
	for {
		got := logs.String()
		ok := true
		for k, n := range want {
			if strings.Count(got, k) != n {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("session lines, want %v:\n%s", want, got)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestBidirTTYLine(t *testing.T) {
	s, _ := startServer(t)
	prog := &bench.Progress{}
	out := &syncBuffer{}
	live := report.StartLive(out, prog, report.LiveOptions{TTY: true, Interval: 20 * time.Millisecond})
	_, err := bench.Run(context.Background(), bench.Config{
		Target:        s.Addr,
		Duration:      300 * time.Millisecond,
		Streams:       1,
		Payload:       32 * 1024,
		Direction:     bench.DirBidir,
		Probes:        5,
		ProbeInterval: 5 * time.Millisecond,
		ProbeTimeout:  200 * time.Millisecond,
		Progress:      prog,
	})
	live.Stop()
	if err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if strings.Contains(text, "\n") {
		t.Fatalf("tty progress must stay on one line:\n%q", text)
	}
	for _, want := range []string{"⇅ bidir", "up now", "down now", "Mbps"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%q", want, text)
		}
	}
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
