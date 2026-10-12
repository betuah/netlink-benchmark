package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beesar/net-peer-benchmark/internal/bench"
	"github.com/beesar/net-peer-benchmark/internal/report"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func startServer(t *testing.T) (*bench.Server, *syncBuffer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	logs := &syncBuffer{}
	s := &bench.Server{
		Addr: "127.0.0.1:0",
		Logf: func(format string, args ...any) {
			_, _ = fmt.Fprintf(logs, format+"\n", args...)
		},
	}
	if err := s.Listen(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = s.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return s, logs
}

func runWithLive(t *testing.T, addr string, opt report.LiveOptions) (bench.Result, string) {
	t.Helper()
	prog := &bench.Progress{}
	out := &syncBuffer{}
	live := report.StartLive(out, prog, opt)
	res, err := bench.Run(context.Background(), bench.Config{
		Mode:          bench.ModeThroughput,
		Target:        addr,
		Duration:      300 * time.Millisecond,
		Streams:       2,
		Payload:       32 * 1024,
		Direction:     bench.DirBoth,
		Probes:        10,
		ProbeInterval: 5 * time.Millisecond,
		ProbeTimeout:  200 * time.Millisecond,
		Progress:      prog,
	})
	live.Stop()
	if err != nil {
		t.Fatal(err)
	}
	return res, out.String()
}

func TestPlainProgressPrintsPhaseLines(t *testing.T) {
	s, logs := startServer(t)
	_, out := runWithLive(t, s.Addr, report.LiveOptions{TTY: false, Interval: 20 * time.Millisecond})

	if strings.Contains(out, "\r") {
		t.Fatalf("plain progress must not use \\r:\n%q", out)
	}
	order := []string{
		"npbench: latency start (10 UDP probes)",
		"npbench: latency done",
		"npbench: upload start (0.3s, 2 streams)",
		"npbench: upload done",
		"npbench: download start (0.3s, 2 streams)",
		"npbench: download done",
	}
	pos := 0
	for _, want := range order {
		i := strings.Index(out[pos:], want)
		if i < 0 {
			t.Fatalf("missing %q after offset %d in:\n%s", want, pos, out)
		}
		pos += i + len(want)
	}

	// 2 upload + 2 download streams, plus one UDP session line. The server
	// logs a stream when its side ends, which can trail the client a little.
	want := map[string]int{"phase=upload": 2, "phase=download": 2, "phase=latency": 1}
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
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("session lines, want %v:\n%s", want, got)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestTTYProgressClearsLine(t *testing.T) {
	s, _ := startServer(t)
	_, out := runWithLive(t, s.Addr, report.LiveOptions{TTY: true, Interval: 20 * time.Millisecond})

	if !strings.Contains(out, "\r") {
		t.Fatalf("tty progress should redraw with \\r:\n%q", out)
	}
	if strings.Contains(out, "\n") {
		t.Fatalf("tty progress must stay on one line:\n%q", out)
	}
	for _, want := range []string{"latency   sent", "upload ", "download ", "Mbps"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%q", want, out)
		}
	}
	last := out[strings.LastIndex(strings.TrimSuffix(out, "\r"), "\r")+1:]
	if strings.TrimSpace(last) != "" {
		t.Fatalf("line not cleared at stop, tail = %q", last)
	}
}
