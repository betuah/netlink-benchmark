package integration_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/beesar/net-peer-benchmark/internal/bench"
	"github.com/beesar/net-peer-benchmark/internal/report"
)

func TestStreamLoopback(t *testing.T) {
	s, logs := startServer(t)
	prog := &bench.Progress{}
	out := &syncBuffer{}
	live := report.StartLive(out, prog, report.LiveOptions{TTY: false, Interval: 20 * time.Millisecond})
	res, err := bench.Run(context.Background(), bench.Config{
		Mode:     bench.ModeStream,
		Target:   s.Addr,
		Duration: 400 * time.Millisecond,
		Bitrate:  500_000, // 0.5 Mbit/s — few packets on loopback
		Packet:   200,
		LateMs:   150,
		Progress: prog,
	})
	live.Stop()
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != bench.ModeStream || res.Stream == nil {
		t.Fatalf("result = %+v", res)
	}
	st := res.Stream
	if st.PacketsSent < 5 {
		t.Fatalf("packets_sent = %d, want >= 5", st.PacketsSent)
	}
	if st.PacketsRecv < st.PacketsSent/2 {
		t.Fatalf("packets_recv = %d / sent %d", st.PacketsRecv, st.PacketsSent)
	}
	if st.Verdict == "" {
		t.Fatal("empty verdict")
	}
	if st.PacketSize != 200 || st.BitrateBps != 500_000 {
		t.Fatalf("config echo = %+v", st)
	}

	text := out.String()
	for _, want := range []string{"npbench: stream start", "npbench: stream done"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if strings.Contains(logs.String(), "phase=stream") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("missing stream session log:\n%s", logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}

	rep := report.Format(res, report.Options{})
	if !strings.Contains(rep, "Stream Stability") || !strings.Contains(rep, string(st.Verdict)) {
		t.Fatalf("bad report:\n%s", rep)
	}
}

func TestMTULoopback(t *testing.T) {
	s, logs := startServer(t)
	prog := &bench.Progress{}
	out := &syncBuffer{}
	live := report.StartLive(out, prog, report.LiveOptions{TTY: false, Interval: 20 * time.Millisecond})
	res, err := bench.Run(context.Background(), bench.Config{
		Mode:     bench.ModeMTU,
		Target:   s.Addr,
		Progress: prog,
	})
	live.Stop()
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != bench.ModeMTU || res.MTU == nil {
		t.Fatalf("result = %+v", res)
	}
	m := res.MTU
	// Loopback should accept near-Ethernet UDP payloads.
	if m.UDPPayload < 1000 {
		t.Fatalf("udp_payload = %d, want >= 1000 on loopback", m.UDPPayload)
	}
	if m.IPEstimate < m.UDPPayload {
		t.Fatalf("ip estimate %d < udp %d", m.IPEstimate, m.UDPPayload)
	}
	if m.RecommendedOverlay != bench.OverlayMTU || m.RecommendedRelay != bench.OverlayRelayMTU {
		t.Fatalf("recommended = %+v", m)
	}

	text := out.String()
	for _, want := range []string{"npbench: mtu start", "npbench: mtu done"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if strings.Contains(logs.String(), "phase=mtu") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("missing mtu session log:\n%s", logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}

	rep := report.Format(res, report.Options{})
	if !strings.Contains(rep, "Path MTU Discovery") || !strings.Contains(rep, "UDP payload MTU") {
		t.Fatalf("bad report:\n%s", rep)
	}
}

func TestStreamHandshakeRejectsOldPeer(t *testing.T) {
	// A UDP sink that discards everything — no stream hello ack.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	go func() {
		buf := make([]byte, 2048)
		for ctx.Err() == nil {
			_ = pc.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			_, _, _ = pc.ReadFrom(buf)
		}
	}()

	_, err = bench.Run(context.Background(), bench.Config{
		Mode:     bench.ModeStream,
		Target:   pc.LocalAddr().String(),
		Duration: 200 * time.Millisecond,
		Bitrate:  100_000,
		Packet:   200,
	})
	if err == nil || !strings.Contains(err.Error(), "stream handshake") {
		t.Fatalf("want handshake error, got %v", err)
	}
}
