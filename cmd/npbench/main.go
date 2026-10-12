package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/beesar/net-peer-benchmark/internal/bench"
)

func main() {
	listen := flag.String("listen", "", "serve TCP+UDP on this address (e.g. :9201); omit to run a client")
	target := flag.String("target", "", "peer overlay host or host:port (e.g. 100.64.0.1)")
	duration := flag.Duration("duration", 10*time.Second, "TCP bulk transfer duration per direction")
	streams := flag.Int("streams", 1, "parallel TCP streams")
	payload := flag.Int("payload", bench.DefaultPayload, "TCP chunk size in bytes")
	direction := flag.String("direction", string(bench.DirBoth), "upload, download, or both")
	probes := flag.Int("probes", bench.DefaultProbes, "UDP echo probes for RTT / loss / jitter")
	probeInterval := flag.Duration("probe-interval", bench.DefaultProbeInterval, "spacing between UDP probes")
	jsonOut := flag.Bool("json", false, "print JSON summary instead of text")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *listen != "" {
		if err := serve(ctx, *listen); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *target == "" {
		fmt.Fprintln(os.Stderr, "usage: npbench -listen :9201")
		fmt.Fprintln(os.Stderr, "       npbench -target 100.64.0.1 [-duration 10s] [-streams 2]")
		os.Exit(2)
	}

	res, err := bench.Run(ctx, bench.Config{
		Target:        *target,
		Duration:      *duration,
		Streams:       *streams,
		Payload:       *payload,
		Direction:     bench.Direction(*direction),
		Probes:        *probes,
		ProbeInterval: *probeInterval,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(res); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	fmt.Print(bench.FormatSummary(res))
}

func serve(ctx context.Context, addr string) error {
	s := &bench.Server{Addr: addr}
	if err := s.Listen(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "npbench listening on %s (tcp+udp)\n", s.Addr)
	return s.Serve(ctx)
}
