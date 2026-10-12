package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/beesar/net-peer-benchmark/internal/bench"
	"github.com/beesar/net-peer-benchmark/internal/report"
)

func main() {
	listen := flag.String("listen", "", "serve TCP+UDP on this address (e.g. :9201); omit to run a client")
	target := flag.String("target", "", "peer overlay host or host:port (e.g. 100.64.0.1)")
	mode := flag.String("mode", string(bench.ModeThroughput),
		"throughput (UDP latency + TCP), stream (UDP CBR stability), or mtu (path MTU discovery)")
	duration := flag.Duration("duration", 10*time.Second, "TCP bulk duration per direction, or stream CBR duration")
	streams := flag.Int("streams", 1, "parallel TCP connections per direction (throughput mode)")
	payload := flag.Int("payload", bench.DefaultPayload, "TCP chunk size in bytes (throughput mode)")
	packet := flag.Int("packet", bench.DefaultStreamPacket, "UDP datagram size in bytes (stream mode; default 1316 = 7×188 MPEG-TS)")
	bitrate := flag.String("bitrate", "5M", "constant bitrate for stream mode (e.g. 5M, 5000k, 5000000)")
	lateMs := flag.Int("late-ms", bench.DefaultLateMs, "stream echo RTT above this counts as late for playback (ms)")
	direction := flag.String("direction", string(bench.DirBoth),
		"upload, download, both (upload then download), or bidir (both at the same time)")
	probes := flag.Int("probes", bench.DefaultProbes, "UDP echo probes for RTT / loss / jitter")
	probeInterval := flag.Duration("probe-interval", bench.DefaultProbeInterval, "spacing between UDP probes")
	jsonOut := flag.Bool("json", false, "print only the JSON result on stdout (no progress unless -progress)")
	quiet := flag.Bool("quiet", false, "no live progress; in -listen mode, no per-session lines")
	showProgress := flag.Bool("progress", false, "show live progress on stderr even with -json")
	noColor := flag.Bool("no-color", false, "no ANSI colors in the report (also honors NO_COLOR)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *listen != "" {
		if err := serve(ctx, *listen, *quiet); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *target == "" {
		fmt.Fprintln(os.Stderr, "usage: npbench -listen :9201")
		fmt.Fprintln(os.Stderr, "       npbench -target HOST [-mode throughput|stream|mtu] ...")
		fmt.Fprintln(os.Stderr, "       npbench -target HOST -mode stream -bitrate 5M -duration 30s")
		fmt.Fprintln(os.Stderr, "       npbench -target HOST -mode mtu")
		os.Exit(2)
	}

	bps, err := bench.ParseBitrate(*bitrate)
	if err != nil {
		fmt.Fprintln(os.Stderr, "npbench: bitrate:", err)
		os.Exit(2)
	}

	cfg := bench.Config{
		Mode:          bench.Mode(*mode),
		Target:        *target,
		Duration:      *duration,
		Streams:       *streams,
		Payload:       *payload,
		Packet:        *packet,
		Bitrate:       bps,
		LateMs:        *lateMs,
		Direction:     bench.Direction(*direction),
		Probes:        *probes,
		ProbeInterval: *probeInterval,
	}
	if err := cfg.Normalize(); err != nil {
		fmt.Fprintln(os.Stderr, "npbench:", err)
		os.Exit(2)
	}

	var live *report.Live
	if !*quiet && (!*jsonOut || *showProgress) {
		cfg.Progress = &bench.Progress{}
		live = report.StartLive(os.Stderr, cfg.Progress, report.LiveOptions{TTY: isTerminal(os.Stderr)})
	}

	res, err := bench.Run(ctx, cfg)
	live.Stop()
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
	fmt.Print(report.Format(res, report.Options{Color: useColor(*noColor)}))
}

func serve(ctx context.Context, addr string, quiet bool) error {
	s := &bench.Server{Addr: addr}
	if !quiet {
		s.Logf = func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "npbench: "+format+"\n", args...)
		}
	}
	if err := s.Listen(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "npbench listening on %s (tcp+udp)\n", s.Addr)
	return s.Serve(ctx)
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

func useColor(disabled bool) bool {
	if disabled || os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		return false
	}
	// Windows consoles only render ANSI once VT processing is enabled, which
	// npbench does not do.
	if runtime.GOOS == "windows" {
		return false
	}
	return isTerminal(os.Stdout)
}
