package bench

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"
)

const (
	mtuMinPayload = 64
	mtuMaxPayload = 1500
	mtuProbeTries = 3
	mtuProbeWait  = 300 * time.Millisecond
)

func runMTU(ctx context.Context, addr string, cfg Config) (Result, error) {
	started := time.Now()
	res := Result{
		Mode:      ModeMTU,
		Target:    addr,
		StartedAt: started,
	}
	cfg.Progress.beginMTU(mtuMinPayload, mtuMaxPayload)

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return Result{}, err
	}
	network := "udp4"
	ipVer := "ipv4"
	headerOverhead := 28 // IPv4(20) + UDP(8)
	if ip := net.ParseIP(host); ip != nil && ip.To4() == nil {
		network = "udp6"
		ipVer = "ipv6"
		headerOverhead = 48 // IPv6(40) + UDP(8)
	}

	raddr, err := net.ResolveUDPAddr(network, addr)
	if err != nil {
		// Fall back to dual-stack resolve then dial with discovered family.
		raddr, err = net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return Result{}, err
		}
		if raddr.IP != nil && raddr.IP.To4() == nil {
			network = "udp6"
			ipVer = "ipv6"
			headerOverhead = 48
		} else {
			network = "udp4"
			ipVer = "ipv4"
			headerOverhead = 28
		}
	}

	conn, err := net.DialUDP(network, nil, raddr)
	if err != nil {
		return Result{}, err
	}
	defer conn.Close()

	dfOK, dfErr := setDontFragment(conn)
	soft := !dfOK
	note := ""
	if dfErr != nil {
		note = "Don't Fragment not set (" + dfErr.Error() + "); soft probe without DF"
	} else if soft {
		note = "Don't Fragment unavailable; soft probe without DF"
	}

	lo, hi := mtuMinPayload, mtuMaxPayload
	best := 0
	var seq uint32
	for lo <= hi {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		mid := (lo + hi) / 2
		cfg.Progress.mtuTrying(mid, best, hi)
		ok := mtuProbe(ctx, conn, &seq, mid)
		if ok {
			best = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	if best < mtuHeaderSize {
		cfg.Progress.endMTU()
		cfg.Progress.done()
		return Result{}, fmt.Errorf("mtu discovery failed: no probe replies (is the peer running a compatible npbench?)")
	}

	ipEst := best + headerOverhead
	m := &MTU{
		UDPPayload:         best,
		IPEstimate:         ipEst,
		IPVersion:          ipVer,
		DFEnabled:          dfOK,
		SoftProbe:          soft,
		RecommendedOverlay: OverlayMTU,
		RecommendedRelay:   OverlayRelayMTU,
		FragmentationRisk:  ipEst < OverlayMTU,
		Note:               note,
	}
	if soft {
		if m.Note != "" {
			m.Note += "; "
		}
		m.Note += "result is a soft upper bound without DF confirmation"
	}
	cfg.Progress.endMTU()
	cfg.Progress.done()
	res.MTU = m
	res.FinishedAt = time.Now()
	return res, nil
}

func mtuProbe(ctx context.Context, conn *net.UDPConn, seq *uint32, size int) bool {
	for try := 0; try < mtuProbeTries; try++ {
		if ctx.Err() != nil {
			return false
		}
		*seq++
		s := *seq
		pkt := encodeMTU(cmdMTUProbe, s, size)
		if _, err := conn.Write(pkt); err != nil {
			if isMsgSize(err) {
				return false
			}
			continue
		}
		buf := make([]byte, 64)
		deadline := time.Now().Add(mtuProbeWait)
		for time.Now().Before(deadline) {
			_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			n, err := conn.Read(buf)
			if err != nil {
				continue
			}
			kind, gotSeq, _, err := decodeMTU(buf[:n])
			if err != nil || kind != cmdMTUReply || gotSeq != s {
				continue
			}
			return true
		}
	}
	return false
}

func isMsgSize(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "message too long") ||
		strings.Contains(s, "Message too long") ||
		strings.Contains(s, "wsasend") ||
		strings.Contains(strings.ToLower(s), "too large")
}
