//go:build darwin

package bench

import (
	"fmt"
	"net"
	"syscall"
)

// Darwin sockopts are missing from the stdlib syscall package.
const (
	darwinIP_DONTFRAG   = 0x1c // IP_DONTFRAG
	darwinIPV6_DONTFRAG = 0x3e // IPV6_DONTFRAG
)

func setDontFragment(conn *net.UDPConn) (bool, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return false, err
	}
	var ctrlErr error
	var set bool
	err = raw.Control(func(fd uintptr) {
		// Prefer IPv4-only sockets: IP_DONTFRAG works on udp4. Dual-stack
		// "udp" sockets need IPV6_DONTFRAG (macOS 15+ also covers IPv4).
		e4 := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, darwinIP_DONTFRAG, 1)
		e6 := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, darwinIPV6_DONTFRAG, 1)
		if e4 != nil && e6 != nil {
			ctrlErr = e4
			return
		}
		set = true
	})
	if err != nil {
		return false, err
	}
	if ctrlErr != nil {
		return false, fmt.Errorf("IP_DONTFRAG: %w", ctrlErr)
	}
	return set, nil
}
