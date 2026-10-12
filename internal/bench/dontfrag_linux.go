//go:build linux

package bench

import (
	"fmt"
	"net"
	"syscall"
)

func setDontFragment(conn *net.UDPConn) (bool, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return false, err
	}
	var ctrlErr error
	err = raw.Control(func(fd uintptr) {
		// Prefer PROBE so the kernel does not cache a reduced path MTU from
		// our intentionally oversized probes.
		if e := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER, syscall.IP_PMTUDISC_PROBE); e != nil {
			ctrlErr = e
		}
		_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, syscall.IPV6_MTU_DISCOVER, syscall.IPV6_PMTUDISC_PROBE)
	})
	if err != nil {
		return false, err
	}
	if ctrlErr != nil {
		return false, fmt.Errorf("IP_MTU_DISCOVER: %w", ctrlErr)
	}
	return true, nil
}
