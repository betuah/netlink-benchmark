//go:build windows

package bench

import (
	"fmt"
	"net"
	"syscall"
)

// Winsock IP_DONTFRAGMENT / IPV6_DONTFRAG.
const (
	windowsIP_DONTFRAGMENT = 14
	windowsIPV6_DONTFRAG   = 14
)

func setDontFragment(conn *net.UDPConn) (bool, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return false, err
	}
	var ctrlErr error
	var set bool
	err = raw.Control(func(fd uintptr) {
		e4 := syscall.SetsockoptInt(syscall.Handle(fd), syscall.IPPROTO_IP, windowsIP_DONTFRAGMENT, 1)
		e6 := syscall.SetsockoptInt(syscall.Handle(fd), syscall.IPPROTO_IPV6, windowsIPV6_DONTFRAG, 1)
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
		return false, fmt.Errorf("IP_DONTFRAGMENT: %w", ctrlErr)
	}
	return set, nil
}
