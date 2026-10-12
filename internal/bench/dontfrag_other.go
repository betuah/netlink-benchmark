//go:build !linux && !darwin && !windows

package bench

import (
	"fmt"
	"net"
	"runtime"
)

func setDontFragment(conn *net.UDPConn) (bool, error) {
	_ = conn
	return false, fmt.Errorf("Don't Fragment sockopt not supported on %s", runtime.GOOS)
}
