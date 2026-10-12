package report

import (
	"fmt"
	"time"
)

var byteUnits = []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

// HumanBytes formats n with IEC units and two decimals ("1.50 MiB").
func HumanBytes(n uint64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	v := float64(n) / 1024
	i := 0
	// Promote before %.2f would round up to "1024.00".
	for v >= 1024-0.005 && i < len(byteUnits)-1 {
		v /= 1024
		i++
	}
	return fmt.Sprintf("%.2f %s", v, byteUnits[i])
}

func seconds(d time.Duration) string {
	return fmt.Sprintf("%.1fs", d.Seconds())
}

func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
