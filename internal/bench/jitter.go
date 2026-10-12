package bench

import "time"

const rttWindow = 8

// jitterMs applies RFC 3550 interarrival jitter over recent RTT samples.
// D between two echoes is the difference of their RTTs. The gain is 1/16.
// A single sample is not jitter yet, so ok is false.
func jitterMs(rtts []time.Duration) (float64, bool) {
	if len(rtts) > rttWindow {
		rtts = rtts[len(rtts)-rttWindow:]
	}
	if len(rtts) < 2 {
		return 0, false
	}
	var j float64
	for i := 1; i < len(rtts); i++ {
		d := float64(rtts[i]-rtts[i-1]) / float64(time.Millisecond)
		if d < 0 {
			d = -d
		}
		j += (d - j) / 16
	}
	return j, true
}
