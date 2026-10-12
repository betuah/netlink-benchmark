package bench

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// ParseBitrate parses a bitrate string into bits per second.
// Accepts raw integers (bps) and SI suffixes: k/K/kbps, m/M/mbps, g/G/gbps.
// Examples: "5000000", "5M", "5m", "5000k", "5Mbps".
func ParseBitrate(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty bitrate")
	}
	lower := strings.ToLower(s)
	mult := int64(1)
	switch {
	case strings.HasSuffix(lower, "gbps"):
		mult = 1_000_000_000
		s = s[:len(s)-4]
	case strings.HasSuffix(lower, "mbps"):
		mult = 1_000_000
		s = s[:len(s)-4]
	case strings.HasSuffix(lower, "kbps"):
		mult = 1_000
		s = s[:len(s)-4]
	case strings.HasSuffix(lower, "bps"):
		mult = 1
		s = s[:len(s)-3]
	default:
		s = strings.TrimSpace(s)
		if s == "" {
			return 0, fmt.Errorf("invalid bitrate")
		}
		last := s[len(s)-1]
		if unicode.IsLetter(rune(last)) {
			switch unicode.ToLower(rune(last)) {
			case 'k':
				mult = 1_000
			case 'm':
				mult = 1_000_000
			case 'g':
				mult = 1_000_000_000
			default:
				return 0, fmt.Errorf("unknown bitrate suffix %q", string(last))
			}
			s = s[:len(s)-1]
		}
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("invalid bitrate")
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("invalid bitrate %q", s)
	}
	bps := int64(v * float64(mult))
	if bps <= 0 {
		return 0, fmt.Errorf("bitrate must be positive")
	}
	return bps, nil
}

// FormatBitrate renders bps with a compact SI suffix when clean.
func FormatBitrate(bps int64) string {
	switch {
	case bps >= 1_000_000_000 && bps%1_000_000_000 == 0:
		return fmt.Sprintf("%dG", bps/1_000_000_000)
	case bps >= 1_000_000 && bps%1_000_000 == 0:
		return fmt.Sprintf("%dM", bps/1_000_000)
	case bps >= 1_000 && bps%1_000 == 0:
		return fmt.Sprintf("%dK", bps/1_000)
	default:
		return fmt.Sprintf("%d", bps)
	}
}
