//go:build integration && load

package compat

import (
	"math/bits"
	"time"
)

// latencyHistogram counts every nonnegative int64 duration in 432 KiB. Buckets
// keep ten fractional binary digits: their upper bounds round up by less than
// 0.1%. Durations below 2048 ns are exact; the maximum remains exact separately.
type latencyHistogram [54 * 1024]uint64

func (h *latencyHistogram) record(duration time.Duration) {
	value := uint64(duration)
	shift := max(bits.Len64(value)-11, 0)
	h[shift*1024+int(value>>shift)]++
}

func (h *latencyHistogram) percentiles(count int, maximum time.Duration) [3]float64 {
	ranks := [3]uint64{(uint64(count)*950 + 999) / 1000, (uint64(count)*990 + 999) / 1000, (uint64(count)*999 + 999) / 1000}
	var result [3]float64
	var seen uint64
	percentile := 0
	for index, amount := range h {
		if amount == 0 {
			continue
		}
		seen += amount
		for percentile < len(ranks) && seen >= ranks[percentile] {
			shift := max(index/1024-1, 0)
			upper := time.Duration((uint64(index-shift*1024+1) << shift) - 1)
			result[percentile] = min(upper, maximum).Seconds()
			percentile++
		}
		if percentile == len(ranks) {
			break
		}
	}
	return result
}
