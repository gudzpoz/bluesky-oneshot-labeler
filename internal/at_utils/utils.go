package at_utils

import "sync/atomic"

func StoreLarger(i *atomic.Int64, v int64) {
	for {
		current := i.Load()
		if v <= current {
			return
		}
		if i.CompareAndSwap(current, v) {
			return
		}
	}
}
