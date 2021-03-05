package common

// MinU64 returns min of two uint64
func MinU64(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}
