package common

import "sync"

// could have used reflect package with {}interface as well.

// ProtectedBool is a boolean protected by RW lock
type ProtectedBool struct {
	m     sync.RWMutex
	value bool
}

// Set sets the value (surprise surprise!)
func (b *ProtectedBool) Set(nvalue bool) {
	b.m.Lock()
	defer b.m.Unlock()
	b.value = nvalue
}

// Get gets the value
func (b *ProtectedBool) Get() bool {
	b.m.RLock()
	defer b.m.RUnlock()
	return b.value
}

// ProtectedString is a string protected by RW lock
type ProtectedString struct {
	m     sync.RWMutex
	value string
}

// Set sets the value (surprise surprise!)
func (b *ProtectedString) Set(nvalue string) {
	b.m.Lock()
	defer b.m.Unlock()
	b.value = nvalue
}

// Get gets the value
func (b *ProtectedString) Get() string {
	b.m.RLock()
	defer b.m.RUnlock()
	return b.value
}

// ProtectedUint64 is a Uint64 protected by RW lock
type ProtectedUint64 struct {
	m     sync.RWMutex
	value uint64
}

// Set sets the value (surprise surprise!)
func (b *ProtectedUint64) Set(nvalue uint64) {
	b.m.Lock()
	defer b.m.Unlock()
	b.value = nvalue
}

// Get gets the value
func (b *ProtectedUint64) Get() uint64 {
	b.m.RLock()
	defer b.m.RUnlock()
	return b.value
}

// U64ToByte converts a uint64 to []byte
func U64ToByte(num uint64) []byte {
	res := make([]byte, 8)

	// encode term in first 8 bytes
	res[0] = uint8(num) // last 1 byte of term
	res[1] = uint8(num >> 8)
	res[2] = uint8(num >> 16)
	res[3] = uint8(num >> 24)
	res[4] = uint8(num >> 32)
	res[5] = uint8(num >> 40)
	res[6] = uint8(num >> 48)
	res[7] = uint8(num >> 56)

	return res
}
