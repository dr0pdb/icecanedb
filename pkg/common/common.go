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
