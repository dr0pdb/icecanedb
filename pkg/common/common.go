/**
 * Copyright 2020 The IcecaneDB Authors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package common

import (
	"sync"

	"google.golang.org/grpc"
)

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

// SetIfGreater sets the nvalue if the old value < nvalue.
// if return value is less than nvalue, then the value was updated.
func (b *ProtectedUint64) SetIfGreater(nvalue uint64) (old uint64) {
	b.m.Lock()
	defer b.m.Unlock()

	old = nvalue
	if b.value < nvalue {
		old = b.value
		b.value = nvalue
	}

	return old
}

// Increment increments the value atomically and returns the new value.
func (b *ProtectedUint64) Increment() uint64 {
	b.m.Lock()
	defer b.m.Unlock()
	b.value++
	return b.value
}

// Get gets the value
func (b *ProtectedUint64) Get() uint64 {
	b.m.RLock()
	defer b.m.RUnlock()
	return b.value
}

// ProtectedMapUConn is a RWMutex protected map from uint64 to grpc.ClientConn
type ProtectedMapUConn struct {
	mu sync.RWMutex
	m  map[uint64]*grpc.ClientConn
}

// NewProtectedMapUConn initializes a new protected map of uint64 to grpc connection.
func NewProtectedMapUConn() *ProtectedMapUConn {
	return &ProtectedMapUConn{
		m: make(map[uint64]*grpc.ClientConn),
	}
}

// Set sets the value
func (pm *ProtectedMapUConn) Set(key uint64, value *grpc.ClientConn) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.m[key] = value
}

// Get gets the value
func (pm *ProtectedMapUConn) Get(key uint64) (*grpc.ClientConn, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	val, ok := pm.m[key]
	return val, ok
}

// Iterate exposes the underlying map by locking it.
func (pm *ProtectedMapUConn) Iterate() map[uint64]*grpc.ClientConn {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.m
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

// ByteToU64 converts a byte slice to Uint64. The byte should have been created using U64ToByte
func ByteToU64(b []byte) uint64 {
	res := uint64(0)

	res |= uint64(b[0])
	res |= uint64(b[1]) << 8
	res |= uint64(b[2]) << 16
	res |= uint64(b[3]) << 24
	res |= uint64(b[4]) << 32
	res |= uint64(b[5]) << 40
	res |= uint64(b[6]) << 48
	res |= uint64(b[7]) << 56

	return res
}

// U64SliceToByteSlice converts a uint64 slice to a byte slice
func U64SliceToByteSlice(uslice []uint64) []byte {
	var res []byte

	for _, val := range uslice {
		res = append(res, U64ToByte(val)...)
	}

	return res
}

// ByteSliceToU64Slice converts a byte slice to a slice of uint64
func ByteSliceToU64Slice(bslice []byte) []uint64 {
	// TODO
	return nil
}

// BoolToByte converts a bool to byte
func BoolToByte(b bool) byte {
	tmp := uint8(0)
	if b {
		tmp = 1
	}

	return tmp
}

// ByteToBool converts a byte to bool
func ByteToBool(b byte) bool {
	return b > 0
}
