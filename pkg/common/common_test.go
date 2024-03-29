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
	"fmt"
	"testing"

	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

func TestUint64BytesConversion(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	n := uint64(121)
	b := U64ToByteSlice(n)
	assert.Equal(t, n, ByteSliceToU64(b), fmt.Sprintf("Unexpected error in uint64-bytes comparison; expected %d actual %d", n, ByteSliceToU64(b)))
}

func TestBoolByteConversion(t *testing.T) {
	b := BoolToByte(true)
	assert.True(t, ByteToBool(b), "unexpected value of decoded b. expected true, found false")

	b = BoolToByte(false)
	assert.False(t, ByteToBool(b), "unexpected value of decoded b. expected false, found true")
}

func TestFloat64ByteConversion(t *testing.T) {
	f := float64(32.01)

	b := Float64ToByteSlice(f)
	df := ByteSliceToFloat64(b)

	assert.Equal(t, f, df, "unexpected value of decoded float64")
}
