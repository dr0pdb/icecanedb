/**
 * Copyright 2021 The IcecaneDB Authors. All rights reserved.
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

package mvcc

import (
	"testing"

	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

func TestTxnValuesSet(t *testing.T) {
	for i := uint64(0); i < 5; i++ {
		svalue := newSetTxnValue(test.TestValues[i])
		assert.Equal(t, test.TestValues[i], svalue.getUserValue(), "decoded value doesn't match")
		assert.False(t, svalue.getDeleteFlag(), "decoded flag as delete but expected a set")
	}
}

func TestTxnValuesDelete(t *testing.T) {
	for i := uint64(0); i < 5; i++ {
		svalue := newDeleteTxnValue()
		assert.True(t, svalue.getDeleteFlag(), "decoded flag as set but expected a delete")
	}
}
