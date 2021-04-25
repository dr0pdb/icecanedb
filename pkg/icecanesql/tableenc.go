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

package icecanesql

import (
	"github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/pkg/frontend"
)

// encodeTableSchema returns the key-value pair to store in the kv service
// to store a table schema.
//
// The format:
// Key: table name
// Value: id (64 bits) | number of columns (64 bits) | col_name (string) | col_type (64 bits) | ...
func encodeTableSchema(table *frontend.TableSpec) (key, value []byte, err error) {
	key = []byte(table.TableName)

	value = common.U64ToByte(uint64(len(table.Columns)))
	for i := 0; i < len(table.Columns); i++ {
		// encode the column spec
	}

	return key, value, err
}

// encodeString encodes the string to bytes
func encodeString(s string) []byte {
	res := make([]byte, 0)

	res = append(res, common.U64ToByte(uint64(len(s)))...)
	res = append(res, []byte(s)...)

	return res
}
