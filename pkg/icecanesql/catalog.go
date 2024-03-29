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

package icecanesql

import (
	"sync"

	"github.com/dr0pdb/icecanedb/pkg/frontend"
)

// catalog contains useful metadata info which are used while
// query planning and optimizations
type catalog struct {
	mu  *sync.RWMutex
	rpc *rpcRepository

	schemaCache map[string]*frontend.TableSpec
}

// returns info of the table if the given name
func (c *catalog) getTableInfo(tableName string, txnID uint64) (*frontend.TableSpec, error) {
	// aquire read lock since schema should be present most of the times
	c.mu.RLock()
	if spec, ok := c.schemaCache[tableName]; ok {
		c.mu.RUnlock()
		return spec, nil
	}
	c.mu.RUnlock()

	k := getTableKeyForQuery(tableName)
	qk, qv, err := c.rpc.get(k, txnID)
	if err != nil {
		return nil, err
	}

	spec, err := decodeTableSchema(qk, qv)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.schemaCache[tableName] = spec
	return spec, err
}

func newCatalog(rpc *rpcRepository) *catalog {
	return &catalog{
		rpc:         rpc,
		mu:          new(sync.RWMutex),
		schemaCache: make(map[string]*frontend.TableSpec),
	}
}
