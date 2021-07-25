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
	"github.com/dr0pdb/icecanedb/pkg/frontend"
	log "github.com/sirupsen/logrus"
)

// InsertExecutor is the executor for the insert query
type InsertExecutor struct {
	rpc   *rpcRepository
	Table *frontend.TableSpec
}

var _ Executor = (*InsertExecutor)(nil)

// Execute executes the insert statement
func (ex *InsertExecutor) Execute(txnID uint64) Result {
	log.Info("icecanesql::dml_executor::InsertExecutor.Execute; start;")

	return nil
}
