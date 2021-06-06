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

import "github.com/dr0pdb/icecanedb/pkg/frontend"

// Validator validates the parsed AST for inconsistencies
type validator interface {
	validate() error
}

var _ validator = (*createTableValidator)(nil)
var _ validator = (*dropTableValidator)(nil)
var _ validator = (*truncateTableValidator)(nil)

// newValidator creates a new validator for the parsed statement
func newValidator(ast frontend.Statement) validator {
	switch st := ast.(type) {
	case *frontend.CreateTableStatement:
		return &createTableValidator{ast: st}
	case *frontend.DropTableStatement:
		return &dropTableValidator{ast: st}
	case *frontend.TruncateTableStatement:
		return &truncateTableValidator{ast: st}
	}

	panic("")
}

// createTableValidator validates a create table statement
type createTableValidator struct {
	ast *frontend.CreateTableStatement
}

func (ctv *createTableValidator) validate() error {
	return nil
}

// dropTableValidator validates a delete table statement
type dropTableValidator struct {
	ast *frontend.DropTableStatement
}

func (ctv *dropTableValidator) validate() error {
	return nil
}

// truncateTableValidator validates a truncate table statement
type truncateTableValidator struct {
	ast *frontend.TruncateTableStatement
}

func (ctv *truncateTableValidator) validate() error {
	return nil
}
