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

// Result denotes the result of the execution of a query plan
type Result interface {
	HasError() bool

	GetError() error
}

// CreateTableResult is the result of the create table operation
type CreateTableResult struct {
	Err error
}

func (ctr *CreateTableResult) HasError() bool {
	return ctr.Err != nil
}

func (ctr *CreateTableResult) GetError() error {
	return ctr.Err
}

var _ Result = (*CreateTableResult)(nil)

// DropTableResult is the result of the drop table operation
type DropTableResult struct {
	Err error
}

func (ctr *DropTableResult) HasError() bool {
	return ctr.Err != nil
}

func (ctr *DropTableResult) GetError() error {
	return ctr.Err
}

var _ Result = (*DropTableResult)(nil)

// TruncateTableResult is the result of the truncate table operation
type TruncateTableResult struct {
	Err error
}

func (ctr *TruncateTableResult) HasError() bool {
	return ctr.Err != nil
}

func (ctr *TruncateTableResult) GetError() error {
	return ctr.Err
}

var _ Result = (*TruncateTableResult)(nil)

type BeginTxnResult struct {
	TxnID uint64
	Err   error
}

func (ctr *BeginTxnResult) HasError() bool {
	return ctr.Err != nil
}

func (ctr *BeginTxnResult) GetError() error {
	return ctr.Err
}

var _ Result = (*BeginTxnResult)(nil)

type FinishTxnResult struct {
	Err error
}

func (ctr *FinishTxnResult) HasError() bool {
	return ctr.Err != nil
}

func (ctr *FinishTxnResult) GetError() error {
	return ctr.Err
}

var _ Result = (*FinishTxnResult)(nil)

// InsertResult is the result of the insert operation
type InsertResult struct {
	Err error
}

func (ctr *InsertResult) HasError() bool {
	return ctr.Err != nil
}

func (ctr *InsertResult) GetError() error {
	return ctr.Err
}

var _ Result = (*InsertResult)(nil)
