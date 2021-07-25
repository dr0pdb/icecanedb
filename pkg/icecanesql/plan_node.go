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

// PlanNode is the query execution plan generated by the planner
// It is used by the executor to execute the query.
type PlanNode interface{}

var _ PlanNode = (*CreateTablePlanNode)(nil)
var _ PlanNode = (*DropTablePlanNode)(nil)
var _ PlanNode = (*TruncateTablePlanNode)(nil)
var _ PlanNode = (*BeginTxnPlanNode)(nil)
var _ PlanNode = (*FinishTxnPlanNode)(nil)
var _ PlanNode = (*InsertPlanNode)(nil)

// CreateTablePlanNode is the planner node for the create table query
type CreateTablePlanNode struct {
	Schema *frontend.TableSpec
}

// DropTablePlanNode is the planner node for the drop table query
type DropTablePlanNode struct {
	TableName string
}

// TruncateTablePlanNode is the planner node for the truncate table query
type TruncateTablePlanNode struct {
	TableName string
}

type BeginTxnPlanNode struct {
	ReadOnly bool
}

type FinishTxnPlanNode struct {
	IsCommit bool
}

// InsertPlanNode is the planner node for the insert table query
type InsertPlanNode struct {
	TableName string
	Columns   []string
	Values    []frontend.Expression
}
