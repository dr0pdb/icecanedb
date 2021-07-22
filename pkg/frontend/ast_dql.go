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

package frontend

var (
	_ Statement = (*SelectStatement)(nil)
)

type SelectStatement struct {
	Selections []*SelectionItem
	Distinct   bool
	From       FromItem
	Where      Expression // must evaluate to a boolean
	Limit      Expression // must evaluate to an integer
}

func (cts *SelectStatement) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (cts *SelectStatement) statement() {}
