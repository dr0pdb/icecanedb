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

type Operator uint64

const (
	OperatorEqual              Operator = iota // '='
	OperatorGreaterThan                        // '>'
	OperatorLessThan                           // '<'
	OperatorPlus                               // '+'
	OperatorMinus                              // '-'
	OperatorAsterisk                           // '*'
	OperatorSlash                              // '/'
	OperatorCaret                              // '^'
	OperatorPercent                            // '%'
	OperatorExclamation                        // '!'
	OperatorQuestionMark                       // '?'
	OperatorNotEqual                           // "!="
	OperatorLessThanEqualTo                    // "<="
	OperatorGreaterThanEqualTo                 // ">="
	OperatorAndAnd                             // "&&"
	OperatorOrOr                               // "||"
)

var (
	itemTypeToOperator map[itemType]Operator = map[itemType]Operator{
		itemEqual:              OperatorEqual,
		itemGreaterThan:        OperatorGreaterThan,
		itemLessThan:           OperatorLessThan,
		itemPlus:               OperatorPlus,
		itemMinus:              OperatorMinus,
		itemAsterisk:           OperatorAsterisk,
		itemSlash:              OperatorSlash,
		itemCaret:              OperatorCaret,
		itemPercent:            OperatorPercent,
		itemExclamation:        OperatorExclamation,
		itemQuestionMark:       OperatorQuestionMark,
		itemNotEqual:           OperatorNotEqual,
		itemLessThanEqualTo:    OperatorLessThanEqualTo,
		itemGreaterThanEqualTo: OperatorGreaterThanEqualTo,
		itemAndAnd:             OperatorAndAnd,
		itemOrOr:               OperatorOrOr,
	}
)
