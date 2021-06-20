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

import "fmt"

func (i item) String() string {
	switch i.typ {
	case itemError:
		return i.val
	case itemEOF:
		return "EOF"
	case itemWhitespace:
		return "WHITESPACE"
	}

	// limit to 10 characters if it's too long
	if len(i.val) > 10 {
		return fmt.Sprintf("%.10q...", i.val)
	}

	return fmt.Sprintf("%q", i.val)
}

func (it itemType) String() string {
	switch it {
	case itemError:
		return "Error"
	case itemEOF:
		return "EOF"
	case itemWhitespace:
		return "WHITESPACE"
	case itemSingleLineComment:
		return "SingleLineComment"

	// literals
	case itemIdentifier:
		return "Identifier"
	case itemTrue:
		return "true"
	case itemFalse:
		return "false"
	case itemInteger:
		return "Integer"
	case itemFloat:
		return "Float"
	case itemString:
		return "String"
	case itemKeyword:
		return "Keyword"

	// symbols
	case itemPeriod:
		return "Period"
	case itemComma:
		return "COMMA"
	case itemLeftParen:
		return "LeftParen"
	case itemRightParen:
		return "RightParen"
	case itemSemicolon:
		return "Semicolon"

	// operators
	case itemEqual:
		return "Equal"
	case itemGreaterThan:
		return "GreaterThan"
	case itemLessThan:
		return "LessThan"
	case itemPlus:
		return "Plus"
	case itemMinus:
		return "Minus"
	case itemAsterisk:
		return "Asterisk"
	case itemSlash:
		return "Slash"
	case itemCaret:
		return "Caret"
	case itemPercent:
		return "Percent"
	case itemExclamation:
		return "Exclamation"
	case itemQuestionMark:
		return "QuestionMark"
	case itemNotEqual:
		return "NotEqual"
	case itemLessThanEqualTo:
		return "itemLessThanEqualTo"
	case itemGreaterThanEqualTo:
		return "itemGreaterThanEqualTo"
	case itemAndAnd:
		return "itemAndAnd"
	case itemOrOr:
		return "itemOrOr"
	}

	return ""
}
