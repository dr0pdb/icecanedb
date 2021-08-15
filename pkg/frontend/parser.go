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

package frontend

import (
	"fmt"
	"strconv"
	"strings"
)

// TODO: refactor this according to this guide: https://blog.golang.org/errors-are-values

// Parser is responsible for parsing the sql string to AST
type Parser struct {
	name  string // only for error reporting and debugging
	lexer *lexer // the lexical scanner

	items []*item // buffered tokens from the lexer for peeking
	pos   int     // next item position in the items buffer

	err error // any error encountered during the parsing process
}

//
// Public functions
//

// Parse the input to an AST
func (p *Parser) Parse() (st Statement, err error) {
	st, _ = p.parseStatement()
	p.nextTokenExpect(itemSemicolon)
	_, err = p.nextTokenExpect(itemEOF)
	return st, err
}

// NewParser creates a parser for the given input
func NewParser(name, input string) *Parser {
	lex, _ := newLexer(name, input)

	return &Parser{
		name:  name,
		lexer: lex,
		items: make([]*item, 0),
	}
}

//
// Internal functions
//

// parseStatements parses a sql statement.
// starting point of the core parsing process.
func (p *Parser) parseStatement() (Statement, error) {
	it := p.peek()

	switch it.typ {
	case itemKeyword:
		keyword := keywords[strings.ToUpper(it.val)]

		switch keyword {
		case keywordCreate, keywordDrop, keywordTruncate:
			return p.parseDDL()

		case keywordBegin, keywordCommit, keywordRollback:
			return p.parseTransaction()

		case keywordInsert:
			return p.parseInsert()
		case keywordUpdate:
			return p.parseUpdate()
		case keywordDelete:
			return p.parseDelete()
		case keywordSelect:
			return p.parseSelect()

		case keywordExplain:
			return p.parseExplain()

		default:
			return nil, fmt.Errorf("icecanesql::parser::parseStatement: unexpected keyword token %v", it.val)
		}

	default:
		return nil, fmt.Errorf("icecanesql::parser::parseStatement: unexpected token %v - %v; expected a keyword token", it.typ, it.val)
	}
}

// parseDDL parses a data definition langauge query.
// It assumes that the first token is a CREATE/DROP/TRUNCATE keyword
func (p *Parser) parseDDL() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	action := p.nextToken() // has to be CREATE/DROP
	table := p.nextToken()

	if !isKeyword(table, keywordTable) {
		return nil, fmt.Errorf("icecanesql::parser::parseDDL: expected keyword \"TABLE\"")
	}

	tableName, err := p.nextTokenIdentifier()
	if err != nil {
		return nil, fmt.Errorf("icecanesql::parser::parseDDL: expected table name")
	}

	if isKeyword(action, keywordCreate) {
		_, err = p.nextTokenExpect(itemLeftParen)
		if err != nil {
			return nil, err
		}

		var cols []*ColumnSpec
		for {
			col, err := p.parseSingleColumnSpec()
			if err != nil {
				return nil, err
			}
			cols = append(cols, col)

			comma := p.nextTokenIf(func(it *item) bool {
				return it.typ == itemComma
			})
			if comma == nil { // last column spec
				break
			}
		}

		_, err = p.nextTokenExpect(itemRightParen)
		if err != nil {
			return nil, err
		}

		spec := NewTableSpec(0, tableName.val, cols)
		stmt := &CreateTableStatement{
			Spec: spec,
		}
		return stmt, nil
	} else if isKeyword(action, keywordDrop) {
		stmt := &DropTableStatement{TableName: tableName.val}
		return stmt, nil
	}

	stmt := &TruncateTableStatement{TableName: tableName.val}
	return stmt, nil
}

func (p *Parser) parseSingleColumnSpec() (*ColumnSpec, error) {
	if p.err != nil {
		return nil, p.err
	}

	colName, err := p.nextTokenIdentifier()
	if err != nil {
		return nil, err
	}

	colType, err := p.nextTokenKeyword()
	if err != nil {
		return nil, err
	}

	var typ FieldType
	switch keywords[strings.ToUpper(colType.val)] {
	case keywordBool, keywordBoolean:
		typ = FieldTypeBoolean

	case keywordInt, keywordInteger:
		typ = FieldTypeInteger

	case keywordFloat, keywordDouble:
		typ = FieldTypeFloat

	case keywordString, keywordText, keywordVarchar, keywordChar:
		typ = FieldTypeString

	default:
		return nil, fmt.Errorf("expected data type for the column")
	}

	cs := &ColumnSpec{
		Name:     colName.val,
		Type:     typ,
		Nullable: true, // true by default
	}

	// column constraints such as nullable, unique..
	for {
		kwd := p.nextTokenIf(func(i *item) bool {
			return i.typ == itemKeyword
		})
		if kwd == nil {
			break
		}

		switch keywords[strings.ToUpper(kwd.val)] {
		case keywordUnique:
			cs.Unique = true

		case keywordIndex:
			cs.Index = true

		case keywordNot:
			null, err := p.nextTokenKeyword()
			if err != nil || (null != nil && keywords[strings.ToUpper(null.val)] != keywordNull) {
				return nil, fmt.Errorf("expected keyword NULL after NOT")
			}
			cs.Nullable = false

		case keywordNull:
			cs.Nullable = true

		case keywordPrimary:
			key, err := p.nextTokenKeyword()
			if err != nil || (key != nil && keywords[strings.ToUpper(key.val)] != keywordKey) {
				return nil, fmt.Errorf("expected keyword KEY after PRIMARY")
			}
			cs.PrimaryKey = true

		case keywordReferences:
			table, err := p.nextTokenIdentifier()
			if err != nil {
				return nil, fmt.Errorf("expected table identifier after REFERENCES")
			}
			cs.References = table.val

		case keywordDefault:
			exp, err := p.parseExpression()
			if err != nil {
				return nil, fmt.Errorf("expected expression after keyword DEFAULT. Found err: %V", err)
			}
			cs.Default = exp

		default:
			return nil, fmt.Errorf("unknown keyword %s in the column specification", kwd.val)
		}
	}

	return cs, nil
}

// parseExpression parses an expression
// Grammar is based on: http://www.craftinginterpreters.com/parsing-expressions.html#recursive-descent-parsing
func (p *Parser) parseExpression() (Expression, error) {
	return p.parseBitOperations()
}

// AND, OR, &&, ||
func (p *Parser) parseBitOperations() (Expression, error) {
	if p.err != nil {
		return nil, p.err
	}

	ex, err := p.parseEquality()

	for {
		if p.err != nil {
			return nil, p.err
		}

		op := p.nextTokenIf(func(i *item) bool {
			if i.typ == itemOrOr || i.typ == itemAndAnd {
				return true
			}

			return isKeyword(i, keywordAnd) || isKeyword(i, keywordOr)
		})
		if op == nil {
			break
		}

		right, err := p.parseEquality()
		if err != nil {
			return nil, err
		}

		if op.typ == itemKeyword {
			k := keywords[op.val]

			if k == keywordAnd {
				ex = &BinaryOpExpression{Op: OperatorAndAnd, L: ex, R: right}
			} else if k == keywordOr {
				ex = &BinaryOpExpression{Op: OperatorOrOr, L: ex, R: right}
			} else {
				p.err = fmt.Errorf("unknown keyword %s found. expected an operator", op.val)
			}
		} else {
			ex = &BinaryOpExpression{Op: itemTypeToOperator[op.typ], L: ex, R: right}
		}
	}

	return ex, err
}

func (p *Parser) parseEquality() (Expression, error) {
	if p.err != nil {
		return nil, p.err
	}

	ex, err := p.parseComparison()

	for {
		if p.err != nil {
			return nil, p.err
		}

		op := p.nextTokenIf(func(i *item) bool {
			return i.typ == itemNotEqual || i.typ == itemEqual
		})
		if op == nil {
			break
		}

		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}

		ex = &BinaryOpExpression{Op: itemTypeToOperator[op.typ], L: ex, R: right}
	}

	return ex, err
}

func (p *Parser) parseComparison() (Expression, error) {
	if p.err != nil {
		return nil, p.err
	}

	ex, err := p.parseTerm()

	for {
		if p.err != nil {
			return nil, p.err
		}

		op := p.nextTokenIf(func(i *item) bool {
			return i.typ == itemGreaterThan || i.typ == itemGreaterThanEqualTo || i.typ == itemLessThan || i.typ == itemLessThanEqualTo
		})
		if op == nil {
			break
		}

		right, err := p.parseTerm()
		if err != nil {
			return nil, err
		}

		ex = &BinaryOpExpression{Op: itemTypeToOperator[op.typ], L: ex, R: right}
	}

	return ex, err
}

func (p *Parser) parseTerm() (Expression, error) {
	if p.err != nil {
		return nil, p.err
	}

	ex, err := p.parseFactor()

	for {
		if p.err != nil {
			return nil, p.err
		}

		op := p.nextTokenIf(func(i *item) bool {
			return i.typ == itemMinus || i.typ == itemPlus
		})
		if op == nil {
			break
		}

		right, err := p.parseFactor()
		if err != nil {
			return nil, err
		}

		ex = &BinaryOpExpression{Op: itemTypeToOperator[op.typ], L: ex, R: right}
	}

	return ex, err
}

func (p *Parser) parseFactor() (Expression, error) {
	if p.err != nil {
		return nil, p.err
	}

	ex, err := p.parseUnary()

	for {
		if p.err != nil {
			return nil, p.err
		}

		op := p.nextTokenIf(func(i *item) bool {
			return i.typ == itemAsterisk || i.typ == itemSlash
		})
		if op == nil {
			break
		}

		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		ex = &BinaryOpExpression{Op: itemTypeToOperator[op.typ], L: ex, R: right}
	}

	return ex, err
}

func (p *Parser) parseUnary() (Expression, error) {
	if p.err != nil {
		return nil, p.err
	}

	// ! or -
	op := p.nextTokenIf(func(i *item) bool {
		return i.typ == itemExclamation || i.typ == itemMinus
	})
	if op != nil {
		ex, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return &UnaryOpExpression{Op: itemTypeToOperator[op.typ], Exp: ex}, nil
	}

	return p.parsePrimary()
}

func (p *Parser) parsePrimary() (Expression, error) {
	if p.err != nil {
		return nil, p.err
	}

	op := p.nextTokenIf(func(i *item) bool {
		return i.typ == itemFalse || i.typ == itemTrue || i.typ == itemInteger || i.typ == itemFloat || i.typ == itemString || i.typ == itemLeftParen || i.typ == itemIdentifier || i.typ == itemAsterisk
	})
	if op != nil {
		if op.typ == itemLeftParen {
			in, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			_, err = p.nextTokenExpect(itemRightParen)
			if err != nil {
				return nil, err
			}

			return &GroupingExpression{InExp: in}, nil
		} else if op.typ == itemIdentifier {
			exp := &IdentifierExpression{
				Identifier: op.val,
			}
			return exp, nil
		} else if op.typ == itemAsterisk {
			return &IdentifierExpression{
				Identifier: "*",
			}, nil
		} else {
			exp := &ValueExpression{
				Val: &Value{},
			}

			switch op.typ {
			case itemFalse, itemTrue:
				val, err := strconv.ParseBool(op.val)
				if err != nil {
					return nil, err
				}

				exp.Val.Typ = FieldTypeBoolean
				exp.Val.Val = val

			case itemInteger:
				val, err := strconv.ParseInt(op.val, 10, 64)
				if err != nil {
					return nil, err
				}

				exp.Val.Typ = FieldTypeInteger
				exp.Val.Val = val
			case itemFloat:
				val, err := strconv.ParseFloat(op.val, 64)
				if err != nil {
					return nil, err
				}

				exp.Val.Typ = FieldTypeFloat
				exp.Val.Val = val

			case itemString:
				exp.Val.Typ = FieldTypeString
				exp.Val.Val = op.val
			}

			return exp, nil
		}
	}

	return nil, fmt.Errorf("expected a primary expression")
}

// parseTransaction parses a transaction statement
func (p *Parser) parseTransaction() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	colType, err := p.nextTokenKeyword()
	if err != nil {
		return nil, err
	}

	var st Statement
	switch keywords[strings.ToUpper(colType.val)] {
	case keywordBegin:
		btst := &BeginTxnStatement{}
		st = btst

		kwd, err := p.nextTokenKeyword()
		if err != nil {
			break
		}

		switch keywords[strings.ToUpper(kwd.val)] {
		case keywordRead: // [ READ ONLY | READ WRITE ]
			kwd2, err := p.nextTokenKeyword()
			if err != nil || (kwd2 != nil && keywords[strings.ToUpper(kwd2.val)] != keywordWrite && keywords[strings.ToUpper(kwd2.val)] != keywordOnly) {
				return nil, fmt.Errorf("expected keyword WRITE/ONLY after READ in transaction spec")
			}

			if keywords[strings.ToUpper(kwd2.val)] == keywordWrite {
				btst.ReadOnly = true
			}

		default:
			return nil, fmt.Errorf("unknown keyword %s in the transaction specification", kwd.val)
		}

		st = btst

	case keywordCommit:
		st = &FinishTxnStatement{IsCommit: true}

	case keywordRollback:
		st = &FinishTxnStatement{IsCommit: false}

	}

	return st, nil
}

func (p *Parser) parseSelect() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	expr := &SelectStatement{}
	_ = p.nextToken() // SELECT

	// get selections - list of (expr * [AS output_name])
	var selections []*SelectionItem
	for {
		exp, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		if iexp, ok := exp.(*IdentifierExpression); ok {
			selections = append(selections, &SelectionItem{Expr: iexp})

			comma := p.nextTokenIf(func(it *item) bool {
				return it.typ == itemComma
			})
			if comma == nil { // last value
				break
			}
		} else {
			p.err = fmt.Errorf("expected an identifier in selection item")
			return nil, p.err
		}
	}
	expr.Selections = selections

	from := p.nextToken()
	if !isKeyword(from, keywordFrom) {
		p.err = fmt.Errorf("icecanesql::parser::parseDDL: expected keyword \"FROM\"")
		return nil, p.err
	}
	fromItem := &Table{}
	tableName, err := p.nextTokenExpect(itemIdentifier)
	if err != nil {
		return nil, err
	}
	fromItem.Name = tableName.val
	asToken := p.nextTokenIf(func(it *item) bool {
		return isKeyword(it, keywordAs)
	})
	if asToken != nil {
		outputName, err := p.nextTokenExpect(itemIdentifier)
		if err != nil {
			return nil, err
		}
		fromItem.Alias = outputName.val
	}
	expr.From = fromItem
	// TODO: support JOIN

	// WHERE clause
	whereToken := p.nextTokenIf(func(it *item) bool {
		return isKeyword(it, keywordWhere)
	})
	if whereToken != nil {
		whereExpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		expr.Where = whereExpr
	}

	// TODO: support LIMIT

	return expr, p.err
}

func (p *Parser) parseInsert() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	_ = p.nextToken() // has to be INSERT
	into := p.nextToken()
	if !isKeyword(into, keywordInto) {
		p.err = fmt.Errorf("icecanesql::parser::parseInsert: expected keyword \"INTO\" after \"INSERT\"")
		return nil, p.err
	}

	tableName, err := p.nextTokenIdentifier()
	if err != nil {
		p.err = fmt.Errorf("icecanesql::parser::parseInsert: expected table name")
		return nil, p.err
	}

	// column names
	leftParen := p.nextTokenIf(func(it *item) bool {
		return it.typ == itemLeftParen
	})
	var columns []string
	if leftParen != nil {
		for {
			colName, err := p.nextTokenIdentifier()
			if err != nil {
				return nil, err
			}
			columns = append(columns, colName.val)

			comma := p.nextTokenIf(func(it *item) bool {
				return it.typ == itemComma
			})
			if comma == nil { // last value
				break
			}
		}

		_, err = p.nextTokenExpect(itemRightParen)
		if err != nil {
			return nil, p.err
		}
	}

	values := p.nextToken()
	if !isKeyword(values, keywordValues) {
		p.err = fmt.Errorf("icecanesql::parser::parseInsert: expected keyword \"VALUES\" after table name")
		return nil, p.err
	}

	_, err = p.nextTokenExpect(itemLeftParen)
	if err != nil {
		return nil, err
	}

	stmt := &InsertStatement{Table: &Table{Name: tableName.val}, Columns: columns}

	// values for each column
	var vals []Expression
	for {
		exp, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		vals = append(vals, exp)

		comma := p.nextTokenIf(func(it *item) bool {
			return it.typ == itemComma
		})
		if comma == nil { // last value
			break
		}
	}
	_, err = p.nextTokenExpect(itemRightParen)
	stmt.Values = vals
	return stmt, err
}

func (p *Parser) parseUpdate() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	_ = p.nextToken() // has to be UPDATE
	expr := &UpdateStatement{}

	tableName, err := p.nextTokenIdentifier()
	if err != nil {
		p.err = fmt.Errorf("icecanesql::parser::parseUpdate: expected table name")
		return nil, p.err
	}
	expr.Table = &Table{Name: tableName.val}

	set := p.nextToken()
	if !isKeyword(set, keywordSet) {
		p.err = fmt.Errorf("icecanesql::parser::parseUpdate: expected keyword \"SET\" after table name")
		return nil, p.err
	}

	var vals []Expression
	for {
		exp, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		vals = append(vals, exp)

		comma := p.nextTokenIf(func(it *item) bool {
			return it.typ == itemComma
		})
		if comma == nil { // last value
			break
		}
	}
	expr.Values = vals

	// predicate - WHERE
	whereToken := p.nextTokenIf(func(it *item) bool {
		return isKeyword(it, keywordWhere)
	})
	if whereToken != nil {
		whereExpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		expr.Predicate = whereExpr
	}

	return expr, nil
}

func (p *Parser) parseDelete() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	_ = p.nextToken() // has to be DELETE
	expr := &DeleteStatement{}

	set := p.nextToken()
	if !isKeyword(set, keywordFrom) {
		p.err = fmt.Errorf("icecanesql::parser::parseInsert: expected keyword \"FROM\" after DELETE")
		return nil, p.err
	}

	tableName, err := p.nextTokenIdentifier()
	if err != nil {
		p.err = fmt.Errorf("icecanesql::parser::parseInsert: expected table name")
		return nil, p.err
	}
	expr.Table = &Table{Name: tableName.val}

	// predicate - WHERE
	whereToken := p.nextTokenIf(func(it *item) bool {
		return isKeyword(it, keywordWhere)
	})
	if whereToken != nil {
		whereExpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		expr.Predicate = whereExpr
	}

	return expr, nil
}

func (p *Parser) parseExplain() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	_ = p.nextToken() // has to be EXPLAIN
	expr := &ExplainStatement{}

	inner, err := p.parseStatement()
	expr.InnerStatement = inner
	return expr, err
}

// isKeyword checks if the given item is the given keyword or not
func isKeyword(it *item, key keywordType) bool {
	if it != nil && it.typ == itemKeyword && keywords[strings.ToUpper(it.val)] == key {
		return true
	}

	return false
}

// nextToken returns the next item from the lexer
// it consumes the item by incrementing pos
// NOTE: It ignores the whitespace token
func (p *Parser) nextToken() *item {
	if p.pos < len(p.items) {
		p.pos++
		return p.items[p.pos-1]
	}

	if p.pos > len(p.items) {
		panic("icecanesql::parser::nextToken: invalid value of pos. exceeded length of buffered entries")
	}

	var it item
	for {
		it = p.lexer.nextItem()
		if it.typ != itemWhitespace {
			p.items = append(p.items, &it)
			p.pos++
			break
		}
	}

	return &it
}

// peek peeks the next item from the lexer but doesn't consume it.
func (p *Parser) peek() *item {
	if p.err != nil {
		return nil
	}

	it := p.nextToken()
	p.pos-- // revert change to pos
	return it
}

// nextTokenIf returns the next token if it satisfies the given predicate
// if the given predicate is satisfied, the parser is advanced otherwise not
func (p *Parser) nextTokenIf(pred func(*item) bool) *item {
	if p.err != nil {
		return nil
	}

	it := p.peek()

	if pred(it) {
		p.nextToken() // advance pos
		return it
	}

	return nil
}

// nextTokenExpect returns the next token if it's of the expected type.
// it throws an error otherwise
func (p *Parser) nextTokenExpect(expected itemType) (*item, error) {
	if p.err != nil {
		return nil, p.err
	}

	it := p.nextToken()
	if it.typ == expected {
		return it, nil
	}

	p.err = fmt.Errorf("icecanesql::parser::nextTokenExpect: Expected token %v, Found token %v", expected, it.typ)
	return nil, p.err
}

// nextTokenKeyword peeks and returns the next token if it's a keyword.
// it returns an error otherwise
func (p *Parser) nextTokenKeyword() (*item, error) {
	if p.err != nil {
		return nil, p.err
	}

	it := p.peek()
	if it.typ == itemKeyword {
		p.nextToken()
		return it, nil
	}

	p.err = fmt.Errorf("icecanesql::parser::nextTokenKeyword: Expected keyword token, Found item %v", it)
	return nil, p.err
}

func (p *Parser) nextTokenIdentifier() (*item, error) {
	if p.err != nil {
		return nil, p.err
	}

	it := p.peek()
	if it.typ == itemIdentifier {
		p.nextToken()
		return it, nil
	}

	p.err = fmt.Errorf("icecanesql::parser::nextTokenIdentifier: Expected identifier token, Found item %v", it)
	return nil, p.err
}
