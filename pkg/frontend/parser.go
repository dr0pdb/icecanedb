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
		case keywordCreate, keywordDrop:
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
// It assumes that the first token is a CREATE/DROP keyword
func (p *Parser) parseDDL() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	action := p.nextToken() // has to be CREATE/DROP
	table := p.nextToken()

	if !isKeyword(table, keywordTable) {
		return nil, fmt.Errorf("icecanesql::parser::parseDDL: expected keyword \"TABLE\"") // throw error
	}

	tableName, err := p.nextTokenIdentifier()
	if err != nil {
		return nil, fmt.Errorf("icecanesql::parser::parseDDL: expected table name")
	}

	if isKeyword(action, keywordCreate) {
		spec := &TableSpec{
			TableName: tableName.val,
		}

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

		spec.Columns = cols
		stmt := &CreateTableStatement{
			Spec: spec,
		}
		return stmt, nil
	}

	stmt := &DropTableStatement{TableName: tableName.val}
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

	var typ ColumnType
	switch keywords[strings.ToUpper(colType.val)] {
	case keywordBool, keywordBoolean:
		typ = ColumnTypeBoolean

	case keywordInt, keywordInteger:
		typ = ColumnTypeInteger

	case keywordFloat, keywordDouble:
		typ = ColumnTypeFloat

	case keywordString, keywordText, keywordVarchar, keywordChar:
		typ = ColumnTypeString

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
		kwd, err := p.nextTokenKeyword()
		if err != nil {
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

		default:
			return nil, fmt.Errorf("unknown keyword %s in the column specification", kwd.val)
		}
	}

	return cs, nil
}

func (p *Parser) parseTransaction() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	panic("")
}

func (p *Parser) parseSelect() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	panic("")
}

func (p *Parser) parseInsert() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	panic("")
}

func (p *Parser) parseUpdate() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	panic("")
}

func (p *Parser) parseDelete() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	panic("")
}

func (p *Parser) parseExplain() (Statement, error) {
	if p.err != nil {
		return nil, p.err
	}

	panic("")
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

	return nil, fmt.Errorf("icecanesql::parser::nextTokenExpect: Expected token %v, Found token %v", expected, it.typ)
}

// nextTokenKeyword returns the next token if it's a keyword.
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

	return nil, fmt.Errorf("icecanesql::parser::nextTokenKeyword: Expected keyword token, Found item %v", it)
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

	return nil, fmt.Errorf("icecanesql::parser::nextTokenIdentifier: Expected identifier token, Found item %v", it)
}
