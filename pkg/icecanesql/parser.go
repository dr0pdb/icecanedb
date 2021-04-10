package icecanesql

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
func (p *Parser) Parse() (Statement, error) {
	panic("not implemented")
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
		keyword := keywords[it.val]

		switch keyword {
		case keywordCreate:
		case keywordDrop:
			return p.parseDDL()

		case keywordBegin:
		case keywordCommit:
		case keywordRollback:
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
			return nil, fmt.Errorf("icecanesql::parser::parseStatement: unexpected keyword token %v", keyword)
		}

	default:
		return nil, fmt.Errorf("icecanesql::parser::parseStatement: unexpected token %v - %v; expected a keyword token", it.typ, it.val)
	}

	panic("icecanesql::parser::parseStatement: won't reach here")
}

// parseDDL parses a data definition langauge query.
// It assumes that the first token is a CREATE/DROP keyword
func (p *Parser) parseDDL() (Statement, error) {
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
	case keywordBool:
	case keywordBoolean:
		typ = ColumnTypeBoolean
		break

	case keywordInt:
	case keywordInteger:
		typ = ColumnTypeInteger
		break

	case keywordFloat:
	case keywordDouble:
		typ = ColumnTypeFloat
		break

	case keywordString:
	case keywordText:
	case keywordVarchar:
	case keywordChar:
		typ = ColumnTypeString
		break

	default:
		return nil, fmt.Errorf("expected data type for the column")
	}

	cs := &ColumnSpec{
		Name: colName.val,
		Type: typ,
	}

	return cs, nil
}

func (p *Parser) parseTransaction() (Statement, error) {
	panic("")
}

func (p *Parser) parseSelect() (Statement, error) {
	panic("")
}

func (p *Parser) parseInsert() (Statement, error) {
	panic("")
}

func (p *Parser) parseUpdate() (Statement, error) {
	panic("")
}

func (p *Parser) parseDelete() (Statement, error) {
	panic("")
}

func (p *Parser) parseExplain() (Statement, error) {
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
	it := p.nextToken()
	p.pos-- // revert change to pos
	return it
}

// nextTokenIf returns the next token if it satisfies the given predicate
func (p *Parser) nextTokenIf(pred func(*item) bool) *item {
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
	it := p.nextToken()
	if it.typ == expected {
		return it, nil
	}

	return nil, fmt.Errorf("icecanesql::parser::nextTokenExpect: Expected token %v, Found token %v", expected, it.typ)
}

// nextTokenKeyword returns the next token if it's a keyword.
// it returns an error otherwise
func (p *Parser) nextTokenKeyword() (*item, error) {
	it := p.nextToken()
	if it.typ == itemKeyword {
		return it, nil
	}

	return nil, fmt.Errorf("icecanesql::parser::nextTokenKeyword: Expected keyword token, Found token %v", it.typ)
}

func (p *Parser) nextTokenIdentifier() (*item, error) {
	it := p.nextToken()
	if it.typ == itemIdentifier {
		return it, nil
	}

	return nil, fmt.Errorf("icecanesql::parser::nextTokenIdentifier: Expected identifier token, Found token %v", it.typ)
}
