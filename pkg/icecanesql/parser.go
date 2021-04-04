package icecanesql

import (
	"fmt"
)

// Parser is responsible for parsing the sql string to AST
type Parser struct {
	name  string // only for error reporting and debugging
	lexer *lexer // the lexical scanner

	items []*item // buffered tokens from the lexer for peeking
	pos   int     // next item position in the items buffer
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
			return p.parseDll()

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

func (p *Parser) parseDll() (Statement, error) {
	panic("")
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
