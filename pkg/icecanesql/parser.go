package icecanesql

// Parser is responsible for parsing the sql string to AST
type Parser struct {
	name  string // only for error reporting and debugging
	lexer *lexer // the lexical scanner
}

// Parse the input to an AST
func (p *Parser) Parse() Statement {
	panic("not implemented")
}

// nextToken returns the next item from the lexer
func (p *Parser) nextToken() *item {
	it := p.lexer.nextItem()
	return &it
}

func (p *Parser) nextTokenIf() *item {
	panic("not implemented")
}

func NewParser(name, input string) *Parser {
	lex, _ := newLexer(name, input)

	return &Parser{
		name:  name,
		lexer: lex,
	}
}
