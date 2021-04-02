package icecanesql

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

//
// This lexer is based on the design of the lexer in the Go template engine.
// For more check this presentation by Rob Pike: https://www.youtube.com/watch?v=HxaD_trXwRE
//

// item represents a single token
type item struct {
	typ itemType
	val string
}

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

// itemType is a SQL token type
type itemType int

const (
	itemError itemType = iota
	itemEOF
	itemWhitespace
	itemSingleLineComment // --

	// literals
	itemIdentifier // field name, table name
	itemNumber
	itemString  // "hello"
	itemKeyword // SELECT, INSERT, ..

	// symbols
	itemPeriod     // '.'
	itemComma      // ','
	itemLeftParen  // '('
	itemRightParen // ')'
	itemSemicolon  // ';'

	// operators
	itemEqual              // '='
	itemGreaterThan        // '>'
	itemLessThan           // '<'
	itemPlus               // '+'
	itemMinus              // '-'
	itemAsterisk           // '*'
	itemSlash              // '/'
	itemCaret              // '^'
	itemPercent            // '%'
	itemExclamation        // '!'
	itemQuestionMark       // '?'
	itemNotEqual           // "!="
	itemLessThanEqualTo    // "<="
	itemGreaterThanEqualTo // ">="
)

const eof = -1

// set of keywords
var keywords = map[string]bool{
	"CREATE":   true,
	"DATABASE": true,
	"TABLE":    true,
	"SELECT":   true,
	"INSERT":   true,
	"DELETE":   true,
	"UPDATE":   true,
	"ALTER":    true,
	"WHERE":    true,
	"ORDER":    true,
	"BY":       true,
	"FROM":     true,

	// data types
	"INT":     true,
	"VARCHAR": true,
}

// lexer is the sql lexer state machine responsible for tokenizing the input.
type lexer struct {
	name  string    // for error reporting
	input string    // the string being scanned right now
	start int       // start position of the current item
	pos   int       // current position in the input
	width int       // width of last token read from the input
	items chan item // channel of scanned items. tokens are emitted via this
}

// stateFn is a function that takes a lexer and returns the new stateFn
type stateFn func(*lexer) stateFn

// predFn is a function to do predicate based filtering/traversal
type predFn func(rune) bool

//
// Helper functions
//

// next returns the next rune in the input.
func (l *lexer) next() (r rune) {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}
	r, l.width =
		utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += l.width
	return r
}

// ignore skips over the pending input before this point.
func (l *lexer) ignore() {
	l.start = l.pos
}

// backup steps back one rune.
// Can be called only once per call of next.
func (l *lexer) backup() {
	l.pos -= l.width
}

func (l *lexer) backupBy(length int) {
	if l.pos < length {
		panic(fmt.Errorf("icecanesql::lexer::backupBy: tried to backup by more than pos length"))
	}

	l.pos -= length
}

// peek returns but does not consume
// the next rune in the input.
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// peekBy returns but does not consume
// the next 'length' runes in the input.
func (l *lexer) peekBy(length int) (res string) {
	width := 0
	var buf []rune

	for i := 0; i < length; i++ {
		r := l.next()
		buf = append(buf, r)
		width += l.width
	}

	// backup by total width
	l.backupBy(width)

	res = string(buf[:])
	return res
}

// accept consumes the next rune
// if it's from the valid set.
func (l *lexer) accept(valid string) bool {
	if strings.ContainsRune(valid, l.next()) {
		return true
	}
	l.backup()
	return false
}

// acceptRun consumes a run of runes from the valid set.
func (l *lexer) acceptRun(valid string) (count int) {
	for strings.ContainsRune(valid, l.next()) {
		count++
	}
	l.backup()
	return count
}

// acceptWhile consumes runes while the predFn returns true
// it returns the number of runes accepted
func (l *lexer) acceptWhile(p predFn) (count int) {
	for p(l.next()) {
		count++
	}
	l.backup()
	return count
}

func (l *lexer) acceptUntil(p predFn) (count int) {
	ch := l.next()
	for ch != eof && !p(ch) {
		count++
		ch = l.next()
	}
	l.backup()
	return count
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nextItem.
func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- item{itemError, fmt.Sprintf(format, args...)}
	return nil
}

// emit passes an item back to the client.
func (l *lexer) emit(t itemType) {
	l.items <- item{t, l.input[l.start:l.pos]}
	l.start = l.pos
}

// run starts executing the state machine.
func (l *lexer) run() {
	for state := lexWhitespace; state != nil; {
		state = state(l)
	}

	close(l.items) // no more tokens
}

// isWhitespace checks if a rune is a whitespace
func isWhitespace(ch rune) bool { return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' }

// isAlphaNumeric checks if the rune is a letter, digit or underscore.
func isAlphaNumeric(ch rune) bool { return unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' }

// isDigit checks if the rune is a digit.
func isDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }

// isEndOfLine checks if the char represents the end of line.
func isEndOfLine(ch rune) bool { return ch == '\n' || ch == eof || ch == '\r' }

// isOperator checks if the rune is an operator.
// note that this only considers single char operators.
func isOperator(ch rune) bool {
	return ch == '+' || ch == '-' || ch == '*' || ch == '/' || ch == '=' || ch == '>' || ch == '<' || ch == '~' || ch == '|' || ch == '^' || ch == '&' || ch == '%'
}

//
// Public functions used by the consumer of the lexer, in our case the parser.
//

// nextItem returns the next item from the input.
// Called by the parser, not in the lexing goroutine.
func (l *lexer) nextItem() item {
	return <-l.items
}

// newLexer creates a new lexer and starts the state machine
func newLexer(name, input string) (*lexer, chan item) {
	l := &lexer{
		name:  name,
		input: input,
		items: make(chan item),
	}
	go l.run() // Concurrently run state machine.
	return l, l.items
}

//
// State functions - Internal
//

func lexWhitespace(l *lexer) stateFn {
	wcount := l.acceptWhile(isWhitespace)
	if wcount > 0 {
		l.emit(itemWhitespace)
	}

	next := l.peek()
	tNext := l.peekBy(2)

	switch {
	case next == eof:
		l.emit(itemEOF)
		return nil

	case tNext == "--":
		return lexSingleLineComment

	case tNext == "!=":
		l.next()
		l.next()
		l.emit(itemNotEqual)
		return lexWhitespace

	case tNext == ">=":
		l.next()
		l.next()
		l.emit(itemGreaterThanEqualTo)
		return lexWhitespace

	case tNext == "<=":
		l.next()
		l.next()
		l.emit(itemLessThanEqualTo)
		return lexWhitespace

	case next == '(':
		l.next()
		l.emit(itemLeftParen)
		return lexWhitespace

	case next == ')':
		l.next()
		l.emit(itemRightParen)
		return lexWhitespace

	case next == ',':
		l.next()
		l.emit(itemComma)
		return lexWhitespace

	case next == ';':
		l.next()
		l.emit(itemSemicolon)
		return lexWhitespace

	case next == '.': // todo: reconsider this?
		l.next()
		l.emit(itemPeriod)
		return lexWhitespace

	case isOperator(next):
		return lexOperator

	case next == '"':
		return lexString

	case isDigit(next):
		return lexNumber

	case isAlphaNumeric(next):
		return lexIdentifierOrKeyword
	}

	return l.errorf("unknown rune: %s", tNext)
}

func lexSingleLineComment(l *lexer) stateFn {
	l.acceptUntil(isEndOfLine)
	l.emit(itemSingleLineComment)
	return lexWhitespace
}

// lexOperator scans singe rune operators
func lexOperator(l *lexer) stateFn {
	op := l.next()

	switch op {
	case '=':
		l.emit(itemEqual)

	case '>':
		l.emit(itemGreaterThan)

	case '<':
		l.emit(itemLessThan)

	case '+':
		l.emit(itemPlus)

	case '-':
		l.emit(itemMinus)

	case '*':
		l.emit(itemAsterisk)

	case '/':
		l.emit(itemSlash)

	case '^':
		l.emit(itemCaret)

	case '%':
		l.emit(itemPercent)

	case '!':
		l.emit(itemExclamation)

	case '?':
		l.emit(itemQuestionMark)

	default:
		return l.errorf("unknown rune: %c", op)
	}

	return lexWhitespace
}

func lexString(l *lexer) stateFn {
	l.next() // opening quote

	// hard to check for unterminated string using below method
	// l.acceptUntil(func(r rune) bool { return r == '"' }) // till you find the closing quote

	for {
		r := l.next()

		if r == eof {
			return l.errorf("unclosed string. expected an end quote")
		} else if r == '"' {
			// found matching quote
			l.emit(itemString)
			return lexWhitespace
		}
	}
}

// lexNumber scans for a number. In case, an alphabet is found, it retracts and calls identifierOrKeyword.
func lexNumber(l *lexer) stateFn {
	count := 0
	count += l.acceptWhile(unicode.IsDigit)

	// floating point
	if l.accept(".") {
		count += 1 + l.acceptWhile(unicode.IsDigit)
	}

	if isAlphaNumeric(l.peek()) {
		l.backupBy(count)
		return lexIdentifierOrKeyword
	}

	l.emit(itemNumber)
	return lexWhitespace
}

func lexIdentifierOrKeyword(l *lexer) stateFn {
	for {
		r := l.next()

		if isAlphaNumeric(r) {
			l.acceptWhile(isAlphaNumeric)

			val := strings.ToUpper(l.input[l.start:l.pos])
			if _, ok := keywords[val]; ok {
				l.emit(itemKeyword)
			} else {
				l.emit(itemIdentifier)
			}
		}

		cnt := l.acceptWhile(isWhitespace)
		if cnt > 0 {
			l.emit(itemWhitespace)
		}

		if l.peek() != '.' {
			break
		}

		l.next()
		l.emit(itemPeriod)
	}

	return lexWhitespace
}
