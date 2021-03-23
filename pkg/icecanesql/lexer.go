package icecanesql

import (
	"fmt"
	"strings"
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

	// literals
	itemIdent // field name, table name

	// misc
	itemAsterisk
	itemComma

	// keywords
	itemSelect
	itemFrom
	itemWhere
)

const eof = -1

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

// peek returns but does not consume
// the next rune in the input.
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
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
func (l *lexer) acceptRun(valid string) {
	for strings.ContainsRune(valid, l.next()) {
	}
	l.backup()
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
	for state := lexStart; state != nil; {
		state = state(l)
	}

	close(l.items) // no more tokens
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
// State functions
//

func lexStart(l *lexer) stateFn {
	return nil
}
