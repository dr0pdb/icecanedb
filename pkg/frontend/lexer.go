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

// itemType is a SQL token type
type itemType int

const (
	itemError itemType = iota
	itemEOF
	itemWhitespace
	itemSingleLineComment // --

	// literals
	itemIdentifier // field name, table name
	itemTrue       // true
	itemFalse      // false
	itemInteger    // 1
	itemFloat      // 1.2
	itemString     // "hello"
	itemKeyword    // SELECT, INSERT, ..

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
	itemAndAnd             // "&&"
	itemOrOr               // "||"
)

const eof = -1

// boolean values
var bools = map[string]itemType{
	"TRUE":  itemTrue,
	"true":  itemTrue,
	"FALSE": itemFalse,
	"false": itemFalse,
}

// set of keywords
var keywords = map[string]keywordType{
	"CREATE":   keywordCreate,
	"DROP":     keywordDrop,
	"TRUNCATE": keywordTruncate,
	"SELECT":   keywordSelect,
	"INSERT":   keywordInsert,
	"DELETE":   keywordDelete,
	"UPDATE":   keywordUpdate,
	"ALTER":    keywordAlter,
	"WHERE":    keywordWhere,
	"ORDER":    keywordOrder,
	"BY":       keywordBy,
	"FROM":     keywordFrom,
	"VALUES":   keywordValues,
	"INTO":     keywordInto,
	"AS":       keywordAs,
	"SET":      keywordSet,

	// txns
	"BEGIN":    keywordBegin,
	"COMMIT":   keywordCommit,
	"ROLLBACK": keywordRollback,

	// objects
	"DATABASE": keywordDatabase,
	"TABLE":    keywordTable,

	// data types
	"BOOL":    keywordBool,
	"BOOLEAN": keywordBoolean,
	"INT":     keywordInt,
	"INTEGER": keywordInteger,
	"FLOAT":   keywordFloat,
	"DOUBLE":  keywordDouble,
	"DATE":    keywordDate,
	"CHAR":    keywordChar,
	"VARCHAR": keywordVarchar,
	"TEXT":    keywordText,
	"STRING":  keywordString,

	// column properties
	"UNIQUE":     keywordUnique,
	"NOT":        keywordNot,
	"NULL":       keywordNull,
	"PRIMARY":    keywordPrimary,
	"KEY":        keywordKey,
	"DEFAULT":    keywordDefault,
	"INDEX":      keywordIndex,
	"REFERENCES": keywordReferences,

	// others
	"EXPLAIN": keywordExplain,
	"READ":    keywordRead,
	"WRITE":   keywordWrite,
	"ONLY":    keywordOnly,
	"AND":     keywordAnd,
	"OR":      keywordOr,
}

// mapping from keyword type to it's string.
// for error output
var keywordStringRep = map[keywordType]string{
	keywordCreate:   "CREATE",
	keywordDrop:     "DROP",
	keywordTruncate: "TRUNCATE",
	keywordSelect:   "SELECT",
	keywordInsert:   "INSERT",
	keywordDelete:   "DELETE",
	keywordUpdate:   "UPDATE",
	keywordAlter:    "ALTER",
	keywordWhere:    "WHERE",
	keywordOrder:    "ORDER",
	keywordBy:       "BY",
	keywordFrom:     "FROM",
	keywordInto:     "INTO",
	keywordValues:   "VALUES",
	keywordAs:       "AS",
	keywordSet:      "SET",

	keywordBegin:    "BEGIN",
	keywordCommit:   "COMMIT",
	keywordRollback: "ROLLBACK",

	keywordDatabase: "DATABASE",
	keywordTable:    "TABLE",

	keywordBool:    "BOOL",
	keywordBoolean: "BOOLEAN",
	keywordInt:     "INT",
	keywordInteger: "INTEGER",
	keywordVarchar: "VARCHAR",
	keywordFloat:   "FLOAT",
	keywordDouble:  "DOUBLE",
	keywordChar:    "CHAR",
	keywordDate:    "DATE",
	keywordText:    "TEXT",
	keywordString:  "STRING",

	keywordUnique:     "UNIQUE",
	keywordNot:        "NOT",
	keywordNull:       "NULL",
	keywordPrimary:    "PRIMARY",
	keywordKey:        "KEY",
	keywordDefault:    "DEFAULT",
	keywordIndex:      "INDEX",
	keywordReferences: "REFERENCES",

	keywordExplain: "EXPLAIN",
	keywordRead:    "READ",
	keywordWrite:   "WRITE",
	keywordOnly:    "ONLY",
	keywordAnd:     "AND",
	keywordOr:      "OR",
}

type keywordType int

const (
	keywordCreate keywordType = iota
	keywordDrop
	keywordTruncate
	keywordSelect
	keywordInsert
	keywordDelete
	keywordUpdate
	keywordAlter
	keywordWhere
	keywordOrder
	keywordBy
	keywordFrom
	keywordInto
	keywordValues
	keywordAs
	keywordSet

	keywordBegin
	keywordCommit
	keywordRollback

	keywordDatabase
	keywordTable

	keywordBool
	keywordBoolean
	keywordInt
	keywordInteger
	keywordVarchar
	keywordFloat
	keywordDouble
	keywordChar
	keywordDate
	keywordText
	keywordString

	keywordUnique
	keywordNot
	keywordNull
	keywordPrimary
	keywordKey
	keywordDefault
	keywordIndex
	keywordReferences

	keywordExplain
	keywordRead
	keywordWrite
	keywordOnly
	keywordAnd
	keywordOr
)

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

	case tNext == "&&":
		l.next()
		l.next()
		l.emit(itemAndAnd)
		return lexWhitespace

	case tNext == "||":
		l.next()
		l.next()
		l.emit(itemOrOr)
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

	case next == '"' || next == '\'':
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
	quote := l.next() // opening quote

	// hard to check for unterminated string using below method
	// l.acceptUntil(func(r rune) bool { return r == '"' }) // till you find the closing quote

	for {
		r := l.next()

		if r == eof {
			return l.errorf("unclosed string. expected an end quote")
		} else if r == quote {
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
	dec := false

	// floating point
	if l.accept(".") {
		count += 1 + l.acceptWhile(unicode.IsDigit)
		dec = true
	}

	if isAlphaNumeric(l.peek()) {
		l.backupBy(count)
		return lexIdentifierOrKeyword
	}

	if dec {
		l.emit(itemFloat)
	} else {
		l.emit(itemInteger)
	}
	return lexWhitespace
}

func lexIdentifierOrKeyword(l *lexer) stateFn {
	for {
		r := l.next()

		if isAlphaNumeric(r) {
			l.acceptWhile(isAlphaNumeric)

			val := strings.ToUpper(l.input[l.start:l.pos])
			if it, ok := bools[val]; ok {
				l.emit(it)
			} else if _, ok := keywords[val]; ok {
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
