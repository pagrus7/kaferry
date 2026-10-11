package sql

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type token struct {
	kind string
	text string
	Position
}

func (l *lexer) position() Position { return Position{Line: l.line, Column: l.column} }

type lexer struct {
	input  string
	index  int
	line   int
	column int
}

func (l *lexer) advance() rune {
	r, n := utf8.DecodeRuneInString(l.input[l.index:])
	l.index += n
	if r == '\n' {
		l.line++
		l.column = 1
	} else {
		l.column++
	}
	return r
}

func (l *lexer) space() error {
	for l.index < len(l.input) {
		r, _ := utf8.DecodeRuneInString(l.input[l.index:])
		if unicode.IsSpace(r) {
			l.advance()
			continue
		}
		if strings.HasPrefix(l.input[l.index:], "--") {
			for l.index < len(l.input) && l.advance() != '\n' {
			}
			continue
		}
		if !strings.HasPrefix(l.input[l.index:], "/*") {
			return nil
		}
		position := l.position()
		l.advance()
		l.advance()
		for !strings.HasPrefix(l.input[l.index:], "*/") {
			if l.index == len(l.input) {
				return At(position, fmt.Errorf("unterminated comment; close it with */"))
			}
			if strings.HasPrefix(l.input[l.index:], "/*") {
				return At(l.position(), fmt.Errorf("nested comments are not supported"))
			}
			l.advance()
		}
		l.advance()
		l.advance()
	}
	return nil
}

func (l *lexer) next() (token, error) {
	if err := l.space(); err != nil {
		return token{}, err
	}
	t := token{Position: l.position(), kind: "end of statement"}
	if l.index == len(l.input) {
		return t, nil
	}
	r := l.advance()
	if r == '\'' {
		t.kind = "string"
		var b strings.Builder
		for l.index < len(l.input) {
			r = l.advance()
			if r != '\'' {
				b.WriteRune(r)
				continue
			}
			if l.index < len(l.input) && l.input[l.index] == '\'' {
				l.advance()
				b.WriteByte('\'')
				continue
			}
			t.text = b.String()
			return t, nil
		}
		return token{}, At(t.Position, fmt.Errorf("unterminated string literal; close it with a single quote"))
	}
	if identifierStart(r) {
		start := l.index - 1
		for l.index < len(l.input) {
			r, _ = utf8.DecodeRuneInString(l.input[l.index:])
			if !identifierStart(r) && (r < '0' || r > '9') {
				break
			}
			l.advance()
		}
		t.kind, t.text = "identifier", l.input[start:l.index]
		return t, nil
	}
	if strings.ContainsRune("(),;*=+-<>", r) {
		t.kind, t.text = "symbol", string(r)
		if (r == '<' || r == '>') && l.index < len(l.input) && l.input[l.index] == '=' {
			l.advance()
			t.text += "="
		}
		return t, nil
	}
	return token{}, At(t.Position, fmt.Errorf("unexpected character; use single quotes for literals and supported SQL operators"))
}

func (t token) description() string {
	switch t.kind {
	case "string":
		return "string literal"
	case "symbol":
		return fmt.Sprintf("%q", t.text)
	default:
		return t.kind
	}
}

func identifierStart(r rune) bool { return r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' }
