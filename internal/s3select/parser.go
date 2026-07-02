package s3select

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// ---- lexer ------------------------------------------------------------------

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tString
	tNumber
	tOp // comparison operator: = <> != < > <= >=
	tLParen
	tRParen
	tComma
	tDot
	tStar
	tLBracket
	tRBracket
)

type token struct {
	kind tokKind
	text string
}

func lex(s string) ([]token, error) {
	var toks []token
	rs := []rune(s)
	i := 0
	n := len(rs)
	for i < n {
		c := rs[i]
		switch {
		case unicode.IsSpace(c):
			i++
		case c == '\'':
			// string literal with '' escaping
			i++
			var b strings.Builder
			closed := false
			for i < n {
				if rs[i] == '\'' {
					if i+1 < n && rs[i+1] == '\'' {
						b.WriteRune('\'')
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				b.WriteRune(rs[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("s3select: unterminated string literal")
			}
			toks = append(toks, token{tString, b.String()})
		case c == '(':
			toks = append(toks, token{tLParen, "("})
			i++
		case c == ')':
			toks = append(toks, token{tRParen, ")"})
			i++
		case c == ',':
			toks = append(toks, token{tComma, ","})
			i++
		case c == '.':
			toks = append(toks, token{tDot, "."})
			i++
		case c == '*':
			toks = append(toks, token{tStar, "*"})
			i++
		case c == '[':
			toks = append(toks, token{tLBracket, "["})
			i++
		case c == ']':
			toks = append(toks, token{tRBracket, "]"})
			i++
		case c == '=':
			toks = append(toks, token{tOp, "="})
			i++
		case c == '<':
			if i+1 < n && rs[i+1] == '>' {
				toks = append(toks, token{tOp, "<>"})
				i += 2
			} else if i+1 < n && rs[i+1] == '=' {
				toks = append(toks, token{tOp, "<="})
				i += 2
			} else {
				toks = append(toks, token{tOp, "<"})
				i++
			}
		case c == '>':
			if i+1 < n && rs[i+1] == '=' {
				toks = append(toks, token{tOp, ">="})
				i += 2
			} else {
				toks = append(toks, token{tOp, ">"})
				i++
			}
		case c == '!':
			if i+1 < n && rs[i+1] == '=' {
				toks = append(toks, token{tOp, "!="})
				i += 2
			} else {
				return nil, fmt.Errorf("s3select: unexpected character %q", string(c))
			}
		case c == '-' || unicode.IsDigit(c):
			// number: optional leading '-', digits, optional fraction
			start := i
			if c == '-' {
				i++
				if i >= n || !unicode.IsDigit(rs[i]) {
					return nil, fmt.Errorf("s3select: unexpected character %q", string(c))
				}
			}
			for i < n && unicode.IsDigit(rs[i]) {
				i++
			}
			if i < n && rs[i] == '.' {
				i++
				for i < n && unicode.IsDigit(rs[i]) {
					i++
				}
			}
			toks = append(toks, token{tNumber, string(rs[start:i])})
		case c == '_' || unicode.IsLetter(c):
			start := i
			for i < n && (rs[i] == '_' || unicode.IsLetter(rs[i]) || unicode.IsDigit(rs[i])) {
				i++
			}
			toks = append(toks, token{tIdent, string(rs[start:i])})
		default:
			return nil, fmt.Errorf("s3select: unexpected character %q", string(c))
		}
	}
	toks = append(toks, token{tEOF, ""})
	return toks, nil
}

// ---- parser -----------------------------------------------------------------

type parser struct {
	toks []token
	pos  int
}

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) next() token {
	t := p.toks[p.pos]
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}
func (p *parser) isKeyword(kw string) bool {
	t := p.peek()
	return t.kind == tIdent && strings.EqualFold(t.text, kw)
}

func parse(sql string) (*query, error) {
	toks, err := lex(sql)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	q := &query{limit: -1}

	if !p.isKeyword("SELECT") {
		return nil, fmt.Errorf("s3select: expected SELECT")
	}
	p.next()

	if err := p.parseProjection(q); err != nil {
		return nil, err
	}

	if !p.isKeyword("FROM") {
		return nil, fmt.Errorf("s3select: expected FROM")
	}
	p.next()
	if err := p.parseFrom(q); err != nil {
		return nil, err
	}

	if p.isKeyword("WHERE") {
		p.next()
		w, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		q.where = w
	}

	if p.isKeyword("LIMIT") {
		p.next()
		t := p.next()
		if t.kind != tNumber {
			return nil, fmt.Errorf("s3select: expected number after LIMIT")
		}
		lim, err := strconv.Atoi(t.text)
		if err != nil || lim < 0 {
			return nil, fmt.Errorf("s3select: invalid LIMIT value %q", t.text)
		}
		q.limit = lim
	}

	if p.peek().kind != tEOF {
		return nil, fmt.Errorf("s3select: unexpected token %q", p.peek().text)
	}
	return q, nil
}

func (p *parser) parseProjection(q *query) error {
	for {
		// COUNT(*)
		if p.peek().kind == tIdent && strings.EqualFold(p.peek().text, "COUNT") &&
			p.toks[p.pos+1].kind == tLParen {
			p.next() // COUNT
			p.next() // (
			if p.peek().kind != tStar {
				return fmt.Errorf("s3select: only COUNT(*) is supported")
			}
			p.next() // *
			if p.peek().kind != tRParen {
				return fmt.Errorf("s3select: expected ) after COUNT(*")
			}
			p.next() // )
			q.proj.countStar = true
			q.proj.items = append(q.proj.items, projItem{name: "_1"})
		} else if p.peek().kind == tStar {
			p.next()
			q.proj.star = true
		} else {
			e, err := p.parseValue()
			if err != nil {
				return err
			}
			name := p.optAlias()
			if name == "" {
				if cr, ok := e.(colRef); ok {
					name = cr.name()
				}
			}
			if name == "" {
				name = fmt.Sprintf("_%d", len(q.proj.items)+1)
			}
			q.proj.items = append(q.proj.items, projItem{expr: e, name: name})
		}

		if p.peek().kind == tComma {
			p.next()
			continue
		}
		break
	}
	if q.proj.star && len(q.proj.items) > 0 {
		return fmt.Errorf("s3select: cannot mix * with other projections")
	}
	return nil
}

// optAlias consumes an optional column/table alias and returns it ("" if none).
func (p *parser) optAlias() string {
	if p.isKeyword("AS") {
		p.next()
		t := p.next()
		if t.kind == tIdent {
			return t.text
		}
		return ""
	}
	if p.peek().kind == tIdent {
		up := strings.ToUpper(p.peek().text)
		switch up {
		case "FROM", "WHERE", "LIMIT", "AND", "OR", "AS", "LIKE", "NOT":
			return ""
		}
		return p.next().text
	}
	return ""
}

func (p *parser) parseFrom(q *query) error {
	t := p.next()
	if t.kind != tIdent || !strings.EqualFold(t.text, "S3Object") {
		return fmt.Errorf("s3select: FROM target must be S3Object")
	}
	// optional [*] or [n]
	if p.peek().kind == tLBracket {
		p.next()
		if p.peek().kind == tStar || p.peek().kind == tNumber {
			p.next()
		}
		if p.peek().kind != tRBracket {
			return fmt.Errorf("s3select: expected ] in FROM clause")
		}
		p.next()
	}
	// optional .path (ignored for streaming purposes)
	for p.peek().kind == tDot {
		p.next()
		if p.peek().kind != tIdent {
			return fmt.Errorf("s3select: expected identifier after . in FROM clause")
		}
		p.next()
	}
	q.alias = p.optAlias()
	return nil
}

// ---- WHERE predicate grammar ------------------------------------------------

func (p *parser) parseOr() (predExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("OR") {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = orPred{left, right}
	}
	return left, nil
}

func (p *parser) parseAnd() (predExpr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("AND") {
		p.next()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = andPred{left, right}
	}
	return left, nil
}

func (p *parser) parseNot() (predExpr, error) {
	if p.isKeyword("NOT") {
		p.next()
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return notPred{inner}, nil
	}
	return p.parsePrimaryPred()
}

func (p *parser) parsePrimaryPred() (predExpr, error) {
	if p.peek().kind == tLParen {
		p.next()
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tRParen {
			return nil, fmt.Errorf("s3select: expected ) in WHERE clause")
		}
		p.next()
		return inner, nil
	}

	left, err := p.parseValue()
	if err != nil {
		return nil, err
	}

	// [NOT] LIKE
	neg := false
	if p.isKeyword("NOT") {
		// lookahead for LIKE
		if p.toks[p.pos+1].kind == tIdent && strings.EqualFold(p.toks[p.pos+1].text, "LIKE") {
			p.next() // NOT
			neg = true
		}
	}
	if p.isKeyword("LIKE") {
		p.next()
		t := p.next()
		if t.kind != tString {
			return nil, fmt.Errorf("s3select: LIKE requires a string pattern")
		}
		re, err := likeToRegexp(t.text)
		if err != nil {
			return nil, fmt.Errorf("s3select: invalid LIKE pattern: %w", err)
		}
		return likePred{left: left, re: re, neg: neg}, nil
	}
	if neg {
		return nil, fmt.Errorf("s3select: expected LIKE after NOT")
	}

	// comparison operator
	if p.peek().kind == tOp {
		op := p.next().text
		right, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		return cmpPred{left: left, right: right, op: op}, nil
	}

	// bare value -> truthiness test
	return truthPred{left}, nil
}

// ---- value expression grammar -----------------------------------------------

func (p *parser) parseValue() (valExpr, error) {
	t := p.peek()
	switch t.kind {
	case tString:
		p.next()
		return strLit{t.text}, nil
	case tNumber:
		p.next()
		f, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return nil, fmt.Errorf("s3select: invalid number %q", t.text)
		}
		return numLit{f}, nil
	case tIdent:
		up := strings.ToUpper(t.text)
		switch up {
		case "TRUE":
			p.next()
			return boolLit{true}, nil
		case "FALSE":
			p.next()
			return boolLit{false}, nil
		case "CAST":
			return p.parseCast()
		}
		return p.parseColRef()
	default:
		return nil, fmt.Errorf("s3select: unexpected token %q in expression", t.text)
	}
}

func (p *parser) parseCast() (valExpr, error) {
	p.next() // CAST
	if p.peek().kind != tLParen {
		return nil, fmt.Errorf("s3select: expected ( after CAST")
	}
	p.next()
	inner, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	if !p.isKeyword("AS") {
		return nil, fmt.Errorf("s3select: expected AS in CAST")
	}
	p.next()
	tt := p.next()
	if tt.kind != tIdent {
		return nil, fmt.Errorf("s3select: expected type in CAST")
	}
	typ := strings.ToUpper(tt.text)
	switch typ {
	case "INT", "INTEGER":
		typ = "INT"
	case "FLOAT", "DECIMAL", "DOUBLE":
		typ = "FLOAT"
	case "STRING", "VARCHAR":
		typ = "STRING"
	default:
		return nil, fmt.Errorf("s3select: unsupported CAST type %q", tt.text)
	}
	if p.peek().kind != tRParen {
		return nil, fmt.Errorf("s3select: expected ) in CAST")
	}
	p.next()
	return castExpr{inner: inner, typ: typ}, nil
}

func (p *parser) parseColRef() (valExpr, error) {
	t := p.next()
	parts := []string{t.text}
	for p.peek().kind == tDot {
		p.next()
		nt := p.peek()
		if nt.kind != tIdent && nt.kind != tNumber {
			return nil, fmt.Errorf("s3select: expected identifier after .")
		}
		p.next()
		parts = append(parts, nt.text)
	}
	return colRef{parts: parts}, nil
}
