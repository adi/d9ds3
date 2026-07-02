package s3select

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// evalCtx carries information needed to resolve column references, such as the
// table alias declared in the FROM clause.
type evalCtx struct {
	alias string
}

// query is the parsed representation of a supported S3 Select statement.
type query struct {
	proj  projection
	alias string
	where predExpr // nil when there is no WHERE clause
	limit int      // -1 when there is no LIMIT clause
}

type projection struct {
	star      bool
	countStar bool
	items     []projItem
}

type projItem struct {
	expr valExpr
	name string
}

// ---- value expressions -----------------------------------------------------

type valExpr interface {
	eval(rec record, ctx *evalCtx) (any, bool)
}

type colRef struct{ parts []string }

func (c colRef) eval(rec record, ctx *evalCtx) (any, bool) {
	parts := c.parts
	if len(parts) > 1 {
		if (ctx.alias != "" && strings.EqualFold(parts[0], ctx.alias)) || strings.EqualFold(parts[0], "S3Object") {
			parts = parts[1:]
		}
	}
	return rec.lookup(parts)
}

func (c colRef) name() string {
	if len(c.parts) == 0 {
		return ""
	}
	return c.parts[len(c.parts)-1]
}

type strLit struct{ s string }

func (l strLit) eval(record, *evalCtx) (any, bool) { return l.s, true }

type numLit struct{ f float64 }

func (l numLit) eval(record, *evalCtx) (any, bool) { return l.f, true }

type boolLit struct{ b bool }

func (l boolLit) eval(record, *evalCtx) (any, bool) { return l.b, true }

type castExpr struct {
	inner valExpr
	typ   string // INT | FLOAT | STRING
}

func (c castExpr) eval(rec record, ctx *evalCtx) (any, bool) {
	v, ok := c.inner.eval(rec, ctx)
	if !ok {
		return nil, false
	}
	switch c.typ {
	case "INT":
		if f, ok := asFloat(v); ok {
			return float64(int64(f)), true
		}
		return nil, false
	case "FLOAT":
		if f, ok := asFloat(v); ok {
			return f, true
		}
		return nil, false
	case "STRING":
		return asString(v), true
	}
	return nil, false
}

// ---- predicate expressions --------------------------------------------------

type predExpr interface {
	test(rec record, ctx *evalCtx) (bool, error)
}

type andPred struct{ a, b predExpr }

func (p andPred) test(rec record, ctx *evalCtx) (bool, error) {
	l, err := p.a.test(rec, ctx)
	if err != nil {
		return false, err
	}
	if !l {
		return false, nil
	}
	return p.b.test(rec, ctx)
}

type orPred struct{ a, b predExpr }

func (p orPred) test(rec record, ctx *evalCtx) (bool, error) {
	l, err := p.a.test(rec, ctx)
	if err != nil {
		return false, err
	}
	if l {
		return true, nil
	}
	return p.b.test(rec, ctx)
}

type notPred struct{ inner predExpr }

func (p notPred) test(rec record, ctx *evalCtx) (bool, error) {
	v, err := p.inner.test(rec, ctx)
	if err != nil {
		return false, err
	}
	return !v, nil
}

type cmpPred struct {
	left, right valExpr
	op          string
}

func (p cmpPred) test(rec record, ctx *evalCtx) (bool, error) {
	l, lok := p.left.eval(rec, ctx)
	r, rok := p.right.eval(rec, ctx)
	if !lok || !rok {
		// A missing operand never satisfies a comparison, but for <> it means
		// the values differ.
		if p.op == "<>" || p.op == "!=" {
			return lok != rok, nil
		}
		return false, nil
	}
	return compareOp(l, r, p.op), nil
}

type likePred struct {
	left valExpr
	re   *regexp.Regexp
	neg  bool
}

func (p likePred) test(rec record, ctx *evalCtx) (bool, error) {
	v, ok := p.left.eval(rec, ctx)
	if !ok {
		return false, nil
	}
	m := p.re.MatchString(asString(v))
	if p.neg {
		return !m, nil
	}
	return m, nil
}

// truthPred lets a bare value expression act as a predicate (e.g. WHERE TRUE).
type truthPred struct{ expr valExpr }

func (p truthPred) test(rec record, ctx *evalCtx) (bool, error) {
	v, ok := p.expr.eval(rec, ctx)
	if !ok {
		return false, nil
	}
	return truthy(v), nil
}

// ---- comparison helpers -----------------------------------------------------

func compareOp(l, r any, op string) bool {
	switch op {
	case "=":
		return valuesEqual(l, r)
	case "<>", "!=":
		return !valuesEqual(l, r)
	case "<", "<=", ">", ">=":
		c, ok := order(l, r)
		if !ok {
			return false
		}
		switch op {
		case "<":
			return c < 0
		case "<=":
			return c <= 0
		case ">":
			return c > 0
		case ">=":
			return c >= 0
		}
	}
	return false
}

func valuesEqual(l, r any) bool {
	if lb, ok := l.(bool); ok {
		if rb, ok := r.(bool); ok {
			return lb == rb
		}
	}
	if lf, lok := asFloat(l); lok {
		if rf, rok := asFloat(r); rok {
			return lf == rf
		}
	}
	return asString(l) == asString(r)
}

func order(l, r any) (int, bool) {
	if lf, lok := asFloat(l); lok {
		if rf, rok := asFloat(r); rok {
			switch {
			case lf < rf:
				return -1, true
			case lf > rf:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	return strings.Compare(asString(l), asString(r)), true
}

func asFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

func asString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	default:
		return fmt.Sprintf("%v", t)
	}
}

func truthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	case float64:
		return t != 0
	}
	return true
}

// likeToRegexp converts a SQL LIKE pattern (% and _) into an anchored regexp.
func likeToRegexp(pattern string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	var lit strings.Builder
	flush := func() {
		if lit.Len() > 0 {
			b.WriteString(regexp.QuoteMeta(lit.String()))
			lit.Reset()
		}
	}
	for _, r := range pattern {
		switch r {
		case '%':
			flush()
			b.WriteString(".*")
		case '_':
			flush()
			b.WriteString(".")
		default:
			lit.WriteRune(r)
		}
	}
	flush()
	b.WriteString("$")
	return regexp.Compile(b.String())
}
