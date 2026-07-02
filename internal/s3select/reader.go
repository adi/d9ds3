package s3select

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// record is a single input row that can resolve column references and be
// emitted whole for a `SELECT *` projection.
type record interface {
	// lookup resolves alias-stripped reference parts to a value.
	lookup(parts []string) (any, bool)
	// csvFields returns the raw CSV fields (ok only for CSV input).
	csvFields() ([]string, bool)
	// jsonValue returns the decoded JSON value (ok only for JSON input).
	jsonValue() (any, bool)
}

// ---- CSV --------------------------------------------------------------------

type csvRecord struct {
	fields []string
	header map[string]int // nil when no usable header
}

func (r csvRecord) lookup(parts []string) (any, bool) {
	if len(parts) != 1 {
		return nil, false
	}
	p := parts[0]
	if len(p) >= 2 && p[0] == '_' {
		if idx, err := strconv.Atoi(p[1:]); err == nil {
			i := idx - 1
			if i >= 0 && i < len(r.fields) {
				return r.fields[i], true
			}
			return nil, false
		}
	}
	if r.header != nil {
		if i, ok := r.header[p]; ok && i < len(r.fields) {
			return r.fields[i], true
		}
	}
	return nil, false
}

func (r csvRecord) csvFields() ([]string, bool) { return r.fields, true }
func (r csvRecord) jsonValue() (any, bool)      { return nil, false }

func firstRune(s, def string) rune {
	if s == "" {
		return []rune(def)[0]
	}
	return []rune(s)[0]
}

func readCSV(in CSVInput, r io.Reader, fn func(record) error) error {
	cr := csv.NewReader(r)
	cr.Comma = firstRune(in.FieldDelimiter, ",")
	cr.LazyQuotes = true
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = false
	if in.CommentChar != "" {
		cr.Comment = firstRune(in.CommentChar, "#")
	}

	var header map[string]int
	useHeader := strings.EqualFold(in.FileHeaderInfo, "USE")
	ignoreHeader := strings.EqualFold(in.FileHeaderInfo, "IGNORE")
	first := true

	for {
		fields, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("s3select: CSV parse error: %w", err)
		}
		if first {
			first = false
			if useHeader {
				header = make(map[string]int, len(fields))
				for i, name := range fields {
					header[name] = i
				}
				continue
			}
			if ignoreHeader {
				continue
			}
		}
		if err := fn(csvRecord{fields: fields, header: header}); err != nil {
			return err
		}
	}
	return nil
}

// ---- JSON -------------------------------------------------------------------

type jsonRecord struct{ v any }

func (r jsonRecord) lookup(parts []string) (any, bool) {
	cur := r.v
	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func (r jsonRecord) csvFields() ([]string, bool) { return nil, false }
func (r jsonRecord) jsonValue() (any, bool)      { return r.v, true }

func readJSON(in JSONInput, r io.Reader, fn func(record) error) error {
	lines := strings.EqualFold(in.Type, "LINES")
	if lines {
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var v any
			if err := json.Unmarshal([]byte(line), &v); err != nil {
				return fmt.Errorf("s3select: JSON parse error: %w", err)
			}
			if err := fn(jsonRecord{v: v}); err != nil {
				return err
			}
		}
		if err := sc.Err(); err != nil {
			return fmt.Errorf("s3select: JSON read error: %w", err)
		}
		return nil
	}

	// DOCUMENT: a top-level array streams its elements; otherwise decode a
	// sequence of one or more concatenated JSON values.
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("s3select: JSON read error: %w", err)
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil
	}
	if trimmed[0] == '[' {
		var arr []any
		if err := json.Unmarshal(trimmed, &arr); err != nil {
			return fmt.Errorf("s3select: JSON parse error: %w", err)
		}
		for _, v := range arr {
			if err := fn(jsonRecord{v: v}); err != nil {
				return err
			}
		}
		return nil
	}

	dec := json.NewDecoder(bytes.NewReader(trimmed))
	for {
		var v any
		if err := dec.Decode(&v); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("s3select: JSON parse error: %w", err)
		}
		if err := fn(jsonRecord{v: v}); err != nil {
			return err
		}
	}
	return nil
}
