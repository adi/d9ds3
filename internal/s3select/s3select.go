// Package s3select implements a focused subset of the Amazon S3 Select query
// engine. It parses a SELECT statement, evaluates it against a CSV or JSON
// input stream, and writes the results back as an AWS event stream that the
// aws-sdk-go-v2 SelectObjectContent event-stream reader can decode.
package s3select

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
)

// CSVInput describes how to parse CSV input.
type CSVInput struct {
	FileHeaderInfo  string // USE | IGNORE | NONE (default NONE)
	FieldDelimiter  string // default ","
	RecordDelimiter string // default "\n"
	QuoteChar       string // default '"'
	CommentChar     string
}

// JSONInput describes how to parse JSON input.
type JSONInput struct {
	Type string // DOCUMENT | LINES (default DOCUMENT)
}

// Input describes the object being queried.
type Input struct {
	Format          string // CSV | JSON
	CSV             CSVInput
	JSON            JSONInput
	CompressionType string // NONE | GZIP
}

// CSVOutput describes how to format CSV output.
type CSVOutput struct {
	FieldDelimiter  string
	RecordDelimiter string
	QuoteChar       string
}

// JSONOutput describes how to format JSON output.
type JSONOutput struct {
	RecordDelimiter string
}

// Output describes the desired result serialization.
type Output struct {
	Format string // CSV | JSON
	CSV    CSVOutput
	JSON   JSONOutput
}

// countReader counts the number of bytes read through it.
type countReader struct {
	r io.Reader
	n int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// Execute runs the SQL against src and writes an AWS S3-Select event stream
// (Records + Stats + End messages) to w using the aws eventstream encoding.
func Execute(sql string, in Input, out Output, src io.Reader, w io.Writer) error {
	q, err := parse(sql)
	if err != nil {
		return err
	}

	cr := &countReader{r: src}
	var reader io.Reader = cr
	if strings.EqualFold(in.CompressionType, "GZIP") {
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("s3select: gzip error: %w", err)
		}
		defer gz.Close()
		reader = gz
	}

	ctx := &evalCtx{alias: q.alias}
	var body bytes.Buffer
	matched := 0
	count := 0

	handle := func(rec record) error {
		if q.where != nil {
			ok, err := q.where.test(rec, ctx)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
		if q.limit >= 0 && matched >= q.limit {
			return errLimitReached
		}
		matched++
		if q.proj.countStar {
			count++
			return nil
		}
		return writeRow(&body, out, q, ctx, rec)
	}

	switch strings.ToUpper(in.Format) {
	case "", "CSV":
		err = readCSV(in.CSV, reader, handle)
	case "JSON":
		err = readJSON(in.JSON, reader, handle)
	default:
		return fmt.Errorf("s3select: unsupported input format %q", in.Format)
	}
	if err != nil && err != errLimitReached {
		return err
	}

	if q.proj.countStar {
		writeCount(&body, out, count)
	}

	return writeEventStream(w, body.Bytes(), cr.n)
}

// errLimitReached is a sentinel used to stop iteration once LIMIT is hit.
var errLimitReached = fmt.Errorf("s3select: limit reached")

// ---- output formatting ------------------------------------------------------

func outFormat(out Output) string {
	f := strings.ToUpper(out.Format)
	if f == "" {
		return "CSV"
	}
	return f
}

func writeRow(body *bytes.Buffer, out Output, q *query, ctx *evalCtx, rec record) error {
	if outFormat(out) == "JSON" {
		return writeJSONRow(body, out, q, ctx, rec)
	}
	return writeCSVRow(body, out, q, ctx, rec)
}

func writeCSVRow(body *bytes.Buffer, out Output, q *query, ctx *evalCtx, rec record) error {
	delim := out.CSV.FieldDelimiter
	if delim == "" {
		delim = ","
	}
	rd := out.CSV.RecordDelimiter
	if rd == "" {
		rd = "\n"
	}

	var vals []string
	if q.proj.star {
		if fields, ok := rec.csvFields(); ok {
			vals = fields
		} else if jv, ok := rec.jsonValue(); ok {
			b, _ := json.Marshal(jv)
			vals = []string{string(b)}
		}
	} else {
		for _, it := range q.proj.items {
			v, ok := it.expr.eval(rec, ctx)
			if !ok {
				vals = append(vals, "")
				continue
			}
			vals = append(vals, asString(v))
		}
	}
	body.WriteString(strings.Join(vals, delim))
	body.WriteString(rd)
	return nil
}

func writeJSONRow(body *bytes.Buffer, out Output, q *query, ctx *evalCtx, rec record) error {
	rd := out.JSON.RecordDelimiter
	if rd == "" {
		rd = "\n"
	}

	if q.proj.star {
		if jv, ok := rec.jsonValue(); ok {
			b, err := json.Marshal(jv)
			if err != nil {
				return fmt.Errorf("s3select: marshal error: %w", err)
			}
			body.Write(b)
			body.WriteString(rd)
			return nil
		}
		// CSV input, JSON output: emit positional _N keys.
		if fields, ok := rec.csvFields(); ok {
			body.WriteByte('{')
			for i, f := range fields {
				if i > 0 {
					body.WriteByte(',')
				}
				writeJSONKV(body, fmt.Sprintf("_%d", i+1), f)
			}
			body.WriteByte('}')
			body.WriteString(rd)
		}
		return nil
	}

	body.WriteByte('{')
	written := 0
	for _, it := range q.proj.items {
		v, ok := it.expr.eval(rec, ctx)
		if !ok {
			continue // omit missing fields, matching S3 Select behaviour
		}
		if written > 0 {
			body.WriteByte(',')
		}
		if err := writeJSONKV(body, it.name, v); err != nil {
			return err
		}
		written++
	}
	body.WriteByte('}')
	body.WriteString(rd)
	return nil
}

func writeJSONKV(body *bytes.Buffer, key string, val any) error {
	kb, err := json.Marshal(key)
	if err != nil {
		return err
	}
	body.Write(kb)
	body.WriteByte(':')
	vb, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("s3select: marshal error: %w", err)
	}
	body.Write(vb)
	return nil
}

func writeCount(body *bytes.Buffer, out Output, count int) {
	if outFormat(out) == "JSON" {
		rd := out.JSON.RecordDelimiter
		if rd == "" {
			rd = "\n"
		}
		body.WriteString(`{"_1":`)
		body.WriteString(strconv.Itoa(count))
		body.WriteString("}")
		body.WriteString(rd)
		return
	}
	rd := out.CSV.RecordDelimiter
	if rd == "" {
		rd = "\n"
	}
	body.WriteString(strconv.Itoa(count))
	body.WriteString(rd)
}

// ---- event stream -----------------------------------------------------------

func writeEventStream(w io.Writer, body []byte, scanned int64) error {
	enc := eventstream.NewEncoder()

	if len(body) > 0 {
		msg := eventstream.Message{
			Headers: eventstream.Headers{
				{Name: ":message-type", Value: eventstream.StringValue("event")},
				{Name: ":event-type", Value: eventstream.StringValue("Records")},
				{Name: ":content-type", Value: eventstream.StringValue("application/octet-stream")},
			},
			Payload: body,
		}
		if err := enc.Encode(w, msg); err != nil {
			return fmt.Errorf("s3select: encode Records: %w", err)
		}
	}

	stats := fmt.Sprintf(
		"<Stats><Details><BytesScanned>%d</BytesScanned><BytesProcessed>%d</BytesProcessed><BytesReturned>%d</BytesReturned></Details></Stats>",
		scanned, scanned, len(body),
	)
	statsMsg := eventstream.Message{
		Headers: eventstream.Headers{
			{Name: ":message-type", Value: eventstream.StringValue("event")},
			{Name: ":event-type", Value: eventstream.StringValue("Stats")},
			{Name: ":content-type", Value: eventstream.StringValue("text/xml")},
		},
		Payload: []byte(stats),
	}
	if err := enc.Encode(w, statsMsg); err != nil {
		return fmt.Errorf("s3select: encode Stats: %w", err)
	}

	endMsg := eventstream.Message{
		Headers: eventstream.Headers{
			{Name: ":message-type", Value: eventstream.StringValue("event")},
			{Name: ":event-type", Value: eventstream.StringValue("End")},
		},
	}
	if err := enc.Encode(w, endMsg); err != nil {
		return fmt.Errorf("s3select: encode End: %w", err)
	}
	return nil
}
