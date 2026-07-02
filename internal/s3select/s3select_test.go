package s3select

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
)

// decoded holds the messages pulled back out of an event stream, grouped by
// their :event-type header.
type decoded struct {
	records []string // Records payloads, in order
	types   []string // event-type of every message, in order
	stats   string
}

func decodeStream(t *testing.T, r io.Reader) decoded {
	t.Helper()
	dec := eventstream.NewDecoder()
	var d decoded
	var payloadBuf []byte
	for {
		msg, err := dec.Decode(r, payloadBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode event stream: %v", err)
		}
		et := msg.Headers.Get(":event-type")
		if et == nil {
			t.Fatalf("message missing :event-type header")
		}
		typ := et.String()
		d.types = append(d.types, typ)
		switch typ {
		case "Records":
			d.records = append(d.records, string(msg.Payload))
		case "Stats":
			d.stats = string(msg.Payload)
		}
	}
	return d
}

func run(t *testing.T, sql string, in Input, out Output, src string) decoded {
	t.Helper()
	var buf bytes.Buffer
	if err := Execute(sql, in, out, strings.NewReader(src), &buf); err != nil {
		t.Fatalf("Execute(%q) error: %v", sql, err)
	}
	return decodeStream(t, &buf)
}

func (d decoded) allRecords() string { return strings.Join(d.records, "") }

// assertTail verifies the stream ends with a Stats then an End message.
func (d decoded) assertTail(t *testing.T) {
	t.Helper()
	if len(d.types) < 2 {
		t.Fatalf("expected at least Stats and End messages, got %v", d.types)
	}
	if d.types[len(d.types)-2] != "Stats" || d.types[len(d.types)-1] != "End" {
		t.Fatalf("stream must end with Stats then End, got %v", d.types)
	}
	if !strings.Contains(d.stats, "<BytesScanned>") || !strings.Contains(d.stats, "<BytesReturned>") {
		t.Fatalf("stats payload malformed: %q", d.stats)
	}
}

const csvWithHeader = "name,age,city\nalice,35,paris\nbob,20,rome\ncarol,42,berlin\n"

func TestCSVHeaderUse(t *testing.T) {
	in := Input{Format: "CSV", CSV: CSVInput{FileHeaderInfo: "USE"}}
	out := Output{Format: "CSV"}
	d := run(t, "SELECT s.name FROM S3Object s WHERE s.age > 30", in, out, csvWithHeader)
	d.assertTail(t)

	got := d.allRecords()
	if !strings.Contains(got, "alice") || !strings.Contains(got, "carol") {
		t.Fatalf("expected alice and carol, got %q", got)
	}
	if strings.Contains(got, "bob") {
		t.Fatalf("did not expect bob (age 20), got %q", got)
	}
}

func TestCSVPositional(t *testing.T) {
	src := "a,x,1\nb,y,2\nc,x,3\n"
	in := Input{Format: "CSV", CSV: CSVInput{FileHeaderInfo: "NONE"}}
	out := Output{Format: "CSV"}
	d := run(t, "SELECT _1, _3 FROM S3Object WHERE _2 = 'x'", in, out, src)
	d.assertTail(t)

	got := d.allRecords()
	wantLines := []string{"a,1", "c,3"}
	for _, w := range wantLines {
		if !strings.Contains(got, w) {
			t.Fatalf("expected %q in output %q", w, got)
		}
	}
	if strings.Contains(got, "b,2") {
		t.Fatalf("did not expect row b (col2=y), got %q", got)
	}
}

func TestJSONLines(t *testing.T) {
	src := `{"id":1,"active":true}
{"id":2,"active":false}
{"id":3,"active":true}`
	in := Input{Format: "JSON", JSON: JSONInput{Type: "LINES"}}
	out := Output{Format: "JSON"}
	d := run(t, "SELECT s.id FROM S3Object[*] s WHERE s.active = TRUE", in, out, src)
	d.assertTail(t)

	got := d.allRecords()
	if !strings.Contains(got, `{"id":1}`) || !strings.Contains(got, `{"id":3}`) {
		t.Fatalf("expected ids 1 and 3, got %q", got)
	}
	if strings.Contains(got, `{"id":2}`) {
		t.Fatalf("did not expect id 2 (inactive), got %q", got)
	}
}

func TestJSONDocumentArray(t *testing.T) {
	src := `[{"a":{"b":"deep"}},{"a":{"b":"other"}}]`
	in := Input{Format: "JSON", JSON: JSONInput{Type: "DOCUMENT"}}
	out := Output{Format: "JSON"}
	d := run(t, "SELECT s.a.b FROM S3Object[*] s WHERE s.a.b = 'deep'", in, out, src)
	d.assertTail(t)
	got := d.allRecords()
	if !strings.Contains(got, `{"b":"deep"}`) || strings.Contains(got, "other") {
		t.Fatalf("nested projection failed, got %q", got)
	}
}

func TestCountStar(t *testing.T) {
	in := Input{Format: "CSV", CSV: CSVInput{FileHeaderInfo: "USE"}}
	out := Output{Format: "CSV"}
	d := run(t, "SELECT COUNT(*) FROM S3Object", in, out, csvWithHeader)
	d.assertTail(t)
	got := strings.TrimSpace(d.allRecords())
	if got != "3" {
		t.Fatalf("expected count 3, got %q", got)
	}
}

func TestCountStarWithWhere(t *testing.T) {
	in := Input{Format: "CSV", CSV: CSVInput{FileHeaderInfo: "USE"}}
	out := Output{Format: "CSV"}
	d := run(t, "SELECT COUNT(*) FROM S3Object s WHERE s.age > 30", in, out, csvWithHeader)
	if got := strings.TrimSpace(d.allRecords()); got != "2" {
		t.Fatalf("expected count 2, got %q", got)
	}
}

func TestLimit(t *testing.T) {
	in := Input{Format: "CSV", CSV: CSVInput{FileHeaderInfo: "USE"}}
	out := Output{Format: "CSV"}
	d := run(t, "SELECT s.name FROM S3Object s LIMIT 1", in, out, csvWithHeader)
	d.assertTail(t)
	got := strings.TrimSpace(d.allRecords())
	if got != "alice" {
		t.Fatalf("expected only alice with LIMIT 1, got %q", got)
	}
}

func TestLike(t *testing.T) {
	src := "name\napple\napricot\nbanana\n"
	in := Input{Format: "CSV", CSV: CSVInput{FileHeaderInfo: "USE"}}
	out := Output{Format: "CSV"}
	d := run(t, "SELECT name FROM S3Object WHERE name LIKE 'ap%'", in, out, src)
	got := d.allRecords()
	if !strings.Contains(got, "apple") || !strings.Contains(got, "apricot") {
		t.Fatalf("expected apple and apricot, got %q", got)
	}
	if strings.Contains(got, "banana") {
		t.Fatalf("did not expect banana, got %q", got)
	}
}

func TestLikeUnderscore(t *testing.T) {
	src := "code\nA1\nA2\nAB3\n"
	in := Input{Format: "CSV", CSV: CSVInput{FileHeaderInfo: "USE"}}
	out := Output{Format: "CSV"}
	d := run(t, "SELECT code FROM S3Object WHERE code LIKE 'A_'", in, out, src)
	got := d.allRecords()
	if !strings.Contains(got, "A1") || !strings.Contains(got, "A2") {
		t.Fatalf("expected A1 and A2, got %q", got)
	}
	if strings.Contains(got, "AB3") {
		t.Fatalf("did not expect AB3 (two chars after A), got %q", got)
	}
}

func TestStarProjectionCSV(t *testing.T) {
	in := Input{Format: "CSV", CSV: CSVInput{FileHeaderInfo: "USE"}}
	out := Output{Format: "CSV"}
	d := run(t, "SELECT * FROM S3Object s WHERE s.city = 'rome'", in, out, csvWithHeader)
	got := strings.TrimSpace(d.allRecords())
	if got != "bob,20,rome" {
		t.Fatalf("expected full row for bob, got %q", got)
	}
}

func TestBooleanCombination(t *testing.T) {
	in := Input{Format: "CSV", CSV: CSVInput{FileHeaderInfo: "USE"}}
	out := Output{Format: "CSV"}
	d := run(t, "SELECT s.name FROM S3Object s WHERE s.age > 30 AND NOT (s.city = 'berlin')", in, out, csvWithHeader)
	got := d.allRecords()
	if !strings.Contains(got, "alice") {
		t.Fatalf("expected alice, got %q", got)
	}
	if strings.Contains(got, "carol") || strings.Contains(got, "bob") {
		t.Fatalf("expected only alice, got %q", got)
	}
}

func TestCast(t *testing.T) {
	in := Input{Format: "CSV", CSV: CSVInput{FileHeaderInfo: "USE"}}
	out := Output{Format: "CSV"}
	d := run(t, "SELECT s.name FROM S3Object s WHERE CAST(s.age AS INT) >= 42", in, out, csvWithHeader)
	got := strings.TrimSpace(d.allRecords())
	if got != "carol" {
		t.Fatalf("expected carol, got %q", got)
	}
}

func TestGzip(t *testing.T) {
	var gzbuf bytes.Buffer
	gw := gzip.NewWriter(&gzbuf)
	if _, err := gw.Write([]byte(csvWithHeader)); err != nil {
		t.Fatal(err)
	}
	gw.Close()

	in := Input{Format: "CSV", CSV: CSVInput{FileHeaderInfo: "USE"}, CompressionType: "GZIP"}
	out := Output{Format: "CSV"}
	var res bytes.Buffer
	if err := Execute("SELECT s.name FROM S3Object s WHERE s.age > 30", in, out, &gzbuf, &res); err != nil {
		t.Fatalf("Execute gzip: %v", err)
	}
	d := decodeStream(t, &res)
	d.assertTail(t)
	got := d.allRecords()
	if !strings.Contains(got, "alice") || !strings.Contains(got, "carol") {
		t.Fatalf("gzip query wrong result: %q", got)
	}
}

func TestHeaderIgnore(t *testing.T) {
	in := Input{Format: "CSV", CSV: CSVInput{FileHeaderInfo: "IGNORE"}}
	out := Output{Format: "CSV"}
	d := run(t, "SELECT _1 FROM S3Object", in, out, csvWithHeader)
	got := d.allRecords()
	if strings.Contains(got, "name") {
		t.Fatalf("header row should have been skipped, got %q", got)
	}
	if !strings.Contains(got, "alice") {
		t.Fatalf("expected data rows, got %q", got)
	}
}

func TestMalformedSQL(t *testing.T) {
	cases := []string{
		"SELCT * FROM S3Object",
		"SELECT * S3Object",
		"SELECT * FROM Table",
		"SELECT * FROM S3Object WHERE",
		"SELECT * FROM S3Object LIMIT abc",
		"SELECT 'unterminated FROM S3Object",
		"SELECT COUNT(name) FROM S3Object",
		"SELECT * FROM S3Object WHERE a = ",
		"SELECT * FROM S3Object trailing junk here",
	}
	for _, sql := range cases {
		var buf bytes.Buffer
		err := Execute(sql, Input{Format: "CSV"}, Output{Format: "CSV"}, strings.NewReader("a,b\n"), &buf)
		if err == nil {
			t.Errorf("expected error for malformed SQL %q, got nil", sql)
		}
	}
}

func TestStreamStructure(t *testing.T) {
	in := Input{Format: "CSV", CSV: CSVInput{FileHeaderInfo: "USE"}}
	out := Output{Format: "CSV"}
	d := run(t, "SELECT s.name FROM S3Object s", in, out, csvWithHeader)
	// Expect: Records..., Stats, End
	if d.types[len(d.types)-1] != "End" {
		t.Fatalf("last message must be End, got %v", d.types)
	}
	if d.types[len(d.types)-2] != "Stats" {
		t.Fatalf("second-to-last message must be Stats, got %v", d.types)
	}
	if d.types[0] != "Records" {
		t.Fatalf("first message must be Records, got %v", d.types)
	}
}
