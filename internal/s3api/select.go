package s3api

import (
	"bytes"
	"encoding/xml"
	"net/http"

	"github.com/adi/d9ds3/internal/s3err"
	"github.com/adi/d9ds3/internal/s3select"
	"github.com/adi/d9ds3/internal/types"
)

// SelectObjectContent request body.
type xSelectRequest struct {
	XMLName        xml.Name      `xml:"SelectObjectContentRequest"`
	Expression     string        `xml:"Expression"`
	ExpressionType string        `xml:"ExpressionType"`
	Input          xSelectInput  `xml:"InputSerialization"`
	Output         xSelectOutput `xml:"OutputSerialization"`
}
type xSelectInput struct {
	CSV             *xSelCSVIn  `xml:"CSV"`
	JSON            *xSelJSONIn `xml:"JSON"`
	CompressionType string      `xml:"CompressionType"`
}
type xSelCSVIn struct {
	FileHeaderInfo  string `xml:"FileHeaderInfo"`
	FieldDelimiter  string `xml:"FieldDelimiter"`
	RecordDelimiter string `xml:"RecordDelimiter"`
	QuoteCharacter  string `xml:"QuoteCharacter"`
	Comments        string `xml:"Comments"`
}
type xSelJSONIn struct {
	Type string `xml:"Type"`
}
type xSelectOutput struct {
	CSV  *xSelCSVOut  `xml:"CSV"`
	JSON *xSelJSONOut `xml:"JSON"`
}
type xSelCSVOut struct {
	FieldDelimiter  string `xml:"FieldDelimiter"`
	RecordDelimiter string `xml:"RecordDelimiter"`
	QuoteCharacter  string `xml:"QuoteCharacter"`
}
type xSelJSONOut struct {
	RecordDelimiter string `xml:"RecordDelimiter"`
}

// handleSelectObject runs an S3 Select query over an object and streams the
// event-stream result. The result is buffered so a query error can be reported as
// a proper S3 error before the 200 status/body is written.
func (s *Server) handleSelectObject(rc *reqCtx) {
	if err := s.authorize(rc, actGetObject); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	body, err := readBody(rc)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	var req xSelectRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		writeErr(rc.w, rc.r, s3err.ErrMalformedXML)
		return
	}

	res, err := s.gw.GetObject(rc.bucket, rc.key, types.GetOptions{VersionID: rc.q.get("versionId")})
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	defer res.Body.Close()

	in := s3select.Input{CompressionType: req.Input.CompressionType}
	switch {
	case req.Input.JSON != nil:
		in.Format = "JSON"
		in.JSON = s3select.JSONInput{Type: req.Input.JSON.Type}
	default:
		in.Format = "CSV"
		if c := req.Input.CSV; c != nil {
			in.CSV = s3select.CSVInput{
				FileHeaderInfo: c.FileHeaderInfo, FieldDelimiter: c.FieldDelimiter,
				RecordDelimiter: c.RecordDelimiter, QuoteChar: c.QuoteCharacter, CommentChar: c.Comments,
			}
		}
	}
	out := s3select.Output{}
	if req.Output.JSON != nil {
		out.Format = "JSON"
		out.JSON = s3select.JSONOutput{RecordDelimiter: req.Output.JSON.RecordDelimiter}
	} else {
		out.Format = "CSV"
		if c := req.Output.CSV; c != nil {
			out.CSV = s3select.CSVOutput{FieldDelimiter: c.FieldDelimiter, RecordDelimiter: c.RecordDelimiter, QuoteChar: c.QuoteCharacter}
		}
	}

	var buf bytes.Buffer
	if err := s3select.Execute(req.Expression, in, out, res.Body, &buf); err != nil {
		writeErr(rc.w, rc.r, s3err.APIError{Code: "InvalidExpression", Message: err.Error(), HTTPStatus: http.StatusBadRequest})
		return
	}
	rc.w.Header().Set("Content-Type", "application/octet-stream")
	rc.w.WriteHeader(http.StatusOK)
	rc.w.Write(buf.Bytes())
}
