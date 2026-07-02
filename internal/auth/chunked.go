package auth

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// chunkedReader decodes an aws-chunked (STREAMING-AWS4-HMAC-SHA256-PAYLOAD)
// request body, validating each chunk's signature as it goes. Each chunk on the
// wire has the form:
//
//	<hex-length>;chunk-signature=<hex-signature>\r\n<data>\r\n
//
// terminated by a zero-length chunk. Reading the chunkedReader yields the
// concatenated decoded <data> bytes; a signature mismatch surfaces as an error
// from Read.
type chunkedReader struct {
	br         *bufio.Reader
	src        io.Closer
	signingKey []byte
	scope      string
	amzDate    string
	prevSig    string // seeded with the header (seed) signature

	pending bytes.Buffer // decoded, not-yet-returned data
	done    bool
	err     error
}

func newChunkedReader(body io.ReadCloser, signingKey []byte, scope, amzDate, seedSignature string) *chunkedReader {
	return &chunkedReader{
		br:         bufio.NewReaderSize(body, 64*1024),
		src:        body,
		signingKey: signingKey,
		scope:      scope,
		amzDate:    amzDate,
		prevSig:    seedSignature,
	}
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	for c.pending.Len() == 0 {
		if c.err != nil {
			return 0, c.err
		}
		if c.done {
			return 0, io.EOF
		}
		if err := c.readChunk(); err != nil {
			c.err = err
			// Return any error immediately; on EOF-with-no-data fall through.
			if c.pending.Len() == 0 {
				return 0, err
			}
		}
	}
	return c.pending.Read(p)
}

// readChunk reads and validates the next chunk, appending its data to pending.
func (c *chunkedReader) readChunk() error {
	header, err := c.br.ReadString('\n')
	if err != nil {
		if err == io.EOF && strings.TrimSpace(header) == "" {
			// Clean end of stream after the terminating chunk was consumed.
			c.done = true
			return io.EOF
		}
		return fmt.Errorf("aws-chunked: reading chunk header: %w", err)
	}
	header = strings.TrimRight(header, "\r\n")
	if header == "" {
		// Trailer/blank line after the terminating chunk (trailer variant): drain.
		c.done = true
		c.drainTrailers()
		return io.EOF
	}

	size, sig, err := parseChunkHeader(header)
	if err != nil {
		return err
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(c.br, data); err != nil {
		return fmt.Errorf("aws-chunked: reading chunk data: %w", err)
	}

	// Validate this chunk's signature against the running chain.
	if err := c.validateChunk(sig, data); err != nil {
		return err
	}
	c.prevSig = sig

	if size > 0 {
		// Consume the trailing CRLF after chunk data.
		if err := c.consumeCRLF(); err != nil {
			return err
		}
		c.pending.Write(data)
		return nil
	}

	// Zero-length terminating chunk. There may be trailers (trailer variant)
	// or a final CRLF; drain whatever remains and finish.
	c.done = true
	c.drainTrailers()
	return io.EOF
}

func (c *chunkedReader) validateChunk(sig string, data []byte) error {
	stringToSign := strings.Join([]string{
		chunkStringToSignPrefix,
		c.amzDate,
		c.scope,
		c.prevSig,
		emptyStringSHA256,
		hexSHA256(data),
	}, "\n")
	expected := hex.EncodeToString(hmacSHA256(c.signingKey, []byte(stringToSign)))
	if !constantTimeEqualHex(expected, sig) {
		return errors.New("aws-chunked: chunk signature does not match")
	}
	return nil
}

// consumeCRLF reads and verifies the "\r\n" that follows chunk data.
func (c *chunkedReader) consumeCRLF() error {
	b := make([]byte, 2)
	if _, err := io.ReadFull(c.br, b); err != nil {
		return fmt.Errorf("aws-chunked: reading chunk terminator: %w", err)
	}
	if b[0] != '\r' || b[1] != '\n' {
		return errors.New("aws-chunked: malformed chunk terminator")
	}
	return nil
}

// drainTrailers reads and discards any trailer headers / trailing bytes after
// the terminating chunk. Trailer signatures are not validated (best effort).
func (c *chunkedReader) drainTrailers() {
	for {
		line, err := c.br.ReadString('\n')
		if strings.TrimRight(line, "\r\n") == "" {
			// Blank line terminates trailers, or nothing more to read.
			return
		}
		if err != nil {
			return
		}
	}
}

// parseChunkHeader parses "<hexlen>;chunk-signature=<sig>" (the signature part
// is optional to tolerate trailer-only lines, but data chunks always include it).
func parseChunkHeader(header string) (size int64, sig string, err error) {
	semi := strings.IndexByte(header, ';')
	sizeStr := header
	if semi >= 0 {
		sizeStr = header[:semi]
		ext := header[semi+1:]
		for _, kv := range strings.Split(ext, ";") {
			kv = strings.TrimSpace(kv)
			if v, ok := strings.CutPrefix(kv, "chunk-signature="); ok {
				sig = v
			}
		}
	}
	size, err = strconv.ParseInt(strings.TrimSpace(sizeStr), 16, 64)
	if err != nil || size < 0 {
		return 0, "", fmt.Errorf("aws-chunked: invalid chunk size %q", sizeStr)
	}
	if sig == "" {
		return 0, "", errors.New("aws-chunked: missing chunk-signature")
	}
	return size, sig, nil
}
