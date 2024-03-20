package seekinghttp

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Logger interface {
	Infof(format string, args ...interface{})
	Debugf(format string, args ...interface{})
}

// SeekingHTTP uses a series of HTTP GETs with Range headers to implement
// io.ReadSeeker and io.ReaderAt.
//
// NOTE: SeekingHTTP is NOT concurrency safe!
type SeekingHTTP struct {
	URL       string
	MinFetch  int64
	KnownSize *int64
	Logger    Logger
	Client    HttpClient

	url        *url.URL
	offset     int64
	last       *bytes.Buffer
	lastOffset int64
}

// _ is a type assertion
var (
	_ io.ReadSeeker = (*SeekingHTTP)(nil)
	_ io.ReaderAt   = (*SeekingHTTP)(nil)
)

// New initializes a SeekingHTTP for the given URL.
func New(url string) *SeekingHTTP {
	return NewWithClient(url, http.DefaultClient)
}

// NewWithClient initializes a SeekingHTTP for the given URL with a client..
func NewWithClient(url string, client HttpClient) *SeekingHTTP {
	return &SeekingHTTP{URL: url, Client: client, MinFetch: 1024 * 1024}
}

func (s *SeekingHTTP) SetLogger(logger Logger) {
	s.Logger = logger
}

func (s *SeekingHTTP) newReq() (*http.Request, error) {
	var err error
	if s.url == nil {
		s.url, err = url.Parse(s.URL)
		if err != nil {
			return nil, err
		}
	}
	return http.NewRequest("GET", s.url.String(), nil)
}

func fmtRange(from, l int64) string {
	var to int64
	if l == 0 {
		to = from
	} else {
		to = from + (l - 1)
	}

	var sb strings.Builder
	sb.Grow(24)
	_, _ = sb.WriteString("bytes=")
	_, _ = sb.WriteString(strconv.FormatInt(from, 10))
	_, _ = sb.WriteString("-")
	_, _ = sb.WriteString(strconv.FormatInt(to, 10))
	return sb.String()
}

// ReadAt reads len(buf) bytes into buf starting at offset off.
// Returns the length read into buf.
func (s *SeekingHTTP) ReadAt(buf []byte, off int64) (n int, err error) {
	n, err = s.ReadAtWithLength(buf, off, int64(len(buf)))
	n = min(len(buf), n)
	if n != len(buf) && err == nil {
		// ReadAt must always return len(buf), nil
		err = io.EOF
	}
	return n, err
}

// ReadAtWithLength reads length bytes into buf starting at offset off.
// If the buffer is short, reads the full length & copies the beginning to the buffer.
// The minimum read size is controlled by MinFetch.
// Returns min(full length read, length) (may be larger than len(buf))
func (s *SeekingHTTP) ReadAtWithLength(buf []byte, off, length int64) (n int, err error) {
	if s.Logger != nil {
		s.Logger.Debugf("ReadAt len %v off %v", length, off)
	}

	if off < 0 {
		return 0, io.EOF
	}

	// Set the length to be at least MinFetch if set.
	if s.MinFetch != 0 {
		length = max(length, s.MinFetch)
	}

	// If the size is known, cap the length to the size.
	if s.KnownSize != nil {
		length = min(*s.KnownSize-off, length)
		if length < 0 {
			return 0, io.EOF
		}
	}

	if s.last != nil && off >= s.lastOffset {
		end := off + length
		if end <= s.lastOffset+int64(s.last.Len()) {
			start := off - s.lastOffset
			if s.Logger != nil {
				s.Logger.Debugf("cache hit: range (%v-%v) is within cache (%v-%v)", off, off+length, s.lastOffset, s.lastOffset+int64(s.last.Len()))
			}
			copy(buf, s.last.Bytes()[start:end-s.lastOffset])
			return min(len(buf), int(length)), nil
		}
	}

	if s.Logger != nil {
		if s.last != nil {
			s.Logger.Debugf("cache miss: range (%v-%v) is NOT within cache (%v-%v)", off, off+length, s.lastOffset, s.lastOffset+int64(s.last.Len()))
		} else {
			s.Logger.Debugf("cache miss: cache empty")
		}
	}

	req, err := s.newReq()
	if err != nil {
		return 0, err
	}

	rng := fmtRange(off, length)
	req.Header.Add("Range", rng)

	if s.Logger != nil {
		s.Logger.Infof("Start HTTP GET with Range: %s", rng)
	}

	resp, err := s.Client.Do(req)
	if err != nil {
		return 0, err
	}

	// body needs to be closed, even if responses that aren't 200 or 206
	defer func(body io.ReadCloser) {
		_, cErr := io.Copy(io.Discard, body)
		if cErr == nil {
			cErr = body.Close()
		} else {
			_ = body.Close()
		}
		if err == nil && cErr != nil {
			err = cErr
		}
	}(resp.Body)

	if s.Logger != nil {
		s.Logger.Infof("Response status: %v", resp.StatusCode)
	}

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
		s.lastOffset = off
		if s.last == nil {
			// Cache does not exist yet. So make it.
			s.last = &bytes.Buffer{}
		} else {
			// Cache is getting replaced. Bring it back to zero bytes, but
			// keep the underlying []byte, since we'll reuse it right away.
			s.last.Reset()
		}

		n, err := s.last.ReadFrom(resp.Body)
		if err != nil {
			return 0, err
		}

		contentLength := resp.ContentLength
		if contentLength == 0 {
			// for some reason the content length header was not set
			contentLength = n
		} else if n != contentLength {
			return 0, errors.Errorf("read %d bytes but content length indicated %d", n, contentLength)
		} else if resp.StatusCode == http.StatusOK && s.KnownSize == nil {
			// status 200 = this is the full file, set the size.
			size := contentLength
			s.KnownSize = &size
		}

		if s.Logger != nil {
			s.Logger.Debugf("loaded %d bytes into last", contentLength)
		}

		n = min(contentLength, length)
		bufN := min(int(n), len(buf))
		copy(buf, s.last.Bytes())

		return bufN, err
	}

	return 0, io.EOF
}

func (s *SeekingHTTP) Read(buf []byte) (int, error) {
	if s.Logger != nil {
		s.Logger.Debugf("got read len %v", len(buf))
	}

	n, err := s.ReadAt(buf, s.offset)
	if err == nil {
		s.offset += int64(n)
	}

	return n, err
}

// Seek sets the offset for the next Read.
func (s *SeekingHTTP) Seek(offset int64, whence int) (int64, error) {
	if s.Logger != nil {
		s.Logger.Debugf("got seek %v %v", offset, whence)
	}

	switch whence {
	case io.SeekStart:
		s.offset = offset
	case io.SeekCurrent:
		s.offset += offset
	case io.SeekEnd:
		var length int64
		if s.KnownSize != nil {
			length = *s.KnownSize
		} else {
			var err error
			length, err = s.Size()
			if err != nil {
				return 0, err
			}
		}

		s.offset = length + offset
		if s.offset > length || s.offset < 0 {
			return 0, io.EOF
		}
	default:
		return 0, os.ErrInvalid
	}

	return s.offset, nil
}

// Size uses an HTTP HEAD to find out how many bytes are available in total.
func (s *SeekingHTTP) Size() (int64, error) {
	if s.KnownSize != nil {
		return *s.KnownSize, nil
	}

	req, err := s.newReq()
	if err != nil {
		return 0, err
	}
	req.Method = "HEAD"

	resp, err := s.Client.Do(req)
	if err != nil {
		return 0, err
	}

	if resp.ContentLength < 0 {
		return 0, errors.New("no content length for Size()")
	}

	length := resp.ContentLength
	if s.Logger != nil {
		s.Logger.Debugf("url: %v, size %v", req.URL.String(), length)
	}
	if length < 0 {
		return 0, errors.New("invalid negative content legnth returned")
	}

	s.KnownSize = &length
	return resp.ContentLength, nil
}
