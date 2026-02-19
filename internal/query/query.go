package query

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"

	"github.com/google/uuid"
)

// Params wraps form values parsed from an AWS Query API request.
type Params struct {
	values url.Values
}

// ParseRequest parses form-encoded body from an HTTP request.
func ParseRequest(r *http.Request) (Params, error) {
	if err := r.ParseForm(); err != nil {
		return Params{}, fmt.Errorf("parse form: %w", err)
	}
	return Params{values: r.Form}, nil
}

// Action returns the Action parameter.
func (p Params) Action() string {
	return p.values.Get("Action")
}

// Get returns the value for a key.
func (p Params) Get(key string) string {
	return p.values.Get(key)
}

// NewRequestID generates a new UUID-based request ID.
func NewRequestID() string {
	return uuid.New().String()
}

// ResponseMetadata is the standard AWS response metadata.
type ResponseMetadata struct {
	XMLName   xml.Name `xml:"ResponseMetadata"`
	RequestID string   `xml:"RequestId"`
}

// WriteXML writes an XML response with the given status code.
func WriteXML(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(status)
	_, _ = fmt.Fprint(w, xml.Header)
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(v)
}

// ErrorResponse is the standard AWS error response.
type ErrorResponse struct {
	XMLName   xml.Name   `xml:"ErrorResponse"`
	Error     ErrorEntry `xml:"Error"`
	RequestID string     `xml:"RequestId"`
}

// ErrorEntry holds a single error.
type ErrorEntry struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// WriteError writes an AWS-style XML error response.
func WriteError(w http.ResponseWriter, status int, code, message string) {
	resp := ErrorResponse{
		Error: ErrorEntry{
			Type:    "Sender",
			Code:    code,
			Message: message,
		},
		RequestID: NewRequestID(),
	}
	WriteXML(w, status, resp)
}
