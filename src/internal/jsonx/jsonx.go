// Package jsonx: the one strict JSON decoder every contract package uses.
package jsonx

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// DecodeStrict has json.Unmarshal semantics plus DisallowUnknownFields, and
// rejects trailing data after the value.
func DecodeStrict(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after the JSON value")
	}
	return nil
}
