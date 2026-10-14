package httpx

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// MaxBodyBytes is the request body limit from the v1 contract.
const MaxBodyBytes = 1 << 20

// DecodeJSON strictly decodes the request body into dst. Unlike gin binding it
// rejects unknown fields, duplicate keys and trailing data after the JSON value.
// On failure it writes the error response and returns false.
func DecodeJSON(c *gin.Context, dst any) bool {
	data, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			WriteError(c, http.StatusRequestEntityTooLarge, CodeBodyTooLarge, "Тело запроса превышает допустимый размер")
			return false
		}
		WriteError(c, http.StatusBadRequest, CodeMalformedJSON, "Не удалось прочитать тело запроса")
		return false
	}

	if err := checkDuplicateKeys(data); err != nil {
		var dup *duplicateKeyError
		if errors.As(err, &dup) {
			WriteError(c, http.StatusBadRequest, CodeMalformedJSON, "Повторяющийся ключ JSON",
				FieldError{Path: dup.path, Code: FieldDuplicateKey})
			return false
		}
		WriteError(c, http.StatusBadRequest, CodeMalformedJSON, "Некорректный JSON")
		return false
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeDecodeError(c, err)
		return false
	}
	// checkDuplicateKeys already guarantees exactly one JSON value; this guards
	// against the two parsers ever disagreeing.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		WriteError(c, http.StatusBadRequest, CodeMalformedJSON, "Тело запроса должно содержать один JSON-объект")
		return false
	}
	return true
}

func writeDecodeError(c *gin.Context, err error) {
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		WriteError(c, http.StatusBadRequest, CodeValidationError, "Некорректный запрос",
			FieldError{Path: typeErr.Field, Code: FieldInvalidType})
		return
	}
	// encoding/json has no typed error for unknown fields.
	if name, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
		WriteError(c, http.StatusBadRequest, CodeValidationError, "Некорректный запрос",
			FieldError{Path: strings.Trim(name, `"`), Code: FieldUnknown})
		return
	}
	WriteError(c, http.StatusBadRequest, CodeMalformedJSON, "Некорректный JSON")
}

type duplicateKeyError struct {
	path string
}

func (e *duplicateKeyError) Error() string {
	return fmt.Sprintf("duplicate key %q", e.path)
}

// checkDuplicateKeys walks the whole document, including free-form objects
// such as input, and fails on the first repeated key. It also rejects
// syntax errors, empty bodies and more than one top-level value.
func checkDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := walkValue(dec, ""); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after JSON value")
	}
	return nil
}

func walkValue(dec *json.Decoder, path string) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}

	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := keyTok.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			keyPath := joinPath(path, key)
			if _, dup := seen[key]; dup {
				return &duplicateKeyError{path: keyPath}
			}
			seen[key] = struct{}{}
			if err := walkValue(dec, keyPath); err != nil {
				return err
			}
		}
	case '[':
		for i := 0; dec.More(); i++ {
			if err := walkValue(dec, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	// Closing '}' or ']'.
	_, err = dec.Token()
	return err
}

func joinPath(parent, key string) string {
	if parent == "" {
		return key
	}
	return parent + "." + key
}
