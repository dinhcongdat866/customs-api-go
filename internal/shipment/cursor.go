package shipment

import (
	"encoding/base64"
	"encoding/json"
	"time"
)

// Cursor marks "where the previous page ended" for keyset pagination.
// (created_at, id) is unique, so the next page is simply every row after it.
type Cursor struct {
	CreatedAt time.Time `json:"t"`
	ID        string    `json:"id"`
}

// Encode turns the cursor into an opaque string. Clients pass it back
// unchanged; the format is ours to change.
func (c Cursor) Encode() string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor is the inverse of Encode. A bad string is a client error.
func DecodeCursor(s string) (*Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, &ValidationError{"cursor is not valid"}
	}
	var c Cursor
	if err := json.Unmarshal(b, &c); err != nil || c.ID == "" || c.CreatedAt.IsZero() {
		return nil, &ValidationError{"cursor is not valid"}
	}
	return &c, nil
}
