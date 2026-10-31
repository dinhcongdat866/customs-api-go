package shipment

import (
	"testing"
	"time"
)

func TestCursorRoundTrip(t *testing.T) {
	in := Cursor{CreatedAt: time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC), ID: "abc"}
	out, err := DecodeCursor(in.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if !out.CreatedAt.Equal(in.CreatedAt) || out.ID != in.ID {
		t.Fatalf("got %+v, want %+v", out, in)
	}
}

func TestDecodeCursor_Garbage(t *testing.T) {
	// not base64, base64 of "not json", base64 of "{}"
	for _, s := range []string{"not base64!", "bm90IGpzb24", "e30"} {
		if _, err := DecodeCursor(s); err == nil {
			t.Errorf("%q: expected error", s)
		}
	}
}
