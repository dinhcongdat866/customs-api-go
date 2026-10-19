package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dinhcongdat866/customs-api-go/internal/shipment"
)

// fakeService satisfies ShipmentService without a database.
type fakeService struct {
	ingest func(customerID, key string, items []shipment.NewShipmentInput) (*shipment.IngestResult, error)
	get    func(id string) (*shipment.Shipment, error)
}

func (f *fakeService) Ingest(_ context.Context, c, k string, items []shipment.NewShipmentInput) (*shipment.IngestResult, error) {
	return f.ingest(c, k, items)
}
func (f *fakeService) Get(_ context.Context, id string) (*shipment.Shipment, error) { return f.get(id) }

type okPinger struct{}

func (okPinger) Ping(context.Context) error { return nil }

func newTestServer(svc ShipmentService) http.Handler {
	return New(svc, okPinger{}, slog.New(slog.DiscardHandler))
}

func TestHealth(t *testing.T) {
	srv := newTestServer(&fakeService{})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestIngest_BadJSON(t *testing.T) {
	srv := newTestServer(&fakeService{})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("POST", "/shipments", strings.NewReader("{not json")))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestIngest_ValidationErrorIs400(t *testing.T) {
	svc := &fakeService{
		ingest: func(string, string, []shipment.NewShipmentInput) (*shipment.IngestResult, error) {
			return nil, &shipment.ValidationError{Msg: "items must not be empty"}
		},
	}
	rec := httptest.NewRecorder()
	body := `{"customerId":"acme","items":[]}`
	newTestServer(svc).ServeHTTP(rec, httptest.NewRequest("POST", "/shipments", strings.NewReader(body)))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var resp map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["error"] != "items must not be empty" {
		t.Fatalf("error = %q", resp["error"])
	}
}

func TestIngest_CreatedAndIdempotencyKeyForwarded(t *testing.T) {
	var gotKey string
	svc := &fakeService{
		ingest: func(c, k string, items []shipment.NewShipmentInput) (*shipment.IngestResult, error) {
			gotKey = k
			if items[0].DeclaredValue.String() != "12.50" {
				t.Fatalf("declaredValue lost precision: %s", items[0].DeclaredValue.String())
			}
			return &shipment.IngestResult{
				Batch:     shipment.Batch{ID: "b1", CustomerID: c, ItemCount: len(items)},
				Shipments: []shipment.Shipment{{ID: "s1", RawTitle: items[0].RawTitle, Status: shipment.StatusReceived}},
			}, nil
		},
	}
	body := `{"customerId":"acme","items":[{"rawTitle":"Cat hoodie","declaredValue":12.50}]}`
	req := httptest.NewRequest("POST", "/shipments", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", "abc-123")
	rec := httptest.NewRecorder()
	newTestServer(svc).ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body %s", rec.Code, rec.Body.String())
	}
	if gotKey != "abc-123" {
		t.Fatalf("idempotency key = %q", gotKey)
	}
}

func TestIngest_ReplayIs200(t *testing.T) {
	svc := &fakeService{
		ingest: func(string, string, []shipment.NewShipmentInput) (*shipment.IngestResult, error) {
			return &shipment.IngestResult{Replayed: true}, nil
		},
	}
	rec := httptest.NewRecorder()
	body := `{"customerId":"acme","items":[{"rawTitle":"x"}]}`
	newTestServer(svc).ServeHTTP(rec, httptest.NewRequest("POST", "/shipments", strings.NewReader(body)))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestGetShipment_NotFound(t *testing.T) {
	svc := &fakeService{get: func(string) (*shipment.Shipment, error) { return nil, shipment.ErrNotFound }}
	rec := httptest.NewRecorder()
	newTestServer(svc).ServeHTTP(rec, httptest.NewRequest("GET", "/shipments/nope", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
