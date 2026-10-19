// Package httpapi is the HTTP edge: decode request, call the service, encode
// response. No business rules live here.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/dinhcongdat866/customs-api-go/internal/shipment"
)

// ShipmentService is what the handlers need; tests pass a fake.
type ShipmentService interface {
	Ingest(ctx context.Context, customerID, idemKey string, items []shipment.NewShipmentInput) (*shipment.IngestResult, error)
	Get(ctx context.Context, id string) (*shipment.Shipment, error)
}

type Pinger interface {
	Ping(ctx context.Context) error
}

type Server struct {
	svc ShipmentService
	db  Pinger
	log *slog.Logger
}

// New wires routes and middleware and returns the root handler.
func New(svc ShipmentService, db Pinger, log *slog.Logger) http.Handler {
	s := &Server{svc: svc, db: db, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /shipments", s.handleIngest)
	mux.HandleFunc("GET /shipments/{id}", s.handleGetShipment)

	return s.logging(mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.db.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "db unreachable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type ingestRequest struct {
	CustomerID string                      `json:"customerId"`
	Items      []shipment.NewShipmentInput `json:"items"`
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB cap

	var req ingestRequest
	dec := json.NewDecoder(r.Body)
	dec.UseNumber() // keep declaredValue as text, never float
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	res, err := s.svc.Ingest(r.Context(), req.CustomerID, r.Header.Get("Idempotency-Key"), req.Items)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}

	status := http.StatusCreated
	if res.Replayed {
		status = http.StatusOK
	}
	writeJSON(w, status, res)
}

func (s *Server) handleGetShipment(w http.ResponseWriter, r *http.Request) {
	sh, err := s.svc.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sh)
}

// writeServiceError maps domain errors to HTTP codes. Anything unknown is a
// 500 with the detail logged, never sent to the client.
func (s *Server) writeServiceError(w http.ResponseWriter, err error) {
	var ve *shipment.ValidationError
	switch {
	case errors.As(err, &ve):
		writeError(w, http.StatusBadRequest, ve.Msg)
	case errors.Is(err, shipment.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	default:
		s.log.Error("request failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// logging is middleware: a closure around the next handler that records
// method, path, status and duration for every request.
func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.Info("http", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "ms", time.Since(start).Milliseconds())
	})
}

// statusRecorder captures the status code, which http.ResponseWriter
// otherwise does not expose after the fact.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
