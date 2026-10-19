// Package shipment holds the domain: what a shipment is and how it is ingested.
package shipment

import (
	"encoding/json"
	"time"
)

// Status is the shipment's position in the pipeline (a small state machine):
//
//	received → processing → cleared | needs_review | needs_info | blocked | failed
type Status string

const (
	StatusReceived    Status = "received"
	StatusProcessing  Status = "processing"
	StatusNeedsReview Status = "needs_review"
	StatusNeedsInfo   Status = "needs_info"
	StatusCleared     Status = "cleared"
	StatusBlocked     Status = "blocked"
	StatusFailed      Status = "failed"
)

type Batch struct {
	ID             string    `json:"id"`
	Source         string    `json:"source"`
	CustomerID     string    `json:"customerId"`
	IdempotencyKey *string   `json:"-"` // never echoed back
	ItemCount      int       `json:"itemCount"`
	CreatedAt      time.Time `json:"createdAt"`
}

// Shipment is one parcel line. Pointer fields are optional (NULL in Postgres,
// omitted in JSON): a nil pointer means "not provided", unlike "" or 0.
type Shipment struct {
	ID                 string    `json:"id"`
	BatchID            string    `json:"batchId"`
	ExternalRef        *string   `json:"externalRef,omitempty"`
	RawTitle           string    `json:"rawTitle"`
	RawDescription     *string   `json:"rawDescription,omitempty"`
	DeclaredValue      *string   `json:"declaredValue,omitempty"` // "12.50": money travels as text
	Currency           *string   `json:"currency,omitempty"`
	OriginCountry      *string   `json:"originCountry,omitempty"`
	DestinationCountry *string   `json:"destinationCountry,omitempty"`
	Quantity           int       `json:"quantity"`
	Material           *string   `json:"material,omitempty"`
	HSCode             *string   `json:"hsCode,omitempty"`
	HSConfidence       *float64  `json:"hsConfidence,omitempty"`
	Status             Status    `json:"status"`
	CreatedAt          time.Time `json:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

// NewShipmentInput is what a client sends. DeclaredValue is json.Number so
// "12.50" and 12.50 are both accepted and neither is ever parsed as float.
type NewShipmentInput struct {
	ExternalRef        *string      `json:"externalRef"`
	RawTitle           string       `json:"rawTitle"`
	RawDescription     *string      `json:"rawDescription"`
	DeclaredValue      *json.Number `json:"declaredValue"`
	Currency           *string      `json:"currency"`
	OriginCountry      *string      `json:"originCountry"`
	DestinationCountry *string      `json:"destinationCountry"`
	Quantity           *int         `json:"quantity"`
	Material           *string      `json:"material"`
}

type Candidate struct {
	HSCode    string  `json:"hsCode"`
	Label     string  `json:"label,omitempty"`
	Reasoning string  `json:"reasoning,omitempty"`
	Score     float64 `json:"score"`
}

// Classification is one attempt by a model, kept forever as history.
type Classification struct {
	ShipmentID    string
	Candidates    []Candidate
	ChosenHSCode  string
	Confidence    float64
	Model         string
	PromptVersion string
	LatencyMs     int
}

type IngestResult struct {
	Batch     Batch      `json:"batch"`
	Shipments []Shipment `json:"shipments"`
	Replayed  bool       `json:"replayed"` // true when the idempotency key was seen before
}
