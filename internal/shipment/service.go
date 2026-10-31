package shipment

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5/pgconn"
)

// ValidationError is returned for bad client input; the HTTP layer maps it to 400.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

// Enqueuer hands a stored shipment to background classification.
type Enqueuer interface {
	Enqueue(shipmentID string)
}

type Service struct {
	repo  Repo
	queue Enqueuer
}

func NewService(repo Repo, queue Enqueuer) *Service {
	return &Service{repo: repo, queue: queue}
}

const maxItemsPerBatch = 500

// Ingest validates, stores the batch atomically, then queues classification.
//
// Idempotency: if the client sends the same Idempotency-Key twice (retry
// after a timeout, for example) the second call returns the first batch
// instead of creating a duplicate.
func (s *Service) Ingest(ctx context.Context, customerID, idemKey string, items []NewShipmentInput) (*IngestResult, error) {
	if err := validate(customerID, items); err != nil {
		return nil, err
	}

	if idemKey != "" {
		res, err := s.replay(ctx, idemKey)
		if err == nil {
			return res, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}

	batch := &Batch{Source: "api", CustomerID: customerID}
	if idemKey != "" {
		batch.IdempotencyKey = &idemKey
	}

	shipments, err := s.repo.CreateBatch(ctx, batch, items)
	if err != nil {
		// Two requests with the same key raced: the other one won, return its batch.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && idemKey != "" {
			return s.replay(ctx, idemKey)
		}
		return nil, err
	}

	// Known gap: if the process dies right here, rows are committed but never
	// queued. ListPendingIDs at startup re-queues them (reconciliation sweep).
	for _, sh := range shipments {
		s.queue.Enqueue(sh.ID)
	}

	return &IngestResult{Batch: *batch, Shipments: shipments}, nil
}

func (s *Service) replay(ctx context.Context, idemKey string) (*IngestResult, error) {
	b, err := s.repo.FindBatchByKey(ctx, idemKey)
	if err != nil {
		return nil, err
	}
	shipments, err := s.repo.ListByBatch(ctx, b.ID)
	if err != nil {
		return nil, err
	}
	return &IngestResult{Batch: *b, Shipments: shipments, Replayed: true}, nil
}

const (
	defaultPageSize = 50
	maxPageSize     = 200
)

// ListReviews returns one page of the review queue. It asks the repo for one
// extra row: if it comes back, there is a next page and the cursor points at
// the last row we actually return.
func (s *Service) ListReviews(ctx context.Context, customerID, cursor string, limit int) (*ReviewPage, error) {
	if limit <= 0 {
		limit = defaultPageSize
	}
	if limit > maxPageSize {
		return nil, &ValidationError{fmt.Sprintf("limit must be at most %d", maxPageSize)}
	}
	q := ReviewQuery{CustomerID: customerID, Limit: limit + 1}
	if cursor != "" {
		c, err := DecodeCursor(cursor)
		if err != nil {
			return nil, err
		}
		q.After = c
	}

	items, err := s.repo.ListReviewQueue(ctx, q)
	if err != nil {
		return nil, err
	}

	page := &ReviewPage{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		last := page.Items[limit-1]
		next := Cursor{CreatedAt: last.CreatedAt, ID: last.ID}.Encode()
		page.NextCursor = &next
	}
	if page.Items == nil {
		page.Items = []Shipment{} // "items": [] rather than null
	}
	return page, nil
}

func (s *Service) Get(ctx context.Context, id string) (*Shipment, error) {
	return s.repo.GetShipment(ctx, id)
}

func (s *Service) GetBatch(ctx context.Context, id string) (*IngestResult, error) {
	b, err := s.repo.GetBatch(ctx, id)
	if err != nil {
		return nil, err
	}
	shipments, err := s.repo.ListByBatch(ctx, b.ID)
	if err != nil {
		return nil, err
	}
	return &IngestResult{Batch: *b, Shipments: shipments}, nil
}

func validate(customerID string, items []NewShipmentInput) error {
	if customerID == "" {
		return &ValidationError{"customerId is required"}
	}
	if len(items) == 0 {
		return &ValidationError{"items must not be empty"}
	}
	if len(items) > maxItemsPerBatch {
		return &ValidationError{fmt.Sprintf("at most %d items per batch", maxItemsPerBatch)}
	}
	for i, it := range items {
		if it.RawTitle == "" {
			return &ValidationError{fmt.Sprintf("items[%d].rawTitle is required", i)}
		}
		if it.Quantity != nil && *it.Quantity < 1 {
			return &ValidationError{fmt.Sprintf("items[%d].quantity must be >= 1", i)}
		}
		countries := map[string]*string{
			"originCountry":      it.OriginCountry,
			"destinationCountry": it.DestinationCountry,
		}
		for name, c := range countries {
			if c != nil && len(*c) != 2 {
				return &ValidationError{fmt.Sprintf("items[%d].%s must be an ISO-2 code", i, name)}
			}
		}
		if it.DeclaredValue != nil {
			// Parse exactly (no float): reject garbage and negatives before the DB sees them.
			v, ok := new(big.Rat).SetString(it.DeclaredValue.String())
			if !ok || v.Sign() < 0 {
				return &ValidationError{fmt.Sprintf("items[%d].declaredValue must be a non-negative number", i)}
			}
		}
	}
	return nil
}
