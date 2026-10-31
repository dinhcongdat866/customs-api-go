package shipment

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound = errors.New("not found")
	// ErrConflict: the row exists but is no longer in the state the caller
	// assumed (someone else got there first).
	ErrConflict = errors.New("conflict")
)

// Repo is everything the service needs from storage. Handlers and tests
// talk to this interface, never to pgx directly.
type Repo interface {
	CreateBatch(ctx context.Context, b *Batch, items []NewShipmentInput) ([]Shipment, error)
	FindBatchByKey(ctx context.Context, key string) (*Batch, error)
	GetBatch(ctx context.Context, key string) (*Batch, error)
	GetShipment(ctx context.Context, id string) (*Shipment, error)
	ListByBatch(ctx context.Context, batchID string) ([]Shipment, error)
	ListPendingIDs(ctx context.Context) ([]string, error)
	ListReviewQueue(ctx context.Context, q ReviewQuery) ([]Shipment, error)
	ApplyDecision(ctx context.Context, id string, d ReviewDecision, finalHSCode string) (*Shipment, error)
	SetStatus(ctx context.Context, id string, s Status) error
	SaveClassification(ctx context.Context, c Classification, newStatus Status) error
}

type PGRepo struct{ pool *pgxpool.Pool }

func NewPGRepo(pool *pgxpool.Pool) *PGRepo { return &PGRepo{pool: pool} }

// Compile-time check that PGRepo really satisfies Repo.
var _ Repo = (*PGRepo)(nil)

const shipmentCols = `
	id, batch_id, external_ref, raw_title, raw_description, declared_value::text,
	currency, origin_country, destination_country, quantity, material,
	hs_code, hs_confidence, status, created_at, updated_at`

func scanShipment(row pgx.Row) (*Shipment, error) {
	var s Shipment
	var status string
	err := row.Scan(
		&s.ID, &s.BatchID, &s.ExternalRef, &s.RawTitle, &s.RawDescription, &s.DeclaredValue,
		&s.Currency, &s.OriginCountry, &s.DestinationCountry, &s.Quantity, &s.Material,
		&s.HSCode, &s.HSConfidence, &status, &s.CreatedAt, &s.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	s.Status = Status(status)
	return &s, nil
}

// CreateBatch writes the batch and all its shipments in ONE transaction:
// either everything is stored or nothing is. Fills b.ID / b.CreatedAt.
func (r *PGRepo) CreateBatch(ctx context.Context, b *Batch, items []NewShipmentInput) ([]Shipment, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) // no-op once Commit succeeded

	err = tx.QueryRow(ctx, `
		INSERT INTO goapi.batches (source, customer_id, idempotency_key, item_count)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at`,
		b.Source, b.CustomerID, b.IdempotencyKey, len(items),
	).Scan(&b.ID, &b.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert batch: %w", err)
	}
	b.ItemCount = len(items)

	out := make([]Shipment, 0, len(items))
	for _, in := range items {
		qty := 1
		if in.Quantity != nil {
			qty = *in.Quantity
		}
		var value *string
		if in.DeclaredValue != nil {
			v := in.DeclaredValue.String()
			value = &v
		}
		row := tx.QueryRow(ctx, `
			INSERT INTO goapi.shipments
			  (batch_id, external_ref, raw_title, raw_description, declared_value, currency,
			   origin_country, destination_country, quantity, material)
			VALUES ($1, $2, $3, $4, $5::text::numeric, $6, $7, $8, $9, $10)
			RETURNING `+shipmentCols,
			b.ID, in.ExternalRef, in.RawTitle, in.RawDescription, value, in.Currency,
			in.OriginCountry, in.DestinationCountry, qty, in.Material,
		)
		s, err := scanShipment(row)
		if err != nil {
			return nil, fmt.Errorf("insert shipment %q: %w", in.RawTitle, err)
		}
		out = append(out, *s)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return out, nil
}

func (r *PGRepo) FindBatchByKey(ctx context.Context, key string) (*Batch, error) {
	var b Batch
	err := r.pool.QueryRow(ctx, `
		SELECT id, source, customer_id, idempotency_key, item_count, created_at
		FROM goapi.batches WHERE idempotency_key = $1`, key,
	).Scan(&b.ID, &b.Source, &b.CustomerID, &b.IdempotencyKey, &b.ItemCount, &b.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find batch: %w", err)
	}
	return &b, nil
}

func (r *PGRepo) GetBatch(ctx context.Context, key string) (*Batch, error) {
	var b Batch
	err := r.pool.QueryRow(ctx, `
		SELECT id, source, customer_id, idempotency_key, item_count, created_at
		FROM goapi.batches WHERE id = $1`, key,
	).Scan(&b.ID, &b.Source, &b.CustomerID, &b.IdempotencyKey, &b.ItemCount, &b.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find batch: %w", err)
	}
	return &b, nil
}

func (r *PGRepo) GetShipment(ctx context.Context, id string) (*Shipment, error) {
	s, err := scanShipment(r.pool.QueryRow(ctx,
		`SELECT `+shipmentCols+` FROM goapi.shipments WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get shipment: %w", err)
	}
	return s, nil
}

func (r *PGRepo) ListByBatch(ctx context.Context, batchID string) ([]Shipment, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+shipmentCols+` FROM goapi.shipments WHERE batch_id = $1 ORDER BY created_at`, batchID)
	if err != nil {
		return nil, fmt.Errorf("list by batch: %w", err)
	}
	defer rows.Close()

	var out []Shipment
	for rows.Next() {
		s, err := scanShipment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *s)
	}
	return out, rows.Err()
}

// ListPendingIDs finds work that was stored but never finished (server died
// between commit and enqueue, or mid-classification). Called at startup.
func (r *PGRepo) ListPendingIDs(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id FROM goapi.shipments WHERE status IN ('received', 'processing') ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list pending: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ListReviewQueue returns up to q.Limit shipments waiting for a human,
// oldest first, starting after q.After (keyset pagination: no OFFSET, so
// page 1000 costs the same as page 1 and rows never shift between pages).
func (r *PGRepo) ListReviewQueue(ctx context.Context, q ReviewQuery) ([]Shipment, error) {
	sql := `SELECT ` + shipmentCols + `
		FROM goapi.shipments s`
	args := []any{string(StatusNeedsReview)}
	where := ` WHERE s.status = $1`

	if q.CustomerID != "" {
		// customer_id lives on the batch, not the shipment. A sub-select
		// instead of a JOIN keeps the shared column list unambiguous.
		args = append(args, q.CustomerID)
		where += fmt.Sprintf(` AND s.batch_id IN (SELECT id FROM goapi.batches WHERE customer_id = $%d)`, len(args))
	}
	if q.After != nil {
		// Row comparison: "later than the cursor" in the same order as ORDER BY.
		args = append(args, q.After.CreatedAt, q.After.ID)
		where += fmt.Sprintf(` AND (s.created_at, s.id) > ($%d, $%d::uuid)`, len(args)-1, len(args))
	}
	args = append(args, q.Limit)
	sql += where + fmt.Sprintf(` ORDER BY s.created_at, s.id LIMIT $%d`, len(args))

	rows, err := r.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("list review queue: %w", err)
	}
	defer rows.Close()

	out := make([]Shipment, 0, q.Limit)
	for rows.Next() {
		s, err := scanShipment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *s)
	}
	return out, rows.Err()
}

// ApplyDecision moves a needs_review shipment to cleared and writes the audit
// row, atomically. The WHERE status = 'needs_review' is the concurrency
// guard: if two reviewers decide the same shipment at once, exactly one
// UPDATE matches; the other sees 0 rows and gets ErrConflict. No SELECT FOR
// UPDATE needed because the state check and the write are one statement.
func (r *PGRepo) ApplyDecision(ctx context.Context, id string, d ReviewDecision, finalHSCode string) (*Shipment, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var before *string
	err = tx.QueryRow(ctx,
		`SELECT hs_code FROM goapi.shipments WHERE id = $1`, id).Scan(&before)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load before: %w", err)
	}

	row := tx.QueryRow(ctx, `
		UPDATE goapi.shipments
		SET status = $2, hs_code = $3, updated_at = now()
		WHERE id = $1 AND status = $4
		RETURNING `+shipmentCols,
		id, string(StatusCleared), finalHSCode, string(StatusNeedsReview))
	sh, err := scanShipment(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrConflict // exists, but not needs_review any more
	}
	if err != nil {
		return nil, fmt.Errorf("apply decision: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO goapi.audit_log (shipment_id, actor, action, detail)
		VALUES ($1, $2, $3, $4)`,
		id, "user:"+d.Reviewer, "review_"+string(d.Decision), map[string]any{
			"hsCodeBefore": before, "hsCodeAfter": finalHSCode, "note": d.Note,
		})
	if err != nil {
		return nil, fmt.Errorf("insert audit: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return sh, nil
}

func (r *PGRepo) SetStatus(ctx context.Context, id string, s Status) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE goapi.shipments SET status = $2, updated_at = now() WHERE id = $1`, id, string(s))
	if err != nil {
		return fmt.Errorf("set status: %w", err)
	}
	return nil
}

// SaveClassification records the attempt, updates the shipment's current
// answer and writes the audit row: all or nothing.
func (r *PGRepo) SaveClassification(ctx context.Context, c Classification, newStatus Status) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO goapi.classifications
		  (shipment_id, candidates, chosen_hs_code, confidence, model, prompt_version, latency_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		c.ShipmentID, c.Candidates, c.ChosenHSCode, c.Confidence, c.Model, c.PromptVersion, c.LatencyMs)
	if err != nil {
		return fmt.Errorf("insert classification: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE goapi.shipments
		SET hs_code = $2, hs_confidence = $3, status = $4, updated_at = now()
		WHERE id = $1`,
		c.ShipmentID, c.ChosenHSCode, c.Confidence, string(newStatus))
	if err != nil {
		return fmt.Errorf("update shipment: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO goapi.audit_log (shipment_id, actor, action, detail)
		VALUES ($1, 'system:classifier', 'classified', $2)`,
		c.ShipmentID, map[string]any{
			"hsCode": c.ChosenHSCode, "confidence": c.Confidence,
			"model": c.Model, "routedTo": newStatus,
		})
	if err != nil {
		return fmt.Errorf("insert audit: %w", err)
	}

	return tx.Commit(ctx)
}
