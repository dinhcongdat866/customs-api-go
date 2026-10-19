// Package classify runs background classification with a fixed number of
// goroutines ("worker pool"). The HTTP handler only drops an ID into the
// queue and returns; the real work happens here.
package classify

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/dinhcongdat866/customs-api-go/internal/shipment"
)

// Store is the slice of the repo this package needs (keeps it testable).
type Store interface {
	GetShipment(ctx context.Context, id string) (*shipment.Shipment, error)
	SetStatus(ctx context.Context, id string, s shipment.Status) error
	SaveClassification(ctx context.Context, c shipment.Classification, newStatus shipment.Status) error
}

// Router thresholds, same as the Next.js pipeline.
const autoClearThreshold = 0.85

type Pool struct {
	jobs    chan string
	store   Store
	workers int
	log     *slog.Logger
	wg      sync.WaitGroup
}

// NewPool creates a queue that can hold `buffer` IDs before Enqueue blocks.
func NewPool(store Store, workers, buffer int, log *slog.Logger) *Pool {
	return &Pool{
		jobs:    make(chan string, buffer),
		store:   store,
		workers: workers,
		log:     log,
	}
}

// Enqueue blocks if the queue is full: back-pressure on purpose. Dropping
// work silently would be worse than a slow request.
func (p *Pool) Enqueue(id string) { p.jobs <- id }

// Start launches the workers. Each one loops: take an ID, process it, repeat,
// until the queue is closed (Stop) or ctx is cancelled.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go func(n int) {
			defer p.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case id, ok := <-p.jobs:
					if !ok {
						return
					}
					p.handle(id)
				}
			}
		}(i)
	}
}

// Stop closes the queue and waits for in-flight work, up to timeout.
func (p *Pool) Stop(timeout time.Duration) {
	close(p.jobs)
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		p.log.Warn("classify: stop timed out, abandoning in-flight work")
	}
}

func (p *Pool) handle(id string) {
	// Own context: a worker's job must not die because the HTTP request ended.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := p.store.SetStatus(ctx, id, shipment.StatusProcessing); err != nil {
		p.log.Error("classify: set processing", "id", id, "err", err)
		return
	}

	sh, err := p.store.GetShipment(ctx, id)
	if err != nil {
		p.log.Error("classify: load", "id", id, "err", err)
		return
	}

	start := time.Now()
	result := fakeClassify(sh.RawTitle)
	result.ShipmentID = id
	result.LatencyMs = int(time.Since(start).Milliseconds())

	// Confidence router: automate only what the model is sure about.
	next := shipment.StatusNeedsReview
	if result.Confidence >= autoClearThreshold {
		next = shipment.StatusCleared
	}

	if err := p.store.SaveClassification(ctx, result, next); err != nil {
		p.log.Error("classify: save", "id", id, "err", err)
		_ = p.store.SetStatus(ctx, id, shipment.StatusFailed)
		return
	}
	p.log.Info("classified", "id", id, "hsCode", result.ChosenHSCode,
		"confidence", result.Confidence, "status", next)
}

// fakeClassify stands in for the LLM call. Keyword lookup with a sleep so the
// pool has something to wait on. Replace with a real model later.
func fakeClassify(title string) shipment.Classification {
	time.Sleep(800 * time.Millisecond)

	t := strings.ToLower(title)
	var cands []shipment.Candidate
	switch {
	case strings.Contains(t, "t-shirt"), strings.Contains(t, "tee"):
		cands = []shipment.Candidate{{HSCode: "6109", Label: "T-shirts, knitted", Score: 0.92}, {HSCode: "6205", Score: 0.05}}
	case strings.Contains(t, "hoodie"), strings.Contains(t, "sweater"):
		cands = []shipment.Candidate{{HSCode: "6110", Label: "Sweaters, pullovers", Score: 0.81}, {HSCode: "6109", Score: 0.12}}
	case strings.Contains(t, "jumpsuit"):
		cands = []shipment.Candidate{{HSCode: "6211", Score: 0.55}, {HSCode: "6114", Score: 0.30}, {HSCode: "6104", Score: 0.10}}
	default:
		cands = []shipment.Candidate{{HSCode: "9999", Label: "unknown", Score: 0.30}}
	}
	return shipment.Classification{
		Candidates:    cands,
		ChosenHSCode:  cands[0].HSCode,
		Confidence:    cands[0].Score,
		Model:         "fake-v0",
		PromptVersion: "v0",
	}
}
