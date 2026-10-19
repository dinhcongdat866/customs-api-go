// Command api runs the customs ingest + classification service.
//
// Startup order: config → database → migrations → worker pool → HTTP server.
// Shutdown order is the reverse: stop taking requests, drain the workers,
// close the database.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dinhcongdat866/customs-api-go/internal/classify"
	"github.com/dinhcongdat866/customs-api-go/internal/config"
	"github.com/dinhcongdat866/customs-api-go/internal/db"
	"github.com/dinhcongdat866/customs-api-go/internal/httpapi"
	"github.com/dinhcongdat866/customs-api-go/internal/shipment"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// ctx is cancelled on Ctrl+C / SIGTERM: the one signal everything listens to.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}
	log.Info("database ready")

	repo := shipment.NewPGRepo(pool)
	workers := classify.NewPool(repo, cfg.Workers, 1000, log)
	workers.Start(ctx)

	// Reconciliation sweep: anything stored but not finished before the last
	// shutdown/crash gets queued again.
	pending, err := repo.ListPendingIDs(ctx)
	if err != nil {
		return err
	}
	for _, id := range pending {
		workers.Enqueue(id)
	}
	if len(pending) > 0 {
		log.Info("re-queued pending shipments", "count", len(pending))
	}

	svc := shipment.NewService(repo, workers)
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      httpapi.New(svc, pool, log),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Serve in its own goroutine so main can wait for the shutdown signal.
	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", srv.Addr, "workers", cfg.Workers)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down")
	}

	// Stop accepting new requests, let in-flight ones finish (10s budget).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}

	// Then drain the workers so no shipment is left half-classified.
	workers.Stop(15 * time.Second)
	log.Info("bye")
	return nil
}
