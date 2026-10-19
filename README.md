# customs-api-go

Go port of the ingest half of [swip-customs-pipeline](https://github.com/dinhcongdat866/swip-customs-pipeline):
accept shipments over HTTP, store them atomically, classify them in the background with a
worker pool, and route each result by confidence (auto-clear vs human review).

Built to learn Go properly: `net/http` + `pgx`, no framework.

## Run

```bash
cp .env.example .env      # set DATABASE_URL (Postgres; Neon works)
go run ./cmd/api
```

```bash
curl localhost:8080/health

curl -X POST localhost:8080/shipments \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"customerId":"acme","items":[
        {"rawTitle":"Cotton t-shirt XL","declaredValue":12.50,"currency":"USD","originCountry":"VN","destinationCountry":"US"},
        {"rawTitle":"Kids jumpsuit","declaredValue":30,"destinationCountry":"US"}]}'

curl localhost:8080/shipments/<id>
```

Send the same `Idempotency-Key` again: you get the original batch back with `200` and `"replayed": true`
instead of a duplicate.

## Layout

```
cmd/api            main: wiring, startup/shutdown order
internal/config    env + .env loading
internal/db        pgx pool, embedded SQL migrations (schema `goapi`)
internal/shipment  domain types, Repo interface + Postgres impl, Service (validate, transaction, idempotency)
internal/classify  worker pool (goroutines + channel), confidence router, fake classifier
internal/httpapi   handlers, error → status mapping, logging middleware, httptest tests
```

## Design notes

- **One transaction per batch.** Batch + all shipments commit together or not at all.
- **Idempotency-Key** on `POST /shipments` makes client retries safe; a race on the same key
  is caught via the unique constraint (`23505`) and replayed.
- **Known crash window:** process dying between commit and enqueue leaves rows with
  `status='received'`. `ListPendingIDs` at startup re-queues them (reconciliation sweep).
  A transactional outbox would close it fully; not needed at this size.
- **Worker pool with back-pressure.** `Enqueue` blocks when the buffer is full rather than
  dropping work. Workers use their own context so a finished HTTP request cannot cancel a
  half-written classification.
- **Confidence router.** `>= 0.85` auto-clears, below goes to `needs_review`, same thresholds
  as the original pipeline.
- **Money is never a float.** `declaredValue` is decoded as `json.Number`, validated with
  `big.Rat`, stored as `numeric(12,2)`, returned as a string.
- **Graceful shutdown.** SIGTERM → stop accepting → finish in-flight requests → drain workers → close DB.

## Test

```bash
go test ./...
```
