# StockFlow

A distributed, real-time market data pipeline written in Go. It simulates a stream of stock ticks, click events, and service telemetry, pushes them through NATS JetStream, computes rolling metrics (moving average, volatility, top-k rankings) on the fly, and serves the results over a REST API and WebSocket feed — with Prometheus/Grafana watching the whole thing.

I built this to get hands-on with the kind of distributed systems problems that don't show up in competitive programming: backpressure, at-least-once delivery, batching vs. latency tradeoffs, and what happens when one service can't keep up with another.

## Architecture

```
                    ┌──────────────┐
                    │  Simulator   │  generates stock/click/telemetry events,
                    │ (WebSocket)  │  broadcasts over ws://:8081/ws
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │  Ingestion   │  reads the WS stream, batches events,
                    │              │  publishes to NATS JetStream (async)
                    └──────┬───────┘
                           │  events.raw.>
                           ▼
                    ┌──────────────┐
                    │  Processor   │  worker-pool consumer, runs each event
                    │              │  through the aggregation pipeline
                    │              │  (moving avg / volatility / top-k)
                    └──────┬───────┘
                           │  events.processed
                           ▼
                    ┌──────────────┐
                    │     API      │  subscribes, persists to Redis,
                    │              │  fans out over WebSocket + REST
                    └──────────────┘

     all four services expose /metrics → Prometheus → Grafana
```

Everything talks to everything else through NATS JetStream — there's no service that calls another service's HTTP API directly. That was a deliberate choice: it means ingestion, processor, and API can all be restarted, scaled, or replaced independently, and JetStream's durable consumers handle redelivery if one of them dies mid-batch.

## What each service actually does

- **simulator** — generates synthetic stock ticks (random-walk price model), click events, and telemetry, and streams them over a WebSocket. This is the only thing that "creates" data; everything else consumes it.
- **ingestion** — the WebSocket client. Batches incoming events (100 at a time or every 50ms, whichever comes first) and publishes them into JetStream using async publish + batched ack confirmation, instead of blocking on every single message.
- **processor** — pulls from JetStream in batches, fans work out across a small worker pool, and runs each event through the pipeline: moving average, Welford's-algorithm volatility, and top-k ranking (by volume for stocks, by click count for pages/elements — these used to share one ranking structure, which was a bug, see below). Republishes the result to `events.processed`.
- **api** — subscribes to processed events, persists the latest snapshot per ticker/page/element into Redis, and pushes updates out over WebSocket to connected clients. Also exposes a small REST API (`/api/ticker/:name`, `/api/top/:category`, `/api/telemetry/:key`) for pulling current state.

## Tech stack

- **Go 1.26** for all four services
- **NATS JetStream** for the message backbone (explicit ack, durable consumers, async batched publish)
- **Redis** for latest-state storage (per-ticker snapshots, top-k rankings)
- **Prometheus + Grafana** for metrics — auto-provisioned dashboard, no manual setup needed
- **Docker Compose** to run the whole thing with one command

## Quick start

```bash
git clone https://github.com/xyn3x/stockflow.git
cd stockflow
docker compose up --build
```

That spins up: simulator, ingestion, processor, api, NATS, Redis, Prometheus, Grafana, and the Redis/NATS exporters.

Once it's up:
- Grafana dashboard: [http://localhost:3000/d/stockflow-main/stockflow](http://localhost:3000/d/stockflow-main/stockflow) (no login needed, anonymous admin is enabled for local dev)
- API: `http://localhost:8080`  (Ex.: `curl http://localhost:8080/api/ticker/AAPL`)
- Raw Prometheus: `http://localhost:9090`
- Simulator health check: `http://localhost:8081/health`

Give it 10-20 seconds after startup for the streams/consumers to get created and the first metrics to show up on the dashboard.

## Configuration

In env right now: 

| File | Setting | What it controls |
|---|---|---|
| `simulator.yaml` | `tick_interval` | How often a new event is generated.|
| `ingestion.yaml` | `publisher.batch_size` / `flush_timeout` | Batching for the NATS publish — bigger batches = better throughput, worse latency. |
| `processor.yaml` | `worker.fetch_batch` / `fetch_timeout` | Same tradeoff, on the consume side. |
| `processor.yaml` | `pipeline.moving_avg_window` / `top_k` | Window size for the moving average, and how many entries to keep in top-k rankings. |

## Performance notes (honest version)

Current sustained throughput is **~800 events/sec** end-to-end, with steady-state processing latency around p50 ~2.5ms / p99 ~5ms. Here's what actually got me there, and where the ceiling currently is:

- Started around ~200 events/sec. The biggest win was replacing synchronous per-message `js.Publish` calls with NATS's two-phase async publish (`PublishAsync` + `PublishAsyncComplete`) in both the ingestion publisher and processor worker — that alone was most of the jump to ~750-800/sec.
- The real ceiling right now isn't the simulator's tick rate — it's that event generation and ingestion are a single-threaded chain (one ticker goroutine → one WebSocket connection → one blocking read loop). Turning the tick interval down further doesn't help because (a) Go timers don't reliably resolve below ~1ms anyway, and (b) even if they did, the serial generate→socket→parse path is the actual bottleneck, not the tick rate.
- Scaling further would mean parallelizing ingestion (multiple simulator connections / concurrent readers) rather than tuning the interval — that's on the roadmap below, not done yet.


## Testing

```bash
go test ./... -race
```

Aggregation logic (moving average, volatility, top-k) and the processing pipeline have unit test coverage, including concurrency tests for the shared aggregation structures used by the processor's worker pool. Worth calling out one real bug the tests caught: the click pipeline was originally sharing a single `TopK` instance across stock volume, page views, and click elements, so `top_elements` in the output could get silently polluted by unrelated ticker/page data. Fixed by giving each category its own ranking structure — regression test is in `pipeline_test.go`.

Services that talk directly to NATS/Redis (`worker.go`, `subscriber.go`, the publisher) don't have unit tests yet — they'd need either testcontainers or an interface refactor to test properly without a live broker, and I've prioritized end-to-end observability (the Grafana dashboard catches lag/drops in practice) over mocked integration tests for now.

## Known limitations / what's not done yet

Being upfront about this rather than pretending it's finished:

- No CI pipeline yet (tests are run locally, not on push)
- No health check endpoints on ingestion/processor/API (simulator has one)
- No `golangci-lint` config
- Ingestion and event generation are single-threaded; scaling past ~800/sec needs concurrency there, not tick-interval tuning
