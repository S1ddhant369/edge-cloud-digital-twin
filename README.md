# Edge-to-Cloud Digital Twin Synchronization

A local, Dockerized distributed-systems project demonstrating offline edge operation,
a bounded SQLite event buffer, Kafka-based cloud ingestion, deterministic conflict
resolution, and short/medium/long outage experiments.

## Architecture

- **Simulated sensor:** publishes readings over MQTT.
- **Edge:** Mosquitto MQTT subscription + durable SQLite buffer. The edge keeps accepting
  readings while the simulated edge-to-cloud link is offline.
- **Cloud transport:** Apache Kafka topic `twin-events`.
- **Cloud:** consumes events and stores the current digital-twin state in SQLite.
- **Control/metrics APIs:** edge `http://localhost:8080`; cloud `http://localhost:8081`.

The `/network` control endpoint toggles the *edge-to-cloud forwarding link* at the
application layer. This is an intentional, repeatable network-outage simulation that
does not require privileged `tc` commands. MQTT sensor-to-edge traffic remains available,
as required for independent local operation.

## Requirements

Install Docker Desktop and ensure Docker Compose is available. On Windows, use PowerShell
or a terminal in VS Code. Allow several minutes for the first image pulls.

## Start

```bash
docker compose up --build -d
docker compose --profile sensor up -d sensor
docker compose ps
```

Check:
- Edge: `http://localhost:8080/health`
- Edge metrics: `http://localhost:8080/metrics`
- Cloud state: `http://localhost:8081/state`
- Cloud metrics: `http://localhost:8081/metrics`

Publish a reading manually from PowerShell:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/publish `
  -ContentType "application/json" `
  -Body '{"twin_id":"machine-01","source":"edge","value":27.4}'
```

## Trigger an outage

Disconnect edge-to-cloud forwarding:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/network `
  -ContentType "application/json" -Body '{"online":false}'
```

Publish readings as normal; they are persisted in SQLite. Reconnect:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/network `
  -ContentType "application/json" -Body '{"online":true}'
```

Watch pending rows drain using `http://localhost:8080/metrics` and cloud state using
`http://localhost:8081/state`.

## Automated experiments

With the containers running, install no extra dependency (the test uses Python standard
library) and run:

```bash
python tests/run_outage_experiment.py
```

It runs short and medium outages and a long burst that exceeds the buffer capacity.
Save the terminal output as experimental evidence. The script reports pending queue size,
buffer evictions, reconciliation latency, and whether the cloud state reappears.

## Buffer policy (bounded and documented)

`BUFFER_MAX_ROWS` defaults to 500 unsynchronized events. When a new event arrives and
the unsynchronized queue exceeds this capacity, the edge evicts the oldest unsynchronized
event first (FIFO eviction). The `evicted` metric counts these removals. Already-synchronized
rows remain in SQLite for auditability; for a production system, add a retention policy
for those rows as well.

## Conflict resolution policy

The cloud stores one current state per `twin_id`. For competing updates:
1. The event with the greatest `event_ts` wins (last-write-wins).
2. If timestamps are equal, `source="cloud"` wins over `source="edge"`.
3. If timestamp and source priority tie, lexicographically greater `event_id` wins.

This deterministic ordering makes the result independent of message arrival order. It assumes
clocks are reasonably synchronized; a production version should use a trusted logical version
or hybrid logical clock. Events are idempotent by `event_id`, so Kafka redelivery does not
apply the same event twice.

## Resource constraints

The edge and cloud containers each have a 256 MB memory limit in Compose. This simulates
constrained compute but is not a precise hardware benchmark; record Docker Desktop resource
settings and machine specifications in the report.

## Metrics and interpretation

- **Reconciliation latency:** time from reconnect request until the edge pending queue drains.
- **Buffer loss:** count of `buffer_evictions` during a trial. Compare injected events to
  successfully delivered unique event IDs for a rigorous per-trial loss calculation.
- **Cloud convergence:** confirm the trial's `twin_id` appears in `/state` and its final
  value/event timestamp match the winning event.

The included experiment is a starter harness; inspect each run's output and repeat trials.
Do not claim zero loss unless the counters and event IDs prove it.

## Useful commands

```bash
docker compose logs -f edge
docker compose logs -f cloud
docker compose logs -f kafka
docker compose down
# Removes containers and persistent data (irreversible for local project data):
docker compose down -v
```

## Known scope / next improvements

This is a locally simulated cloud, not a deployment to AWS/Azure/GCP. The outage toggle
controls application-level forwarding rather than physically severing a TCP route. For a
stronger network-layer experiment, add Toxiproxy or Linux `tc netem` and measure packet
loss/latency independently. Further improvements: expose a Prometheus metrics endpoint,
persist trial IDs for exact loss accounting, add cloud-origin update injection, and add
integration tests for equal-timestamp conflicts.
