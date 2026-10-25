# Producer benchmarks and broker crash tests

The replay producer used `acks=all`, and the project treated that as durable. Killing the broker
in the middle of a replay showed it was not: acknowledged events disappeared, and the producer
reported zero failures. This file records how that was measured, the two-setting fix, what the fix
costs, and how to reproduce every number. The harness is [`bench.py`](bench.py); every run's raw
output is in [`raw/`](raw/).

Measurements were taken on 2026-10-02, when the problem was found, and repeated on 2026-10-24
against the commit that ships the fix.

## Summary

- **Before:** across 9 runs, a broker kill at 2,000 events/s lost between 15 and 133 acknowledged events per run, while the producer reported 45,473 acked and 0 delivery failures each time.
- **Cause:** the Redpanda image starts in developer mode (`write_caching_default: "true"`). An `acks=all` write is acknowledged from memory, before fsync. A crash drops whatever had not reached disk yet.
- **Fix:**
  - `write.caching=false` on both topics, so an ack means the batch is on disk (`src/create_topics.py`).
  - `enable.idempotence=true` on the producer, so a retried batch cannot be duplicated or reordered (`src/replay_to_kafka.py`).
- **After:** 11 of 11 crash runs had 0 lost, 0 duplicated and 0 per-key order violations.
- **Cost:**
  - Turning write caching off cost no measurable throughput.
  - Idempotence does not slow streaming. It adds a one-time wait of about 0.5 s at producer startup, which takes the full replay from 0.38 s to 0.84 s.

## Environment

- Apple M1 Pro (10 cores, 16 GB); Docker Desktop VM with 5 CPUs and 7.7 GB
- Redpanda v24.2.7, single node, `--smp=1 --overprovisioned`, the same image and flags as `docker-compose.yml`
- confluent-kafka 2.15.0 (librdkafka 2.15.0), pydantic 2.11.7, Python 3.13
- Topics: 3 partitions, replication factor 1
- Data: `data/play_events.jsonl`, 45,473 events

The bench broker listens on `localhost:19092` and is never the compose broker, because the crash test kills it:

```bash
docker run -d --name bench-redpanda -p 19092:19092 redpandadata/redpanda:v24.2.7 \
  redpanda start --smp=1 --overprovisioned --node-id=0 \
  --kafka-addr=0.0.0.0:19092 --advertise-kafka-addr=localhost:19092
```

By default `bench.py` uses the repo's own producer config (`PRODUCER_CONFIG`) and topic config
(`TOPIC_CONFIG`), so it measures what ships. Two overrides reproduce the old setup:

- `--idempotent 0` turns producer idempotence off.
- `TOPIC_CONFIG=write.caching=true` creates the topic with write caching on.

## 1. Crash test

### Method

1. Replay all 45,473 events at 2,000 events/s, which takes about 22.7 s.
2. At t = 6 s, `docker kill` the broker (SIGKILL), then `docker start` it again. The broker was down for 0.5 to 1.2 s.
3. When the replay finishes, read the whole topic back and audit it against the 45,473 event ids: missing, duplicated, key mismatches, and per-key event-time order.

### Results

| Case | Topic write caching | Idempotence | Runs | Lost | Duplicated | What happened |
|---|---|---|---|---|---|---|
| A. old setup | on | off | 9 | 49, 91, 84, 102, 52, 86 (10-02)<br>133, 15, 68 (10-24) | 0 | The producer reported 45,473 acked and 0 delivery failures in every run. **Silent loss.** |
| B. old setup, graceful `docker restart` | on | off | 3 | 0 | 0 | A graceful shutdown flushes to disk. Only a crash exposes the gap. |
| C. idempotence only | on | on | 7 | n/a | n/a | The producer hit a fatal `OUT_OF_ORDER_SEQUENCE_NUMBER` and stopped after writing about 12,000 records. **The loss becomes loud.** |
| D. write caching off only | off | off | 9 | 0 in 9 of 9 | 11 in 1 of 9 | That run also had 1 per-key order violation: a batch was retried after its ack was lost. |
| E. both (shipped) | off | on | 11 | 0 | 0 | 0 per-key order violations in 11 of 11. |

How the runs split by date:

- 10-02: A 6, B 3, C 5, D 9, E 8.
- 10-24: A 3, C 2, E 3.

In 3 of the 10-02 D runs and 3 of the 10-02 E runs, write caching was turned off for the whole cluster (`rpk cluster config set write_caching_default false`) instead of per topic. The results were the same.

Commands:

```bash
TOPIC_CONFIG=write.caching=true python bench/bench.py restart --mode kill --idempotent 0     # A
TOPIC_CONFIG=write.caching=true python bench/bench.py restart --mode restart --idempotent 0  # B
TOPIC_CONFIG=write.caching=true python bench/bench.py restart --mode kill --idempotent 1     # C
python bench/bench.py restart --mode kill --idempotent 0                                     # D
python bench/bench.py restart --mode kill                                                    # E
```

### Why both settings are needed

- **Write caching off** makes an acknowledgement mean "on disk". With caching on, `acks=all` on a single node only means "in memory".
- **Idempotence** gives each batch a producer id and a sequence number:
  - The broker drops a duplicate retry, which removes the duplicates and the reordering seen in case D.
  - If acknowledged data does go missing, the producer sees a gap in the sequence and stops with a fatal error instead of continuing silently (case C).
- **Neither alone is enough.**
  - Idempotence alone turns silent loss into a stopped pipeline. That is better, but the data is still gone.
  - Write caching off alone still allows duplicates after a retry.

## 2. Throughput

### Write caching (10-02)

The repo's replay script, before idempotence was added, run end to end: Python startup, validation, send and flush. 5 runs per row, fresh topic per run, every run audited.

| Topic config | Median wall time | Events/s |
|---|---|---|
| default (write caching on) | 0.438 s | ~103.8K |
| `write.caching=false` (topic) | 0.422 s | ~107.8K |
| write caching off (cluster) | 0.444 s | ~102.4K |

All 15 runs: 45,473 records, 0 missing, 0 duplicated, 0 key mismatches, 0 per-key order violations.
Waiting for fsync before the ack costs no measurable bulk throughput here.

### Idempotence (10-24)

The script before and after this change, same broker, 5 runs each:

| Script | Topic write caching | Median wall time | Events/s |
|---|---|---|---|
| before (no idempotence) | on | 0.380 s | ~120K |
| before (no idempotence) | off | 0.375 s | ~121K |
| after (idempotence) | off | 0.844 s | ~54K |
| after (idempotence) | on | 0.844 s | ~54K |

Where the extra time goes:

- **The ~0.5 s is a fixed startup cost, not lost throughput.** With idempotence, even a single message took 0.52 s to be acknowledged, against 0.009 s without. Once the first ack arrives, the full stream drains in the same time with or without idempotence: 0.13 s against 0.12 s.
- **The wait is the producer-id handshake.** librdkafka's debug log (`debug=eos`) shows it. When the producer starts, no broker connection is up yet, so the producer-id state machine arms a retry timer. The timer fires about 500 ms later, and the id is then granted in 8 ms.
- **The in-flight cap is not the cause.** Idempotence caps in-flight requests at 5 per connection, but setting that cap without idempotence stayed at 0.15 s.

For a long-running producer the wait is paid once. For this one-shot bulk replay, it doubles the wall time.

## 3. Latency (10-02, write caching off and idempotence on; 3 runs each, medians)

End to end means the bench consumer's receive time minus the producer's CreateTime. The consumer runs in a separate process on the same host clock.

| Rate | linger.ms | e2e p50 | e2e p95 | e2e p99 |
|---|---|---|---|---|
| burst (~165K events/s, excluding startup) | 20 | 34.0 ms | 51.9 ms | 59.5 ms |
| steady 5K events/s | 20 | 13.0 ms | 22.5 ms | 25.1 ms |
| steady 5K events/s | 5 | 4.2 ms | 6.8 ms | 7.4 ms |

At a steady rate, cutting linger from 20 to 5 ms cuts p95 by 3.3x (22.5 to 6.8 ms). A 10-24 re-run of the 5K events/s, linger 5 row gave p95 6.9 ms.

At burst rate, single exploratory runs (write caching on, idempotence off) found that a larger linger is better on both throughput and latency:

| linger.ms | Events/s | p95 |
|---|---|---|
| 0 | 154.7K | 136.3 ms |
| 5 | 161.5K | 108.6 ms |
| 20 | 168.4K | 57.6 ms |

At 1K events/s, linger 20 gave p95 23.5 ms and linger 5 gave 7.6 ms.

Takeaway: keep 20 ms for bulk replay and backfill, and use about 5 ms for a live trickle.

## 4. Contract gate cost

| Measure | Result |
|---|---|
| `PlayEventV1.model_validate_json` on 45,473 lines (median of 5) | 0.121 s, 2.65 µs/event, ~377K events/s (10-02); 0.108 s (10-24) |

## 5. Malformed-event injection

The 8 model-rejectable cases from `tests/fixtures/invalid_events.jsonl`, 100 copies each, were interleaved at random (seed 42) into the 45,473 valid events and replayed with the repo script. Same result on 10-02 and 10-24:

- Script output: `Queued 45473 events ..., rejected 800, delivery failures 0`
- Topic audit: 45,473 records, 0 missing, 0 duplicated, 0 key mismatches
- The 9th fixture case, a Kafka key that differs from `listener_id`, cannot be produced, because the producer derives the key from the validated payload. Consumer 1 still asserts it.

## Raw files

| File | Contents |
|---|---|
| `raw/results_fault.jsonl` | 10-02: case A (3 of the 6 runs; the first 3 were recorded only in notes) and case B |
| `raw/results_fault_fixed.jsonl` | 10-02: write caching off for the whole cluster; cases D and E, 3 runs each |
| `raw/results_fault_topicfix.jsonl` | 10-02: write caching off per topic; case D, 3 runs, including the duplicate run |
| `raw/results_final.jsonl` | 10-02: write caching off per topic; case D (3 runs) and case E (5 runs) |
| `raw/results_latency_final.jsonl` | 10-02: the latency table, 3 runs per row |
| `raw/results_verify_2026-10-24.jsonl` | 10-24, against this commit: cases A (3), C (2) and E (3) |

The 10-02 case C runs are not in `raw/`. The earlier harness crashed on the producer's fatal error instead of logging it; `cmd_restart` now records the error and still audits the topic.

## Caveats

- This is a single node with replication factor 1, on a laptop, with synthetic data. These are not production SLAs.
- In production, a 3-replica topic would usually survive one broker crash even with write caching on, because the acknowledged data lives on the other replicas. The general lesson holds: a delivery guarantee is a property of producer, broker and replication together, and you verify it by injecting failure.
- The end-to-end latency stops at a bench consumer. It does not include the fraud consumers' processing.
- The stage-1 forwarder (`src/consumer_stage1.py`) and the graph emitter (`src/graph_emitter.py`) have their own producers, which still use `acks=all` without idempotence. The same change applies to them, but they have not been crash-tested.
