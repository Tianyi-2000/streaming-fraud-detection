"""Benchmark and crash-test harness for the producer half of the pipeline.

Runs against a throwaway Redpanda broker, never the compose one, because the
crash test kills it. Same image and flags as docker-compose.yml:

    docker run -d --name bench-redpanda -p 19092:19092 redpandadata/redpanda:v24.2.7 \
      redpanda start --smp=1 --overprovisioned --node-id=0 \
      --kafka-addr=0.0.0.0:19092 --advertise-kafka-addr=localhost:19092

Mirrors src/replay_to_kafka.py (same contract validation, same key) and takes
its producer config from there, and its topic config from src/create_topics.py,
so by default it measures what the repo ships. Overrides reproduce the old setup:
  --idempotent 0                     producer without idempotence
  TOPIC_CONFIG=write.caching=true    topic acked from memory before fsync

Subcommands:
  validate                      validation-only cost of the contract gate
  shipped  --runs N             time the repo's replay script end to end
  latency  --rate R --linger L  produce->ack and produce->consume latency
  restart  --mode {restart,kill} --idempotent {0,1}
                                broker restart/kill mid-replay, then audit topic
  malformed                     inject invalid events, audit what reached topic

Results and how to read them: bench/BENCHMARK_RESULTS.md
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import statistics
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from contracts.play_event_v1 import PlayEventV1, play_events_key  # noqa: E402
from src.create_topics import TOPIC_CONFIG  # noqa: E402
from src.replay_to_kafka import PRODUCER_CONFIG  # noqa: E402

BROKER = "localhost:19092"
EVENTS = REPO / "data" / "play_events.jsonl"
PY = str(Path(sys.executable))


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return float("nan")
    k = (len(xs) - 1) * p / 100
    f, c = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def new_topic(prefix="bench"):
    name = f"{prefix}-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": BROKER})
    override = os.environ.get("TOPIC_CONFIG")
    cfg = TOPIC_CONFIG if override is None else dict(kv.split("=", 1) for kv in override.split(",") if kv)
    fs = admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1, config=cfg)])
    for f in fs.values():
        f.result(30)
    return name


def audit_topic(topic, expected_ids=None):
    """Read the whole topic and report count, dups, key mismatches, order."""
    c = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": f"audit-{uuid.uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    md = c.list_topics(topic, timeout=10)
    parts = list(md.topics[topic].partitions)
    hw = {}
    for p in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        hw[p] = hi
    total_expected = sum(hw.values())
    c.assign([TopicPartition(topic, p, 0) for p in parts])
    seen = 0
    ids = []
    key_mismatch = 0
    last_time_by_key = {}
    order_violations = 0
    per_part = {p: 0 for p in parts}
    while seen < total_expected:
        msg = c.poll(5)
        if msg is None:
            break
        if msg.error():
            continue
        seen += 1
        per_part[msg.partition()] += 1
        v = json.loads(msg.value())
        ids.append(v["event_id"])
        if msg.key().decode() != v["listener_id"]:
            key_mismatch += 1
        k = v["listener_id"]
        t = v["event_time"]
        if k in last_time_by_key and t < last_time_by_key[k]:
            order_violations += 1
        last_time_by_key[k] = t
    c.close()
    uniq = set(ids)
    out = {
        "records": seen,
        "unique_event_ids": len(uniq),
        "duplicates": seen - len(uniq),
        "key_mismatches": key_mismatch,
        "per_key_order_violations": order_violations,
        "per_partition": per_part,
    }
    if expected_ids is not None:
        out["missing"] = len(expected_ids - uniq)
    return out


def all_ids():
    with EVENTS.open() as f:
        return {json.loads(l)["event_id"] for l in f if l.strip()}


# --------------------------------------------------------------------------
def cmd_validate(_):
    lines = [l.strip() for l in EVENTS.open() if l.strip()]
    t0 = time.perf_counter()
    for l in lines:
        json.loads(l)
    t_json = time.perf_counter() - t0
    runs = []
    for _ in range(5):
        t0 = time.perf_counter()
        for l in lines:
            PlayEventV1.model_validate_json(l)
        runs.append(time.perf_counter() - t0)
    med = statistics.median(runs)
    print(json.dumps({
        "events": len(lines),
        "json_loads_s": round(t_json, 3),
        "pydantic_validate_median_s": round(med, 3),
        "validate_events_per_s": round(len(lines) / med),
        "per_event_us": round(med / len(lines) * 1e6, 2),
    }, indent=2))


def cmd_shipped(args):
    """Run the repo's own replay script, N times, fresh topic each."""
    results = []
    ids = all_ids()
    for i in range(args.runs):
        topic = new_topic("shipped")
        t0 = time.perf_counter()
        p = subprocess.run(
            [PY, str(REPO / "src" / "replay_to_kafka.py"), "--broker", BROKER, "--topic", topic],
            capture_output=True, text=True, cwd=REPO,
        )
        wall = time.perf_counter() - t0
        last = [l for l in p.stdout.splitlines() if l.startswith("Done.")]
        a = audit_topic(topic, ids)
        results.append({"run": i + 1, "wall_s": round(wall, 3), "rc": p.returncode,
                        "eps_wall": round(45473 / wall), "script": last[0] if last else p.stderr[-300:],
                        **{k: a[k] for k in ("records", "duplicates", "missing", "key_mismatches", "per_key_order_violations")}})
        print(json.dumps(results[-1]))
    walls = [r["wall_s"] for r in results]
    print(json.dumps({"median_wall_s": statistics.median(walls),
                      "median_eps_incl_startup": round(45473 / statistics.median(walls))}))


def _consumer_proc(topic, ready, out_path, expected, idle_s):
    c = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": f"lat-{uuid.uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    md = c.list_topics(topic, timeout=10)
    parts = list(md.topics[topic].partitions)
    c.assign([TopicPartition(topic, p, 0) for p in parts])
    c.poll(0.5)
    ready.set()
    lats = []
    last = time.time()
    while len(lats) < expected and time.time() - last < idle_s:
        msg = c.poll(0.2)
        if msg is None or msg.error():
            continue
        now_ms = time.time() * 1000
        _, ts = msg.timestamp()
        lats.append(now_ms - ts)
        last = time.time()
    c.close()
    Path(out_path).write_text(json.dumps(lats))


def instrumented_replay(topic, rate=0.0, linger=20, idempotent=True, lines=None,
                        on_tick=None, flush_timeout=120):
    conf = {**PRODUCER_CONFIG, "bootstrap.servers": BROKER, "linger.ms": linger,
            "enable.idempotence": idempotent, "client.id": "play-events-replay-bench"}
    p = Producer(conf)
    sent = rejected = 0
    fails = {"n": 0, "errs": {}}
    acked = {"n": 0}
    ack_lat = []

    def cb_factory(t_prod):
        def cb(err, _msg):
            if err is not None:
                fails["n"] += 1
                fails["errs"][str(err.name())] = fails["errs"].get(str(err.name()), 0) + 1
            else:
                acked["n"] += 1
                ack_lat.append((time.perf_counter() - t_prod) * 1000)
        return cb

    lines = lines if lines is not None else [l.strip() for l in EVENTS.open() if l.strip()]
    t0 = time.perf_counter()
    interval = 1.0 / rate if rate else 0.0
    for i, line in enumerate(lines):
        if rate:
            target = t0 + i * interval
            while True:
                now = time.perf_counter()
                if now >= target:
                    break
                p.poll(min(target - now, 0.005))
        try:
            ev = PlayEventV1.model_validate_json(line)
        except Exception:
            rejected += 1
            continue
        while True:
            try:
                p.produce(topic=topic, key=play_events_key(ev), value=line.encode(),
                          on_delivery=cb_factory(time.perf_counter()))
                break
            except BufferError:
                p.poll(0.05)
        sent += 1
        p.poll(0)
        if on_tick:
            on_tick(time.perf_counter() - t0)
    t_enq = time.perf_counter() - t0
    remaining = p.flush(flush_timeout)
    t_all = time.perf_counter() - t0
    return {"sent": sent, "rejected": rejected, "acked": acked["n"], "delivery_failures": fails["n"],
            "failure_kinds": fails["errs"], "unflushed": remaining,
            "enqueue_s": round(t_enq, 3), "total_s": round(t_all, 3),
            "eps_total": round(sent / t_all), "ack_lat_ms": ack_lat}


def cmd_latency(args):
    topic = new_topic("lat")
    ready = mp.Event()
    out = Path(f"/tmp/lat-{uuid.uuid4().hex}.json")
    out = Path(__file__).parent / out.name
    cp = mp.Process(target=_consumer_proc, args=(topic, ready, str(out), 45473, 8))
    cp.start()
    ready.wait(30)
    time.sleep(1)
    r = instrumented_replay(topic, rate=args.rate, linger=args.linger, idempotent=bool(args.idempotent))
    cp.join(120)
    e2e = json.loads(out.read_text())
    out.unlink()
    ack = r.pop("ack_lat_ms")
    print(json.dumps({
        "rate_target": args.rate or "max", "linger_ms": args.linger, "idempotent": bool(args.idempotent),
        **{k: r[k] for k in ("sent", "acked", "delivery_failures", "total_s", "eps_total")},
        "ack_p50_ms": round(pct(ack, 50), 1), "ack_p95_ms": round(pct(ack, 95), 1), "ack_p99_ms": round(pct(ack, 99), 1),
        "e2e_n": len(e2e),
        "e2e_p50_ms": round(pct(e2e, 50), 1), "e2e_p95_ms": round(pct(e2e, 95), 1),
        "e2e_p99_ms": round(pct(e2e, 99), 1), "e2e_max_ms": round(max(e2e), 1),
    }))


def cmd_restart(args):
    topic = new_topic("fault")
    ids = all_ids()
    fired = {"done": False, "at": None, "dur": None}

    def tick(elapsed):
        if not fired["done"] and elapsed >= args.at:
            fired["done"] = True
            fired["at"] = round(elapsed, 2)

            def go():
                t = time.perf_counter()
                if args.mode == "restart":
                    subprocess.run(["docker", "restart", "bench-redpanda"], capture_output=True)
                else:
                    subprocess.run(["docker", "kill", "bench-redpanda"], capture_output=True)
                    subprocess.run(["docker", "start", "bench-redpanda"], capture_output=True)
                fired["dur"] = round(time.perf_counter() - t, 2)
            threading.Thread(target=go, daemon=True).start()

    try:
        r = instrumented_replay(topic, rate=args.rate, idempotent=bool(args.idempotent), on_tick=tick,
                                flush_timeout=args.flush)
        r.pop("ack_lat_ms")
        r["fatal"] = None
    except (KafkaException, SystemError) as exc:
        # An idempotent producer that finds a gap in its sequence numbers goes
        # fatal and stops. That loud stop is the result, so record it and still
        # audit what reached the topic. A delivery callback that runs after the
        # error was raised surfaces as SystemError, with the KafkaException as
        # its context.
        kexc = exc if isinstance(exc, KafkaException) else exc.__context__
        name = kexc.args[0].name() if isinstance(kexc, KafkaException) else repr(exc)
        r = {"sent": None, "acked": None, "delivery_failures": None, "failure_kinds": None,
             "unflushed": None, "total_s": None, "fatal": name}
    # make sure broker is healthy before auditing
    for _ in range(60):
        h = subprocess.run(["docker", "exec", "bench-redpanda", "rpk", "cluster", "health",
                            "-X", f"brokers={BROKER}"], capture_output=True, text=True).stdout
        if "Healthy:" in h and "true" in h.split("Healthy:")[1].split("\n")[0]:
            break
        time.sleep(1)
    a = audit_topic(topic, ids)
    print(json.dumps({"mode": args.mode, "idempotent": bool(args.idempotent), "rate": args.rate,
                      "fault_at_s": fired["at"], "broker_down_s": fired["dur"],
                      **{k: r[k] for k in ("sent", "acked", "delivery_failures", "failure_kinds", "fatal", "unflushed", "total_s")},
                      **{k: a[k] for k in ("records", "duplicates", "missing", "key_mismatches", "per_key_order_violations")}}))


def cmd_malformed(args):
    import random
    rng = random.Random(42)
    valid = [l.strip() for l in EVENTS.open() if l.strip()]
    cases = [json.loads(l) for l in (REPO / "tests" / "fixtures" / "invalid_events.jsonl").open() if l.strip()]
    model_cases = [c for c in cases if c["expect"] == "model_reject"]
    bad = []
    for c in model_cases:
        for _ in range(args.copies):
            bad.append(json.dumps(c["record"]))
    mixed = valid[:]
    for b in bad:
        mixed.insert(rng.randrange(len(mixed) + 1), b)
    path = Path(__file__).parent / "mixed_events.jsonl"
    path.write_text("\n".join(mixed) + "\n")
    topic = new_topic("malformed")
    p = subprocess.run([PY, str(REPO / "src" / "replay_to_kafka.py"), "--broker", BROKER,
                        "--topic", topic, "--input", str(path)], capture_output=True, text=True, cwd=REPO)
    a = audit_topic(topic, set(json.loads(v)["event_id"] for v in valid))
    done = [l for l in p.stdout.splitlines() if l.startswith("Done.")]
    print(json.dumps({"model_reject_case_types": len(model_cases), "case_names": [c["case"] for c in model_cases],
                      "injected_bad": len(bad), "valid": len(valid), "script": done[0] if done else p.stderr[-400:],
                      **{k: a[k] for k in ("records", "duplicates", "missing", "key_mismatches")}}))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("validate")
    s = sub.add_parser("shipped"); s.add_argument("--runs", type=int, default=5)
    s = sub.add_parser("latency"); s.add_argument("--rate", type=float, default=0.0)
    s.add_argument("--linger", type=int, default=20); s.add_argument("--idempotent", type=int, default=1)
    s = sub.add_parser("restart"); s.add_argument("--mode", choices=["restart", "kill"], default="restart")
    s.add_argument("--idempotent", type=int, default=1); s.add_argument("--rate", type=float, default=2000)
    s.add_argument("--at", type=float, default=6.0); s.add_argument("--flush", type=float, default=120)
    s = sub.add_parser("malformed"); s.add_argument("--copies", type=int, default=100)
    a = ap.parse_args()
    {"validate": cmd_validate, "shipped": cmd_shipped, "latency": cmd_latency,
     "restart": cmd_restart, "malformed": cmd_malformed}[a.cmd](a)
