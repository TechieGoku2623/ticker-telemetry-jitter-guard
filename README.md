# Ticker Telemetry Jitter Guard

A high-throughput, low-latency asynchronous engine engineered to resolve inter-arrival jitter, inverted NBBO quotes, and sub-450-microsecond arrival bursts on struct-packed venue ticks before they are admitted to the per-venue ring.

## 🏗️ Systems Architecture & Event Topology

`TickerTelemetryJitterGuard` is the only admission point. Each venue keeps its own `deque` ring so one noisy book cannot overwrite another venue's cache. Frames enter through `admit` or as a batch through `run_worker`. The in-process queue is the stand-in for the Kafka topic `md.ticks.nbbo`. A downstream consumer would publish that topic; this process does not open a broker socket.

The wire record is packed with `struct`. `pack_frame` and `unpack_frame` are the only serialization path. Shared ring state is taken under an `asyncio.Lock`. `logging.basicConfig` stamps every line with a timezone-aware timestamp. A real fault — inverted market, non-finite price, or a backward clock step past the clamp — raises `EngineKernelException` and is counted in the batch rejection list rather than written into the ring.

Clock alignment follows the SEC Rule 613 consolidated-audit-trail expectation that venue timestamps stay ordered against a synchronized clock. The guard clamps a small backward step and rejects a skew that exceeds the configured bound. It does not discipline an exchange clock and it does not submit orders.

## 📊 Core Visual Walkthrough & Engine Pipeline Flow

```
venue adapter
    |  struct frame (venue, symbol hash, bid, ask, size, epoch_ns)
    v
asyncio.Queue  ---- topic name md.ticks.nbbo
    |
    v
admit --> finite-price gate --> monotonic clock gate --> inverted-market gate
    |                              |
    |                         EngineKernelException
    v
burst detector (gap < 450 µs, count >= burst minimum)
    |
    v
per-venue deque ring
    |
    v
batch report: accepted, rejected, mean and stdev inter-arrival
```

Insert the structural terminal walkthrough recording at docs/assets/terminal-walkthrough.gif before publishing the release notes.

## ⚡ Low-Level OS Mechanics & Network Physics

Inter-arrival time is the difference of `epoch_ns` fields, not a sleep. A burst is a run of arrivals whose gap is under 450 microseconds (`450_000` ns). The detector records the anchor timestamp and the gap; it does not drop the book, and it does not spin. The ring is a fixed `deque` with a configured capacity, so admission stays O(1) and does not allocate a new buffer per tick.

Symbol identity on the ring is a stable integer hash, not a retained string table that grows with the session. `statistics` supplies the mean and the sample standard deviation of the inter-arrival series once a batch has been admitted. `math` rejects NaN and infinity before a price can enter the mean. The event loop is single-threaded; the lock exists so a snapshot coroutine and the worker cannot tear a ring slot.

## ⚖️ Architecture Trade-offs & Pragmatic Decisions

A Redis hash would be the multi-process cache. Inside one process the deque is the cache, which removes a serialization hop from the hot path. The cost is that the ring dies with the process. That is acceptable for a guard whose job is to refuse bad ticks, not to be the system of record.

Burst detection uses a fixed gap and a fixed count. An adaptive threshold would track a moving venue, and it would also move under a quote flood. The constant 450-microsecond gate is the contract the harness asserts. Rejecting the whole burst would hide a real print; the guard flags `burst_clamped` and keeps the frames that passed the price and clock gates.

## 🚀 Local Installation & Benchmarking

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python src/main.py
python src/test_harness.py
```

```python
import asyncio

from src.main import TickerTelemetryJitterGuard


async def demo() -> None:
    guard = TickerTelemetryJitterGuard()
    payload = guard.pack_frame(
        "XNYS", "ACME", 189.20, 189.24, 100, 1_700_000_000_000_000_000
    )
    await guard.admit(payload)


asyncio.run(demo())
```

Runtime dependencies are the Python 3.12 standard library. `requirements.txt` is comments only, so `pip install -r requirements.txt` succeeds without fetching a package.

## 🖥️ Terminal Diagnostic Output Preview

```
WARNING [ticker.jitter] clock skew clamped venue=XNAS raw_ns=1700000000020050000 held_ns=1700000000020900001 backward_ns=850000
WARNING [ticker.jitter] frame rejected error=inverted market venue=ARCX bid=189.4 ask=189.3
WARNING [ticker.jitter] frame rejected error=NaN price venue=ARCX bid=nan ask=189.5
WARNING [ticker.jitter] burst clamped ticks=11 gap_ns=450000 anchor_ns=1700000000020000000
INFO [ticker.jitter] batch topic=md.ticks.nbbo accepted=18 rejected=2 burst=True mean_interarrival_us=2941.176 ring_depth=18
```

`python src/main.py` exits 0 after that batch. The stdout JSON names the topic `md.ticks.nbbo` and lists the two rejections.

## 📊 Empirical Benchmarking Performance Report

Numbers below are the status dict printed by `python src/test_harness.py` on this tree (seeded `random.Random`, 8000 measured iterations, `time.perf_counter_ns` converted to microseconds, `tracemalloc` peak).

| Metric | Measured |
| --- | ---: |
| Status | PASS |
| Iterations | 8000 |
| Average latency | 7.364 µs |
| Empirical P99 | 9.669 µs |
| Scenario latency | 113.222 µs |
| tracemalloc peak | 997701 bytes |

## 🛡️ Edge-Case Resilience & SOC2/Regulatory Compliance

Non-monotonic timestamps are clamped when the backward step is inside `max_backward_ns` and rejected with `EngineKernelException` when the skew exceeds that bound. NaN and infinite prices are rejected before they can poison the inter-arrival statistics or the ring. An inverted market (`bid > ask`) is rejected with the venue and both prices in the fault text.

The guard does not store customer identifiers. The symbol is reduced to a hash on the ring record. Processing integrity is the relevant SOC 2 point: a rejected frame is logged as a fault and is absent from `ring_snapshot`. SEC Rule 613 is the clock-ordering reference for the monotonic gate. This module does not file a CAT report and does not claim to be a clock source.
