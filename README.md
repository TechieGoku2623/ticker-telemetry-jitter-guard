# Ticker Telemetry Jitter Guard

A high-throughput, low-latency asynchronous engine engineered to resolve consolidated NBBO inter-arrival jitter by clamping sub-five-second venue clock steps, rejecting non-finite or inverted quotes, and scoring burst structure with a two-sample Allan deviation.

## 🏗️ Systems Architecture & Event Topology

`TickerTelemetryJitterGuard` is the admission point. Each venue keeps its own gap ring, a `deque` of inter-arrival nanoseconds, so one noisy book cannot move another venue's clock. `run` packs every record with `struct`, puts the bytes on an `asyncio.Queue`, and reads them back before `ingest`. That queue is the in-process stand-in for the Kafka topic `md.ticks.nbbo`. This process does not open a broker socket.

A Redis hash would be the multi-process ring. It is not used. Kinesis and TimescaleDB are not on this path: the guard returns a dict and does not write a time-series row. Shared ring state is taken under an `asyncio.Lock`. `logging.basicConfig` lives in `__main__.py` and stamps every line with a timezone-aware timestamp.

The wire layout is little-endian: venue `4s`, FNV-1a symbol `uint32`, bid `float64`, ask `float64`, size `uint32`, epoch `uint64`, sequence `uint32`. A locked market (`ask == bid`) is legal. An inverted market (`ask < bid`) is not.

```
venue adapter
    |  struct frame (venue, FNV-1a, bid, ask, size, epoch_ns, sequence)
    v
asyncio.Queue  ---- production name: Kafka topic md.ticks.nbbo
    |              this process: in-memory queue, no broker
    v
per-venue monotonic clock
    |-- backward step <= 5s --> clamp to last_ns + 1
    |-- backward step >  5s --> EngineKernelException
    |-- NaN, inf, ask < bid --> rejected, ring unchanged
    v
per-venue gap ring (deque)
    |
    v
Allan deviation + burst flag (8 gaps, each < 450_000 ns)
```

Clock order follows the SEC Rule 613 expectation that consolidated audit timestamps stay ordered against a synchronized clock. The guard clamps or rejects. It does not discipline an exchange clock and it does not submit orders.

## 📊 Core Visual Walkthrough & Engine Pipeline Flow

```
tick dict
   |
   v
pack_tick --> Queue.put --> Queue.get --> unpack_tick
   |
   v
ingest
   |-- fatal clock skew -----------------------> raise
   |-- bad price --------------------------------> rejected += 1
   |-- accepted --------------------------------> append gap if prior epoch exists
   v
report
   accepted, rejected, burst
   adev_ns = sqrt(0.5 * mean((g[i+1] - g[i])^2))
   mean_interarrival_us
   ring_depth {venue: len(gap ring)}
```

Gaps are concatenated in venue-code order before the Allan deviation and the mean are computed. A burst is detected per venue: eight or more consecutive gaps each strictly under 450_000 ns. The first accepted tick of a venue creates the ring and does not invent a gap.

Insert the structural terminal walkthrough recording at docs/assets/terminal-walkthrough.gif before publishing the release notes.

## ⚡ Low-Level OS Mechanics & Network Physics

Inter-arrival time is the difference of `epoch_ns` fields, not a sleep. 450 microseconds is `450_000` ns. The two-sample Allan deviation uses `statistics.fmean` on the squared first difference of those gaps and `math.sqrt` on half of that mean. `math.isnan` and `math.isfinite` run before a price can enter the ring.

Symbol identity on the wire is FNV-1a 32-bit (offset basis 2166136261, prime 16777619), not a retained string table. The event loop is single-threaded. The lock exists so a second `run` cannot tear a venue's last timestamp. There is no TCP connect, no DNS lookup, and no multicast join.

## ⚖️ Architecture Trade-offs & Pragmatic Decisions

The gap ring is process-local. A Redis hash would survive a restart and would add a serialization hop this guard does not pay. The ring dies with the process. That is acceptable because the guard refuses bad ticks; it is not the system of record.

Burst detection uses a fixed gap and a fixed count of eight. An adaptive threshold would follow a quiet venue and would also move during a quote flood. The constant 450-microsecond gate is the contract the harness asserts. A burst sets `burst` and keeps the frames that passed the price and clock gates.

## 🚀 Local Installation & Benchmarking

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
python -m ticker_telemetry_jitter_guard
python -m ticker_telemetry_jitter_guard.harness
```

```python
import asyncio

from ticker_telemetry_jitter_guard import TickerTelemetryJitterGuard


async def demo() -> None:
    guard = TickerTelemetryJitterGuard()
    report = await guard.run(
        [
            {
                "venue": "XNYS",
                "symbol": "AAPL",
                "bid": 189.25,
                "ask": 189.5,
                "size": 100,
                "epoch_ns": 1_700_000_000_000_000_000,
                "sequence": 1,
            }
        ]
    )
    accepted = int(report["accepted"])
    depth = report["ring_depth"]
    print(accepted, depth)


asyncio.run(demo())
```

Runtime dependencies are the Python 3.12 standard library. `requirements.txt` is comments only, so `pip install -r requirements.txt` succeeds without fetching a package. Install the wheel with `pip install .`.

## 🖥️ Terminal Diagnostic Output Preview

```
2026-10-02T03:20:07+0000 WARNING [ticker_telemetry_jitter_guard.engine] clock skew clamped venue=XNYS backward_ns=1500 held_ns=1700000000010000001
2026-10-02T03:20:07+0000 WARNING [ticker_telemetry_jitter_guard.engine] burst detected min_gaps=8 gap_ns<450000
2026-10-02T03:20:07+0000 INFO [ticker_telemetry_jitter_guard.engine] topic=md.ticks.nbbo accepted=16 rejected=0 burst=True adev_ns=541010.308 mean_interarrival_us=771.429 ring_depth={'XNAS': 8, 'XNYS': 6}
2026-10-02T03:20:07+0000 INFO [__main__] {'topic': 'md.ticks.nbbo', 'accepted': 16, 'rejected': 0, 'burst': True, 'adev_ns': 541010.3084472534, 'mean_interarrival_us': 771.4286428571428, 'ring_depth': {'XNAS': 8, 'XNYS': 6}, 'clamped': 1, 'gaps_ns': [100000, 100000, 100000, 100000, 100000, 100000, 100000, 100000, 2000000, 2000000, 2000000, 2000000, 2000000, 1]}
```

`python -m ticker_telemetry_jitter_guard` exits 0. The clamped XNYS gap is 1 ns. The eight 100_000 ns XNAS gaps set `burst`. The mean mixes those rings in venue-code order, XNAS then XNYS.

## 📊 Empirical Benchmarking Performance Report

Numbers below are the status dict printed by `PYTHONPATH=src python -m ticker_telemetry_jitter_guard.harness` on this tree. The harness uses `random.Random(613)`, 5000 iterations of a four-tick batch, `time.perf_counter_ns` converted to microseconds, and `tracemalloc` peak during the benchmark.

| Metric | Measured |
| --- | ---: |
| Status | PASS |
| Iterations | 5000 |
| Average latency | 694.572 µs |
| Empirical P99 | 815.923 µs |
| Scenario latency | 135.346 µs |
| tracemalloc peak | 206435 bytes |

## 🛡️ Edge-Case Resilience & SOC2/Regulatory Compliance

A backward clock step of five seconds or less is clamped to `last_ns + 1` and counted in `clamped`. A backward step that exceeds five seconds (`5_000_000_000` ns) raises `EngineKernelException` and names the venue. The clocks are per venue: an XNAS tick cannot make an XNYS timestamp look late.

NaN, infinity, and an inverted book (`ask < bid`) increment `rejected` and do not append a gap. The batch continues. A locked book (`ask == bid`) is accepted.

The guard does not store an account identifier. The symbol crosses the wire as an FNV-1a hash. SEC Rule 613 is the clock-ordering reference for the monotonic gate. This module does not file a CAT report and does not claim to be a clock source. Processing integrity is the operational control: a rejected frame is logged and is absent from that venue's gap ring.
