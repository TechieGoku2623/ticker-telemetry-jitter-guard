"""Deterministic checks and a 5000-iteration latency benchmark."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import sys
import time
import tracemalloc
from typing import Mapping, Sequence

from .engine import (
    BURST_GAP_NS,
    BURST_MIN_GAPS,
    MAX_BACKWARD_NS,
    TickerTelemetryJitterGuard,
)
from .exceptions import EngineKernelException
from .wire import fnv1a_32, pack_tick, unpack_tick

ITERATIONS: int = 5_000
SEED: int = 613


def _percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    if not ordered:
        return 0.0
    index = math.ceil(fraction * len(ordered)) - 1
    return ordered[max(0, min(index, len(ordered) - 1))]


def _tick(
    venue: str,
    symbol: str,
    bid: float,
    ask: float,
    size: int,
    epoch_ns: int,
    sequence: int,
) -> dict[str, object]:
    return {
        "venue": venue,
        "symbol": symbol,
        "bid": bid,
        "ask": ask,
        "size": size,
        "epoch_ns": epoch_ns,
        "sequence": sequence,
    }


async def _checks(failures: list[str]) -> None:
    payload = pack_tick(
        "XNYS", "AAPL", 189.25, 189.5, 100, 1_700_000_000_000_000_000, 7
    )
    frame = unpack_tick(payload)
    if (
        frame["symbol_hash"] != fnv1a_32("AAPL")
        or frame["symbol_hash"] != 1_489_842_955
    ):
        failures.append("symbol hash did not round-trip")
    if frame["venue"] != "XNYS" or frame["bid"] != 189.25 or frame["sequence"] != 7:
        failures.append("finite tick fields did not round-trip")
    if payload != pack_tick(
        "XNYS", "AAPL", 189.25, 189.5, 100, 1_700_000_000_000_000_000, 7
    ):
        failures.append("pack was not stable")
    try:
        unpack_tick(b"\x00\x01")
        failures.append("short frame did not raise")
    except EngineKernelException as exc:
        if not str(exc):
            failures.append("short frame exception had no message")

    base = 1_000_000_000_000
    clean = TickerTelemetryJitterGuard()
    report = await clean.run(
        [
            _tick("XNYS", "AAPL", 10.5, 10.75, 10, base, 1),
            _tick("XNYS", "AAPL", 10.5, 10.75, 10, base + 100, 2),
            _tick("XNYS", "AAPL", 10.5, 10.75, 10, base + 400, 3),
        ]
    )
    if report["accepted"] != 3 or report["rejected"] != 0 or report["burst"]:
        failures.append("clean batch was not accepted")
    if report["gaps_ns"] != [100, 300]:
        failures.append(f"gaps were {report['gaps_ns']}")
    expected_adev = math.sqrt(0.5 * float((300 - 100) ** 2))
    if not math.isclose(float(report["adev_ns"]), expected_adev, abs_tol=1e-9):
        failures.append(f"adev was {report['adev_ns']}")
    if not math.isclose(float(report["mean_interarrival_us"]), 0.2, abs_tol=1e-12):
        failures.append("mean interarrival drifted")
    depth = report["ring_depth"]
    if not isinstance(depth, dict) or depth.get("XNYS") != 2:
        failures.append(f"ring depth was {depth}")

    locked = await TickerTelemetryJitterGuard().run(
        [_tick("XNYS", "AAPL", 11.0, 11.0, 1, base, 1)]
    )
    if locked["accepted"] != 1:
        failures.append("locked market ask==bid was rejected")

    mild = await TickerTelemetryJitterGuard().run(
        [
            _tick("XNYS", "AAPL", 10.0, 10.25, 1, 5_000_000_000, 1),
            _tick("XNYS", "AAPL", 10.0, 10.25, 1, 5_000_000_000 - 1_000, 2),
        ]
    )
    if mild["clamped"] != 1 or mild["gaps_ns"] != [1] or mild["accepted"] != 2:
        failures.append("sub-five-second backward step was not clamped to last+1")

    boundary = await TickerTelemetryJitterGuard().run(
        [
            _tick("XNAS", "MSFT", 10.0, 10.25, 1, 20_000_000_000, 1),
            _tick("XNAS", "MSFT", 10.0, 10.25, 1, 20_000_000_000 - MAX_BACKWARD_NS, 2),
        ]
    )
    if boundary["clamped"] != 1 or boundary["accepted"] != 2:
        failures.append("exactly five seconds of backward skew was not clamped")

    other_venue = await TickerTelemetryJitterGuard().run(
        [
            _tick("XNYS", "AAPL", 10.0, 10.25, 1, 50_000_000_000, 1),
            _tick("XNAS", "MSFT", 10.0, 10.25, 1, 1_000, 1),
        ]
    )
    if other_venue["accepted"] != 2 or other_venue["clamped"] != 0:
        failures.append("per-venue clock leaked across venues")

    try:
        await TickerTelemetryJitterGuard().run(
            [
                _tick("XNYS", "AAPL", 10.0, 10.25, 1, 10_000_000_000, 1),
                _tick(
                    "XNYS",
                    "AAPL",
                    10.0,
                    10.25,
                    1,
                    10_000_000_000 - MAX_BACKWARD_NS - 1,
                    2,
                ),
            ]
        )
        failures.append("backward skew over 5 seconds did not raise")
    except EngineKernelException as exc:
        if "clock skew" not in str(exc):
            failures.append("clock skew exception text missing")

    screened = await TickerTelemetryJitterGuard().run(
        [
            _tick("ARCX", "IBM", float("nan"), 19.0, 1, base, 1),
            _tick("ARCX", "IBM", 10.0, float("inf"), 1, base + 1_000_000, 2),
            _tick("ARCX", "IBM", 12.0, 11.0, 1, base + 2_000_000, 3),
            _tick("ARCX", "IBM", 10.0, 10.25, 1, base + 3_000_000, 4),
        ]
    )
    if screened["rejected"] != 3 or screened["accepted"] != 1:
        failures.append("NaN, infinity, or inverted market was not rejected")

    burst_start = 8_000_000_000_000
    burst_rows: list[dict[str, object]] = [
        _tick("XNAS", "MSFT", 50.0, 50.25, 3, burst_start + step * 100_000, step + 1)
        for step in range(BURST_MIN_GAPS + 1)
    ]
    burst_report = await TickerTelemetryJitterGuard().run(burst_rows)
    if not burst_report["burst"]:
        failures.append("run of 8 sub-450us gaps was not a burst")
    gaps = burst_report["gaps_ns"]
    if not isinstance(gaps, list) or len(gaps) != BURST_MIN_GAPS:
        failures.append("burst gap count mismatch")
    elif any(int(gap) >= BURST_GAP_NS for gap in gaps):
        failures.append("burst gaps were not under 450000 ns")

    quiet = await TickerTelemetryJitterGuard().run(
        [
            _tick("XNYS", "T", 11.0, 11.25, 1, base + step * 1_000_000, step + 1)
            for step in range(BURST_MIN_GAPS + 1)
        ]
    )
    if quiet["burst"]:
        failures.append("millisecond spacing was marked as a burst")


def _batches(rng: random.Random) -> list[list[dict[str, object]]]:
    batches: list[list[dict[str, object]]] = []
    epoch = 2_000_000_000_000_000
    sequence = 1
    for _ in range(ITERATIONS):
        batch: list[dict[str, object]] = []
        for _slot in range(4):
            epoch += 1_000_000
            bid = 180.0 + rng.random()
            batch.append(_tick("XNYS", "AAPL", bid, bid + 0.25, 100, epoch, sequence))
            sequence += 1
        batches.append(batch)
    return batches


async def _benchmark(
    batches: Sequence[Sequence[Mapping[str, object]]],
) -> list[float]:
    guard = TickerTelemetryJitterGuard()
    samples: list[float] = []
    for batch in batches:
        started = time.perf_counter_ns()
        await guard.run(batch)
        samples.append((time.perf_counter_ns() - started) / 1_000.0)
    return samples


def main() -> int:
    """Print one status dict and exit 0 only when every check passes."""
    logging.getLogger("ticker_telemetry_jitter_guard").setLevel(logging.ERROR)
    failures: list[str] = []
    asyncio.run(_checks(failures))
    probe = TickerTelemetryJitterGuard()
    started = time.perf_counter_ns()
    asyncio.run(
        probe.run(
            [
                _tick("XNYS", "AAPL", 101.0, 101.25, 10, 3_000_000_000_000, 1),
                _tick("XNYS", "AAPL", 101.0, 101.25, 12, 3_000_001_000_000, 2),
            ]
        )
    )
    latency_us = (time.perf_counter_ns() - started) / 1_000.0
    batches = _batches(random.Random(SEED))
    tracemalloc.start()
    samples = asyncio.run(_benchmark(batches))
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    status = {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "latency_us": round(latency_us, 3),
        "memory_peak_bytes": peak,
        "benchmark_iterations": len(samples),
        "benchmark_avg_us": round(sum(samples) / len(samples), 3),
        "benchmark_p99_us": round(_percentile(samples, 0.99), 3),
    }
    sys.stdout.write(json.dumps(status) + "\n")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
