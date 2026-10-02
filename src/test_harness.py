"""Deterministic checks and a short latency benchmark for the jitter guard."""

from __future__ import annotations

import asyncio
import math
import random
import sys
import time
import tracemalloc
from pathlib import Path


def _load():
    root = Path(__file__).resolve().parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    import main

    return main


def _percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    if not ordered:
        return 0.0
    index = math.ceil(fraction * len(ordered)) - 1
    index = max(0, min(index, len(ordered) - 1))
    return ordered[index]


async def _expect_raise(factory, failures: list[str], label: str) -> None:
    try:
        await factory()
    except _load().EngineKernelException:
        return
    failures.append(label)


async def _checks(mod, failures: list[str]) -> None:
    rng = random.Random(613)
    guard = mod.TickerTelemetryJitterGuard()
    base = 1_700_000_000_000_000_000
    normal: list[bytes] = []
    for index in range(40):
        bid = 100.0 + rng.random()
        normal.append(
            guard.pack_frame(
                "XNYS",
                "AAPL",
                bid,
                bid + 0.05,
                25 + index,
                base + index * 1_000_000,
            )
        )
    report = await guard.run_worker(normal)
    if report["accepted_count"] != 40 or report["rejected_count"] != 0:
        failures.append("clean batch was not fully accepted")
    if report["burst_detected"]:
        failures.append("millisecond spacing was marked as a burst")
    if not math.isclose(
        report["mean_interarrival_us"], 1000.0, rel_tol=0.0, abs_tol=1e-6
    ):
        failures.append("mean interarrival drifted from 1000 us")
    sequence = report["clamped_sequence"]
    for prev, cur in zip(sequence, sequence[1:]):
        if int(cur["epoch_ns"]) <= int(prev["epoch_ns"]):
            failures.append("clean sequence was not strictly increasing")
            break
    encoded = guard.pack_frame("XNAS", "MSFT", 10.0, 10.25, 5, base)
    unpacked = guard.unpack_frame(encoded)
    if unpacked["symbol_hash"] != mod.symbol_hash("MSFT"):
        failures.append("symbol hash did not round-trip")
    if unpacked["venue"] != "XNAS":
        failures.append("venue code did not round-trip")

    inverted = mod.TickerTelemetryJitterGuard()
    await _expect_raise(
        lambda: inverted.admit(inverted.pack_frame("ARCX", "IBM", 20.0, 19.0, 1, base)),
        failures,
        "inverted market did not raise",
    )
    poisoned = mod.TickerTelemetryJitterGuard()
    await _expect_raise(
        lambda: poisoned.admit(
            poisoned.pack_frame("ARCX", "IBM", float("nan"), 19.0, 1, base)
        ),
        failures,
        "NaN price did not raise",
    )
    infinite = mod.TickerTelemetryJitterGuard()
    await _expect_raise(
        lambda: infinite.admit(
            infinite.pack_frame("ARCX", "IBM", 10.0, float("inf"), 1, base)
        ),
        failures,
        "non-finite price did not raise",
    )
    empty = mod.TickerTelemetryJitterGuard()
    try:
        await empty.run_worker([])
        failures.append("empty batch did not raise")
    except mod.EngineKernelException as exc:
        if not str(exc):
            failures.append("empty batch exception had no message")
    short = mod.TickerTelemetryJitterGuard()
    await _expect_raise(
        lambda: short.admit(b"\x00\x01"), failures, "short frame did not raise"
    )

    skew = mod.TickerTelemetryJitterGuard()
    skew_frames = [
        skew.pack_frame("XNYS", "AAPL", 10.0, 10.1, 8, 5_000_000_000),
        skew.pack_frame("XNYS", "AAPL", 10.0, 10.1, 8, 5_000_000_000 - 2_000),
        skew.pack_frame("XNYS", "AAPL", 10.0, 10.1, 8, 5_000_000_000 + 5_000_000),
    ]
    skew_report = await skew.run_worker(skew_frames)
    skew_seq = skew_report["clamped_sequence"]
    if not skew_seq[1]["skew_clamped"]:
        failures.append("mild clock skew was not clamped")
    if int(skew_seq[1]["epoch_ns"]) != int(skew_seq[0]["epoch_ns"]) + 1:
        failures.append("clamped skew timestamp was not last+1")
    if int(skew_report["skew_clamps"]) < 1:
        failures.append("skew clamp counter did not move")

    severe = mod.TickerTelemetryJitterGuard()
    severe_frames = [
        severe.pack_frame("XNYS", "AAPL", 10.0, 10.2, 4, 10_000_000_000),
        severe.pack_frame("XNYS", "AAPL", 10.0, 10.2, 4, 1),
        severe.pack_frame("XNYS", "AAPL", 10.1, 10.3, 4, 10_005_000_000),
    ]
    severe_report = await severe.run_worker(severe_frames)
    if severe_report["rejected_count"] != 1 or severe_report["accepted_count"] != 2:
        failures.append("extreme clock skew was not rejected")
    if any("clock skew" not in item for item in severe_report["rejections"]):
        failures.append("extreme clock skew rejection text missing")

    burst = mod.TickerTelemetryJitterGuard()
    burst_start = 8_000_000_000_000
    burst_frames = [
        burst.pack_frame("XNAS", "MSFT", 50.0, 50.05, 3, burst_start + step * 100_000)
        for step in range(10)
    ]
    burst_report = await burst.run_worker(burst_frames)
    if not burst_report["burst_detected"]:
        failures.append("450us burst was not detected")
    clamped = [row for row in burst_report["clamped_sequence"] if row["burst_clamped"]]
    if len(clamped) < 8:
        failures.append("burst clamp did not cover the run")
    for prev, cur in zip(clamped, clamped[1:]):
        if int(cur["epoch_ns"]) - int(prev["epoch_ns"]) != mod.BURST_GAP_NS:
            failures.append("burst spacing was not clamped to 450us")
            break
    epochs = [int(row["epoch_ns"]) for row in burst_report["clamped_sequence"]]
    if epochs != sorted(epochs) or len(set(epochs)) != len(epochs):
        failures.append("clamped burst sequence was not strictly increasing")

    locked = mod.TickerTelemetryJitterGuard()
    locked_report = await locked.run_worker(
        [locked.pack_frame("XNYS", "T", 11.0, 11.0, 1, base)]
    )
    if locked_report["accepted_count"] != 1:
        failures.append("locked market ask==bid was rejected")

    mixed = mod.TickerTelemetryJitterGuard()
    mixed_report = await mixed.run_worker(
        [
            mixed.pack_frame("XNYS", "AAPL", 10.0, 10.2, 1, base),
            mixed.pack_frame("XNYS", "AAPL", 12.0, 11.0, 1, base + 2_000_000),
            mixed.pack_frame("XNYS", "AAPL", float("nan"), 11.0, 1, base + 4_000_000),
            mixed.pack_frame("XNYS", "AAPL", 10.2, 10.4, 1, base + 6_000_000),
        ]
    )
    if mixed_report["rejected_count"] != 2 or mixed_report["accepted_count"] != 2:
        failures.append("mixed batch rejection count mismatch")

    ring = mod.TickerTelemetryJitterGuard(ring_capacity=4)
    epoch = 9_000_000_000_000
    for _ in range(10):
        epoch += 1_000_000
        await ring.admit(ring.pack_frame("XNYS", "AAPL", 10.0, 10.2, 1, epoch))
        epoch += 1_000_000
        await ring.admit(ring.pack_frame("XNAS", "MSFT", 20.0, 20.2, 1, epoch))
    xnys = await ring.ring_snapshot("XNYS")
    xnas = await ring.ring_snapshot("XNAS")
    if len(xnys) != 4 or len(xnas) != 4:
        failures.append("venue ring did not honor maxlen")
    if any(row["venue"] != "XNYS" for row in xnys):
        failures.append("XNYS ring contains another venue")
    if any(row["venue"] != "XNAS" for row in xnas):
        failures.append("XNAS ring contains another venue")
    if {row["symbol_hash"] for row in xnys} & {row["symbol_hash"] for row in xnas}:
        failures.append("venue rings share a symbol hash")


async def _benchmark(mod) -> list[float]:
    rng = random.Random(613)
    guard = mod.TickerTelemetryJitterGuard()
    payloads: list[bytes] = []
    for index in range(8_000):
        bid = 180.0 + rng.random()
        payloads.append(
            guard.pack_frame(
                "XNYS",
                "AAPL",
                bid,
                bid + 0.02,
                100,
                2_000_000_000_000_000 + index * 1_000_000,
            )
        )
    samples: list[float] = []
    for payload in payloads:
        started = time.perf_counter_ns()
        await guard.admit(payload)
        samples.append((time.perf_counter_ns() - started) / 1_000.0)
    return samples


def main() -> int:
    mod = _load()
    failures: list[str] = []
    asyncio.run(_checks(mod, failures))
    probe = mod.TickerTelemetryJitterGuard()
    frame = probe.pack_frame("XNYS", "AAPL", 101.0, 101.05, 10, 3_000_000_000_000)
    started = time.perf_counter_ns()
    asyncio.run(probe.admit(frame))
    latency_us = (time.perf_counter_ns() - started) / 1_000.0
    tracemalloc.start()
    samples = asyncio.run(_benchmark(mod))
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
    sys.stdout.write(repr(status) + "\n")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
