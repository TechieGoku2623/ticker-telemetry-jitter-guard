"""Wire, admission, and clock-skew tests. No network."""

from __future__ import annotations

import asyncio
import logging
import math
import unittest

from ticker_telemetry_jitter_guard import (
    EngineKernelException,
    TickerTelemetryJitterGuard,
)
from ticker_telemetry_jitter_guard.engine import MAX_BACKWARD_NS
from ticker_telemetry_jitter_guard.wire import fnv1a_32, pack_tick, unpack_tick

logging.getLogger("ticker_telemetry_jitter_guard").setLevel(logging.CRITICAL)


def _tick(
    epoch_ns: int,
    sequence: int,
    bid: float = 10.5,
    ask: float = 10.75,
) -> dict[str, object]:
    return {
        "venue": "XNYS",
        "symbol": "AAPL",
        "bid": bid,
        "ask": ask,
        "size": 10,
        "epoch_ns": epoch_ns,
        "sequence": sequence,
    }


class TickerEngineTest(unittest.TestCase):
    def test_wire_roundtrip(self) -> None:
        epoch_ns = 1_700_000_000_000_000_000
        payload = pack_tick("NY", "AAPL", 189.25, 189.5, 100, epoch_ns, 7)
        frame = unpack_tick(payload)
        self.assertEqual(frame["venue"], "NY")
        self.assertEqual(frame["symbol_hash"], 1_489_842_955)
        self.assertEqual(frame["symbol_hash"], fnv1a_32("AAPL"))
        self.assertEqual(frame["bid"], 189.25)
        self.assertEqual(frame["ask"], 189.5)
        self.assertEqual(frame["size"], 100)
        self.assertEqual(frame["epoch_ns"], epoch_ns)
        self.assertEqual(frame["sequence"], 7)
        self.assertEqual(
            payload, pack_tick("NY", "AAPL", 189.25, 189.5, 100, epoch_ns, 7)
        )
        with self.assertRaises(EngineKernelException):
            unpack_tick(b"\x00\x01")

    def test_happy_path_allan_deviation(self) -> None:
        base = 1_000_000_000_000
        report = asyncio.run(
            TickerTelemetryJitterGuard().run(
                [
                    _tick(base, 1),
                    _tick(base + 100, 2),
                    _tick(base + 400, 3),
                ]
            )
        )
        self.assertEqual(report["accepted"], 3)
        self.assertEqual(report["rejected"], 0)
        self.assertFalse(report["burst"])
        self.assertEqual(report["gaps_ns"], [100, 300])
        self.assertEqual(report["ring_depth"], {"XNYS": 2})
        self.assertTrue(
            math.isclose(float(report["mean_interarrival_us"]), 0.2, abs_tol=1e-12)
        )
        expected = math.sqrt(0.5 * float((300 - 100) ** 2))
        self.assertTrue(math.isclose(float(report["adev_ns"]), expected, abs_tol=1e-9))
        self.assertEqual(report["topic"], "md.ticks.nbbo")

    def test_nonfinite_and_inverted_are_rejected(self) -> None:
        base = 2_000_000_000_000
        report = asyncio.run(
            TickerTelemetryJitterGuard().run(
                [
                    _tick(base, 1, bid=float("nan"), ask=19.0),
                    _tick(base + 1_000_000, 2, bid=10.0, ask=float("inf")),
                    _tick(base + 2_000_000, 3, bid=12.0, ask=11.0),
                    _tick(base + 3_000_000, 4),
                ]
            )
        )
        self.assertEqual(report["rejected"], 3)
        self.assertEqual(report["accepted"], 1)
        self.assertEqual(report["ring_depth"], {"XNYS": 0})

    def test_backward_skew_over_five_seconds_raises(self) -> None:
        guard = TickerTelemetryJitterGuard()
        with self.assertRaises(EngineKernelException) as caught:
            asyncio.run(
                guard.run(
                    [
                        _tick(10_000_000_000, 1),
                        _tick(10_000_000_000 - MAX_BACKWARD_NS - 1, 2),
                    ]
                )
            )
        self.assertIn("clock skew", str(caught.exception))
        self.assertTrue(caught.exception.fatal)
