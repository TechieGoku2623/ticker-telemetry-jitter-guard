"""NBBO tick jitter guard.

Frames are packed on the wire layout used by the venue adapters. The kernel
keeps one deque ring per venue so a noisy book cannot overwrite another
venue's cache. The in-process queue is the stand-in for the Kafka topic
``md.ticks.nbbo``. A Redis hash would be the multi-process cache; the ring
avoids that hop inside this process.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import statistics
import struct
import sys
from collections import deque
from typing import Final

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("ticker.jitter")

TOPIC: Final[str] = "md.ticks.nbbo"
FRAME: Final[struct.Struct] = struct.Struct("<4sIddIQ")
BURST_GAP_NS: Final[int] = 450_000
BURST_MIN_TICKS: Final[int] = 8
MAX_BACKWARD_NS: Final[int] = 5_000_000_000
RING_CAPACITY: Final[int] = 256


class EngineKernelException(Exception):
    """Raised when a tick cannot be repaired inside the kernel."""


def symbol_hash(symbol: str) -> int:
    """Return the FNV-1a 32-bit hash of an ASCII symbol."""
    value = 2_166_136_261
    for octet in symbol.encode("ascii"):
        value ^= octet
        value = (value * 16_777_619) & 0xFFFFFFFF
    return value


def _mean_stdev(samples: list[float]) -> tuple[float, float]:
    if not samples:
        return 0.0, 0.0
    center = statistics.fmean(samples)
    if len(samples) == 1:
        return center, 0.0
    return center, statistics.pstdev(samples)


class TickerTelemetryJitterGuard:
    """Validate, clock-clamp, and burst-clamp a consolidated NBBO batch."""

    def __init__(
        self,
        ring_capacity: int = RING_CAPACITY,
        burst_gap_ns: int = BURST_GAP_NS,
        burst_min_ticks: int = BURST_MIN_TICKS,
        max_backward_ns: int = MAX_BACKWARD_NS,
    ) -> None:
        if ring_capacity < 1:
            raise EngineKernelException("ring capacity must be positive")
        if burst_min_ticks < 2:
            raise EngineKernelException("burst length must cover at least two ticks")
        if burst_gap_ns < 1:
            raise EngineKernelException("burst gap must be positive")
        self._ring_capacity = ring_capacity
        self._burst_gap_ns = burst_gap_ns
        self._burst_min_ticks = burst_min_ticks
        self._max_backward_ns = max_backward_ns
        self._lock = asyncio.Lock()
        self._inbound: asyncio.Queue[bytes] = asyncio.Queue()
        self._venue_rings: dict[str, deque[dict[str, object]]] = {}
        self._last_epoch_ns: int | None = None
        self._skew_clamps = 0

    def pack_frame(
        self,
        venue: str,
        symbol: str,
        bid: float,
        ask: float,
        size: int,
        epoch_ns: int,
    ) -> bytes:
        """Pack one tick. Layout: venue, symbol hash, bid, ask, size, epoch ns."""
        try:
            raw_venue = venue.encode("ascii")
            symbol.encode("ascii")
        except UnicodeEncodeError as exc:
            raise EngineKernelException("venue and symbol must be ASCII") from exc
        if not raw_venue or len(raw_venue) > 4:
            raise EngineKernelException(f"venue code must be 1 to 4 bytes: {venue}")
        if size < 0:
            raise EngineKernelException(f"negative size {size}")
        if epoch_ns < 0:
            raise EngineKernelException(f"negative epoch {epoch_ns}")
        padded = raw_venue.ljust(4, b" ")
        try:
            return FRAME.pack(
                padded,
                symbol_hash(symbol),
                float(bid),
                float(ask),
                size,
                epoch_ns,
            )
        except struct.error as exc:
            raise EngineKernelException("frame pack failed") from exc

    def unpack_frame(self, payload: bytes) -> dict[str, object]:
        """Unpack one tick frame. Corrupt lengths raise ``EngineKernelException``."""
        if len(payload) != FRAME.size:
            raise EngineKernelException(f"frame length {len(payload)} != {FRAME.size}")
        try:
            venue_b, sym_hash, bid, ask, size, epoch_ns = FRAME.unpack(payload)
        except struct.error as exc:
            raise EngineKernelException("frame unpack failed") from exc
        return {
            "venue": venue_b.decode("ascii").rstrip(" "),
            "symbol_hash": int(sym_hash),
            "bid": float(bid),
            "ask": float(ask),
            "size": int(size),
            "epoch_ns": int(epoch_ns),
        }

    def _screen_locked(self, payload: bytes) -> dict[str, object]:
        frame = self.unpack_frame(payload)
        bid = float(frame["bid"])
        ask = float(frame["ask"])
        epoch_ns = int(frame["epoch_ns"])
        if math.isnan(bid) or math.isnan(ask):
            raise EngineKernelException(
                f"NaN price venue={frame['venue']} bid={bid} ask={ask}"
            )
        if not math.isfinite(bid) or not math.isfinite(ask):
            raise EngineKernelException(
                f"non-finite price venue={frame['venue']} bid={bid} ask={ask}"
            )
        if ask < bid:
            raise EngineKernelException(
                f"inverted market venue={frame['venue']} bid={bid} ask={ask}"
            )
        skew = False
        raw_epoch_ns = epoch_ns
        if self._last_epoch_ns is not None and epoch_ns < self._last_epoch_ns:
            backward = self._last_epoch_ns - epoch_ns
            if backward > self._max_backward_ns:
                raise EngineKernelException(
                    f"clock skew {backward} ns exceeds {self._max_backward_ns}"
                )
            epoch_ns = self._last_epoch_ns + 1
            self._skew_clamps += 1
            skew = True
            LOGGER.warning(
                "clock skew clamped venue=%s raw_ns=%d held_ns=%d backward_ns=%d",
                frame["venue"],
                raw_epoch_ns,
                epoch_ns,
                backward,
            )
        if int(frame["size"]) == 0:
            LOGGER.warning(
                "zero size accepted venue=%s symbol_hash=%s",
                frame["venue"],
                frame["symbol_hash"],
            )
        self._last_epoch_ns = epoch_ns
        frame["raw_epoch_ns"] = raw_epoch_ns
        frame["epoch_ns"] = epoch_ns
        frame["skew_clamped"] = skew
        frame["burst_clamped"] = False
        return frame

    def _apply_burst_locked(self, rows: list[dict[str, object]]) -> bool:
        detected = False
        index = 1
        gap = self._burst_gap_ns
        minimum = self._burst_min_ticks
        while index < len(rows):
            current = int(rows[index]["epoch_ns"])
            previous = int(rows[index - 1]["epoch_ns"])
            if current - previous >= gap:
                index += 1
                continue
            start = index - 1
            index += 1
            while index < len(rows):
                step = int(rows[index]["epoch_ns"]) - int(rows[index - 1]["epoch_ns"])
                if step >= gap:
                    break
                index += 1
            run_length = index - start
            if run_length >= minimum:
                detected = True
                anchor = int(rows[start]["epoch_ns"])
                for step, cursor in enumerate(range(start, index)):
                    rows[cursor]["epoch_ns"] = anchor + step * gap
                    rows[cursor]["burst_clamped"] = True
                LOGGER.warning(
                    "burst clamped ticks=%d gap_ns=%d anchor_ns=%d",
                    run_length,
                    gap,
                    anchor,
                )
        for cursor in range(1, len(rows)):
            prev_epoch = int(rows[cursor - 1]["epoch_ns"])
            cur_epoch = int(rows[cursor]["epoch_ns"])
            if cur_epoch <= prev_epoch:
                rows[cursor]["epoch_ns"] = prev_epoch + gap
                rows[cursor]["burst_clamped"] = True
        if rows:
            self._last_epoch_ns = int(rows[-1]["epoch_ns"])
        return detected

    def _remember_locked(self, row: dict[str, object]) -> None:
        venue = str(row["venue"])
        ring = self._venue_rings.get(venue)
        if ring is None:
            ring = deque(maxlen=self._ring_capacity)
            self._venue_rings[venue] = ring
        ring.append(
            {
                "venue": venue,
                "symbol_hash": row["symbol_hash"],
                "bid": row["bid"],
                "ask": row["ask"],
                "size": row["size"],
                "epoch_ns": row["epoch_ns"],
            }
        )

    def _sequence_view(self, rows: list[dict[str, object]]) -> list[dict[str, object]]:
        view: list[dict[str, object]] = []
        for row in rows:
            view.append(
                {
                    "venue": row["venue"],
                    "symbol_hash": row["symbol_hash"],
                    "bid": row["bid"],
                    "ask": row["ask"],
                    "size": row["size"],
                    "epoch_ns": row["epoch_ns"],
                    "burst_clamped": row["burst_clamped"],
                    "skew_clamped": row["skew_clamped"],
                }
            )
        return view

    async def admit(self, payload: bytes) -> dict[str, object]:
        """Screen one frame. Faults raise; mild clock skew is clamped."""
        async with self._lock:
            row = self._screen_locked(payload)
            self._remember_locked(row)
            return {
                "venue": row["venue"],
                "symbol_hash": row["symbol_hash"],
                "bid": row["bid"],
                "ask": row["ask"],
                "size": row["size"],
                "epoch_ns": row["epoch_ns"],
                "skew_clamped": row["skew_clamped"],
            }

    async def ring_snapshot(self, venue: str) -> list[dict[str, object]]:
        """Return a copy of one venue ring. Rings are not shared across venues."""
        async with self._lock:
            ring = self._venue_rings.get(venue)
            if ring is None:
                return []
            return list(ring)

    async def run_worker(self, payloads: list[bytes]) -> dict[str, object]:
        """Drain the ``md.ticks.nbbo`` stand-in and return a JSON object."""
        if not payloads:
            raise EngineKernelException("empty tick batch")
        for payload in payloads:
            await self._inbound.put(payload)
        buffered: list[bytes] = []
        for _ in range(len(payloads)):
            buffered.append(await self._inbound.get())
        accepted: list[dict[str, object]] = []
        rejections: list[str] = []
        async with self._lock:
            for payload in buffered:
                try:
                    accepted.append(self._screen_locked(payload))
                except EngineKernelException as exc:
                    rejections.append(str(exc))
                    LOGGER.warning("frame rejected error=%s", exc)
            burst = self._apply_burst_locked(accepted)
            for row in accepted:
                self._remember_locked(row)
            deltas_us: list[float] = []
            for prev, cur in zip(accepted, accepted[1:]):
                delta_ns = int(cur["epoch_ns"]) - int(prev["epoch_ns"])
                deltas_us.append(delta_ns / 1_000.0)
            mean_us, stdev_us = _mean_stdev(deltas_us)
            ring_depth = sum(len(ring) for ring in self._venue_rings.values())
            report: dict[str, object] = {
                "topic": TOPIC,
                "accepted_count": len(accepted),
                "rejected_count": len(rejections),
                "burst_detected": burst,
                "skew_clamps": self._skew_clamps,
                "mean_interarrival_us": mean_us,
                "stdev_interarrival_us": stdev_us,
                "ring_depth": ring_depth,
                "venues": sorted(self._venue_rings),
                "rejections": rejections,
                "clamped_sequence": self._sequence_view(accepted),
            }
        decoded: dict[str, object] = json.loads(json.dumps(report))
        LOGGER.info(
            "batch topic=%s accepted=%d rejected=%d burst=%s "
            "mean_interarrival_us=%.3f ring_depth=%d",
            TOPIC,
            decoded["accepted_count"],
            decoded["rejected_count"],
            decoded["burst_detected"],
            decoded["mean_interarrival_us"],
            decoded["ring_depth"],
        )
        return decoded


async def _scenario() -> None:
    guard = TickerTelemetryJitterGuard()
    base = 1_700_000_000_000_000_000
    frames: list[bytes] = []
    for offset in range(6):
        frames.append(
            guard.pack_frame(
                "XNYS",
                "AAPL",
                189.20 + offset * 0.01,
                189.24 + offset * 0.01,
                100 + offset,
                base + offset * 2_000_000,
            )
        )
    burst_start = base + 20_000_000
    for step in range(10):
        frames.append(
            guard.pack_frame(
                "XNAS",
                "MSFT",
                410.10,
                410.14,
                50,
                burst_start + step * 100_000,
            )
        )
    frames.append(
        guard.pack_frame(
            "XNAS",
            "MSFT",
            410.10,
            410.14,
            40,
            burst_start + 50_000,
        )
    )
    frames.append(
        guard.pack_frame("ARCX", "AAPL", 189.40, 189.30, 10, base + 40_000_000)
    )
    frames.append(
        guard.pack_frame(
            "ARCX",
            "AAPL",
            float("nan"),
            189.50,
            10,
            base + 42_000_000,
        )
    )
    frames.append(
        guard.pack_frame("XNYS", "AAPL", 189.50, 189.55, 80, base + 50_000_000)
    )
    report = await guard.run_worker(frames)
    sys.stdout.write(json.dumps(report) + "\n")


if __name__ == "__main__":
    asyncio.run(_scenario())
