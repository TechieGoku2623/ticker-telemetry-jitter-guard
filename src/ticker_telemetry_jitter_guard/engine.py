"""Per-venue clock clamp, quote gate, and two-sample Allan deviation."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import statistics
import struct
from collections import deque
from typing import Mapping, Sequence

from .exceptions import EngineKernelException
from .wire import FORMAT, pack_tick, unpack_tick

LOGGER = logging.getLogger(__name__)

TOPIC: str = "md.ticks.nbbo"
BURST_GAP_NS: int = 450_000
BURST_MIN_GAPS: int = 8
MAX_BACKWARD_NS: int = 5_000_000_000
RING_CAPACITY: int = 256


def _as_int(record: Mapping[str, object], key: str) -> int:
    raw = record[key]
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise EngineKernelException(f"{key} must be an integer")
    return raw


def _as_float(record: Mapping[str, object], key: str) -> float:
    raw = record[key]
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise EngineKernelException(f"{key} must be numeric")
    return float(raw)


def _as_str(record: Mapping[str, object], key: str) -> str:
    raw = record[key]
    if not isinstance(raw, str):
        raise EngineKernelException(f"{key} must be a string")
    return raw


def _check_size(payload: bytes) -> None:
    expected = struct.calcsize(FORMAT)
    if len(payload) != expected:
        raise EngineKernelException(f"frame length {len(payload)} != {expected}")


def _json_object(report: dict[str, object]) -> dict[str, object]:
    decoded = json.loads(json.dumps(report, allow_nan=False))
    if not isinstance(decoded, dict):
        raise EngineKernelException("report was not a JSON object")
    return decoded


def _burst(gaps: deque[int]) -> bool:
    run = 0
    for gap in gaps:
        if gap < BURST_GAP_NS:
            run += 1
            if run >= BURST_MIN_GAPS:
                return True
        else:
            run = 0
    return False


def _allan_deviation_ns(gaps: list[int]) -> float:
    """Two-sample Allan deviation of an inter-arrival series.

    ADEV = sqrt(0.5 * mean((g[i+1] - g[i]) ** 2)). Fewer than two gaps
    has no adjacent pair, so the deviation is 0.
    """
    if len(gaps) < 2:
        return 0.0
    squares = [
        float(gaps[index + 1] - gaps[index]) ** 2 for index in range(len(gaps) - 1)
    ]
    return math.sqrt(0.5 * statistics.fmean(squares))


class TickerTelemetryJitterGuard:
    """Admit consolidated NBBO ticks onto an in-process ``md.ticks.nbbo`` queue."""

    def __init__(self, ring_capacity: int = RING_CAPACITY) -> None:
        if ring_capacity < 2:
            raise EngineKernelException("ring capacity must be at least 2")
        self._ring_capacity = ring_capacity
        self._lock = asyncio.Lock()
        self._inbound: asyncio.Queue[bytes] = asyncio.Queue()
        self._gaps: dict[str, deque[int]] = {}
        self._last_ns: dict[str, int] = {}
        self._frame_size = struct.calcsize(FORMAT)

    async def run(self, records: Sequence[Mapping[str, object]]) -> dict[str, object]:
        """Screen a batch and return JSON-serializable admission counters."""
        batch = list(records)
        if not batch:
            raise EngineKernelException("empty tick batch")
        accepted = 0
        rejected = 0
        clamped = 0
        async with self._lock:
            for record in batch:
                payload = pack_tick(
                    _as_str(record, "venue"),
                    _as_str(record, "symbol"),
                    _as_float(record, "bid"),
                    _as_float(record, "ask"),
                    _as_int(record, "size"),
                    _as_int(record, "epoch_ns"),
                    _as_int(record, "sequence"),
                )
                _check_size(payload)
                if len(payload) != self._frame_size:
                    raise EngineKernelException("frame size mismatch")
                await self._inbound.put(payload)
                queued = await self._inbound.get()
                frame = unpack_tick(queued)
                try:
                    was_clamped = self.ingest(frame)
                except EngineKernelException as exc:
                    if exc.fatal:
                        raise
                    rejected += 1
                    LOGGER.warning("tick rejected error=%s", exc)
                    continue
                accepted += 1
                if was_clamped:
                    clamped += 1
            report = self._report(accepted, rejected, clamped)
        LOGGER.info(
            "topic=%s accepted=%d rejected=%d burst=%s adev_ns=%.3f "
            "mean_interarrival_us=%.3f ring_depth=%s",
            TOPIC,
            report["accepted"],
            report["rejected"],
            report["burst"],
            report["adev_ns"],
            report["mean_interarrival_us"],
            report["ring_depth"],
        )
        return _json_object(report)

    def ingest(self, frame: Mapping[str, object]) -> bool:
        """Screen one unpacked frame.

        ``run`` holds the engine lock. A small backward step is clamped to
        ``last + 1``. A backward step above five seconds raises. NaN, infinity,
        and ``ask < bid`` are non-fatal rejects.
        """
        bid = float(frame["bid"])
        ask = float(frame["ask"])
        venue = str(frame["venue"])
        epoch_ns = int(frame["epoch_ns"])
        if math.isnan(bid) or math.isnan(ask):
            raise EngineKernelException(
                f"NaN price venue={venue} bid={bid} ask={ask}",
                fatal=False,
            )
        if not math.isfinite(bid) or not math.isfinite(ask):
            raise EngineKernelException(
                f"non-finite price venue={venue} bid={bid} ask={ask}",
                fatal=False,
            )
        if ask < bid:
            raise EngineKernelException(
                f"inverted market venue={venue} bid={bid} ask={ask}",
                fatal=False,
            )
        ring = self._gaps.get(venue)
        if ring is None:
            ring = deque(maxlen=self._ring_capacity)
            self._gaps[venue] = ring
        clamped = False
        last = self._last_ns.get(venue)
        if last is not None and epoch_ns < last:
            backward = last - epoch_ns
            if backward > MAX_BACKWARD_NS:
                raise EngineKernelException(
                    f"clock skew {backward} ns exceeds {MAX_BACKWARD_NS} venue={venue}"
                )
            epoch_ns = last + 1
            clamped = True
            LOGGER.warning(
                "clock skew clamped venue=%s backward_ns=%d held_ns=%d",
                venue,
                backward,
                epoch_ns,
            )
        if last is not None:
            ring.append(epoch_ns - last)
        self._last_ns[venue] = epoch_ns
        return clamped

    def _report(self, accepted: int, rejected: int, clamped: int) -> dict[str, object]:
        ordered: list[int] = []
        depth: dict[str, int] = {}
        burst = False
        for venue in sorted(self._gaps):
            ring = self._gaps[venue]
            depth[venue] = len(ring)
            ordered.extend(ring)
            if _burst(ring):
                burst = True
        if ordered:
            mean_us = statistics.fmean(ordered) / 1_000.0
        else:
            mean_us = 0.0
        if burst:
            LOGGER.warning(
                "burst detected min_gaps=%d gap_ns<%d", BURST_MIN_GAPS, BURST_GAP_NS
            )
        return {
            "topic": TOPIC,
            "accepted": accepted,
            "rejected": rejected,
            "burst": burst,
            "adev_ns": _allan_deviation_ns(ordered),
            "mean_interarrival_us": mean_us,
            "ring_depth": depth,
            "clamped": clamped,
            "gaps_ns": ordered,
        }
