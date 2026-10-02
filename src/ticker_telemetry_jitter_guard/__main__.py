"""Command-line batch for the ticker telemetry jitter guard."""

from __future__ import annotations

import asyncio
import logging

from .engine import TickerTelemetryJitterGuard


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


def main() -> int:
    """Run one mixed NBBO batch and log the admission dict."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    guard = TickerTelemetryJitterGuard()
    base = 1_700_000_000_000_000_000
    records: list[dict[str, object]] = []
    for index in range(6):
        records.append(
            _tick(
                "XNYS",
                "AAPL",
                189.25,
                189.5,
                100 + index,
                base + index * 2_000_000,
                index + 1,
            )
        )
    records.append(
        _tick(
            "XNYS",
            "AAPL",
            189.25,
            189.5,
            80,
            base + 5 * 2_000_000 - 1_500,
            7,
        )
    )
    burst_anchor = base + 30_000_000
    for step in range(9):
        records.append(
            _tick(
                "XNAS",
                "MSFT",
                410.5,
                410.75,
                40,
                burst_anchor + step * 100_000,
                step + 1,
            )
        )
    report = asyncio.run(guard.run(records))
    logging.getLogger(__name__).info("%s", report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
