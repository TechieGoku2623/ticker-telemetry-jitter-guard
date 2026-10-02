"""Little-endian NBBO frames for the md.ticks.nbbo stand-in."""

from __future__ import annotations

import struct

from .exceptions import EngineKernelException

FORMAT: str = "<4sIddIQI"
FRAME: struct.Struct = struct.Struct(FORMAT)
FNV_OFFSET: int = 2_166_136_261
FNV_PRIME: int = 16_777_619


def fnv1a_32(symbol: str) -> int:
    """Return the FNV-1a 32-bit hash of an ASCII symbol."""
    try:
        raw = symbol.encode("ascii")
    except UnicodeEncodeError as exc:
        raise EngineKernelException("symbol must be ASCII") from exc
    value = FNV_OFFSET
    for octet in raw:
        value ^= octet
        value = (value * FNV_PRIME) & 0xFFFFFFFF
    return value


def pack_tick(
    venue: str,
    symbol: str,
    bid: float,
    ask: float,
    size: int,
    epoch_ns: int,
    sequence: int,
) -> bytes:
    """Pack one tick.

    Layout, little-endian: venue ``4s``, FNV-1a symbol ``uint32``, bid
    ``float64``, ask ``float64``, size ``uint32``, epoch ``uint64``,
    sequence ``uint32``.
    """
    try:
        raw_venue = venue.encode("ascii")
    except UnicodeEncodeError as exc:
        raise EngineKernelException("venue must be ASCII") from exc
    if not raw_venue or len(raw_venue) > 4:
        raise EngineKernelException(f"venue code must be 1 to 4 bytes: {venue}")
    if size < 0 or size > 0xFFFFFFFF:
        raise EngineKernelException(f"size out of uint32 range: {size}")
    if epoch_ns < 0 or epoch_ns > 0xFFFFFFFFFFFFFFFF:
        raise EngineKernelException(f"epoch_ns out of uint64 range: {epoch_ns}")
    if sequence < 0 or sequence > 0xFFFFFFFF:
        raise EngineKernelException(f"sequence out of uint32 range: {sequence}")
    symbol_hash = fnv1a_32(symbol)
    try:
        return FRAME.pack(
            raw_venue.ljust(4, b" "),
            symbol_hash,
            float(bid),
            float(ask),
            size,
            epoch_ns,
            sequence,
        )
    except (struct.error, OverflowError, ValueError) as exc:
        raise EngineKernelException("tick pack failed") from exc


def unpack_tick(payload: bytes) -> dict[str, object]:
    """Unpack one tick. Finite values round-trip exactly."""
    if len(payload) != FRAME.size:
        raise EngineKernelException(f"frame length {len(payload)} != {FRAME.size}")
    try:
        venue_b, symbol_hash, bid, ask, size, epoch_ns, sequence = FRAME.unpack(payload)
    except struct.error as exc:
        raise EngineKernelException("tick unpack failed") from exc
    return {
        "venue": venue_b.decode("ascii").rstrip(" "),
        "symbol_hash": int(symbol_hash),
        "bid": float(bid),
        "ask": float(ask),
        "size": int(size),
        "epoch_ns": int(epoch_ns),
        "sequence": int(sequence),
    }
