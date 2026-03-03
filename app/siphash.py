from __future__ import annotations


MASK64 = 0xFFFFFFFFFFFFFFFF


def _rotl64(value: int, shift: int) -> int:
    return ((value << shift) | (value >> (64 - shift))) & MASK64


def _sipround(v0: int, v1: int, v2: int, v3: int) -> tuple[int, int, int, int]:
    v0 = (v0 + v1) & MASK64
    v1 = _rotl64(v1, 13)
    v1 ^= v0
    v0 = _rotl64(v0, 32)

    v2 = (v2 + v3) & MASK64
    v3 = _rotl64(v3, 16)
    v3 ^= v2

    v0 = (v0 + v3) & MASK64
    v3 = _rotl64(v3, 21)
    v3 ^= v0

    v2 = (v2 + v1) & MASK64
    v1 = _rotl64(v1, 17)
    v1 ^= v2
    v2 = _rotl64(v2, 32)

    return v0, v1, v2, v3


def siphash24(key: bytes, message: bytes) -> int:
    """Compute SipHash-2-4 using a 16-byte secret key."""
    if len(key) != 16:
        raise ValueError("SipHash key must be exactly 16 bytes")

    k0 = int.from_bytes(key[:8], "little")
    k1 = int.from_bytes(key[8:], "little")

    v0 = 0x736F6D6570736575 ^ k0
    v1 = 0x646F72616E646F6D ^ k1
    v2 = 0x6C7967656E657261 ^ k0
    v3 = 0x7465646279746573 ^ k1

    msg_len = len(message)
    full_blocks = msg_len // 8

    for block_idx in range(full_blocks):
        start = block_idx * 8
        m = int.from_bytes(message[start : start + 8], "little")
        v3 ^= m
        v0, v1, v2, v3 = _sipround(v0, v1, v2, v3)
        v0, v1, v2, v3 = _sipround(v0, v1, v2, v3)
        v0 ^= m

    tail = message[full_blocks * 8 :]
    b = msg_len << 56
    for i, byte in enumerate(tail):
        b |= byte << (8 * i)

    v3 ^= b
    v0, v1, v2, v3 = _sipround(v0, v1, v2, v3)
    v0, v1, v2, v3 = _sipround(v0, v1, v2, v3)
    v0 ^= b

    v2 ^= 0xFF
    for _ in range(4):
        v0, v1, v2, v3 = _sipround(v0, v1, v2, v3)

    return (v0 ^ v1 ^ v2 ^ v3) & MASK64
