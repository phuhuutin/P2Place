#!/usr/bin/env python3
"""Minimal P2Place browser-protocol WebSocket client (Python stdlib only).

Why this exists
---------------
The P2Place peer does not send a dedicated ``put_ok`` message.  The browser
protocol confirms a write the way ``web/app.js`` confirms it: after the write
quorum succeeds the gateway broadcasts

    {"op":"pixel","x":<n>,"y":<n>,"color":"#RRGGBB"}

to every connected browser, including the one that issued the PUT.  A failed
PUT produces ``{"op":"error","message":...}`` (or, in the worst case, no
application message at all before the socket dies).

Therefore a *client-visible ACK* is defined here as a ``pixel`` message on the
issuing connection whose x, y and color exactly match the request.  Server-side
``[quorum-done]`` lines are never used as ACKs; they are parsed separately for
corroboration.

All PUTs use a fresh connection and a fresh request so a stale or unrelated
broadcast cannot be mistaken for the acknowledgement of an earlier request.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import struct
import time

GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_RESPONSE_BYTES = 8 * 1024 * 1024


def normalize_color(color: object) -> str:
    """Return an upper-case #RRGGBB string (or the raw string if malformed)."""
    if not isinstance(color, str):
        return str(color).upper()
    value = color.strip()
    if len(value) == 7 and value.startswith("#"):
        return value.upper()
    return value.upper()


def _recv_exact(sock: socket.socket, buffer: bytes, size: int) -> tuple[bytes, bytes]:
    while len(buffer) < size:
        chunk = sock.recv(max(4096, size - len(buffer)))
        if not chunk:
            raise EOFError("socket closed")
        buffer += chunk
    return buffer[:size], buffer[size:]


def _send_frame(sock: socket.socket, opcode: int, payload: bytes) -> None:
    length = len(payload)
    header = bytes([0x80 | opcode])
    if length < 126:
        header += bytes([0x80 | length])
    elif length <= 0xFFFF:
        header += bytes([0x80 | 126]) + struct.pack("!H", length)
    else:
        header += bytes([0x80 | 127]) + struct.pack("!Q", length)
    mask = os.urandom(4)
    masked = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
    sock.sendall(header + mask + masked)


def _read_message(sock: socket.socket, buffer: bytes, deadline: float) -> tuple[int, bytes, bytes]:
    """Read one complete WebSocket message, handling fragmentation and control frames."""
    opcode = None
    fragments = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout("websocket read deadline")
        sock.settimeout(remaining)
        head, buffer = _recv_exact(sock, buffer, 2)
        fin = bool(head[0] & 0x80)
        op = head[0] & 0x0F
        length = head[1] & 0x7F
        if length == 126:
            raw, buffer = _recv_exact(sock, buffer, 2)
            length = struct.unpack("!H", raw)[0]
        elif length == 127:
            raw, buffer = _recv_exact(sock, buffer, 8)
            length = struct.unpack("!Q", raw)[0]
        if length > MAX_RESPONSE_BYTES:
            raise ValueError(f"oversize websocket frame ({length} bytes)")
        mask = None
        if head[1] & 0x80:
            mask, buffer = _recv_exact(sock, buffer, 4)
        payload, buffer = _recv_exact(sock, buffer, length)
        if mask:
            payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))

        if op == 0x8:
            raise EOFError("websocket close frame")
        if op == 0x9:  # ping -> pong
            _send_frame(sock, 0xA, payload)
            continue
        if op == 0xA:  # pong
            continue
        if op in (0x1, 0x2):
            opcode = op
            fragments = bytearray(payload)
        elif op == 0x0:
            fragments += payload
        else:
            continue
        if fin:
            return opcode, bytes(fragments), buffer


def _open(host: str, port: int, timeout: float) -> tuple[socket.socket, bytes]:
    sock = socket.create_connection((host, port), timeout=timeout)
    sock.settimeout(timeout)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    request = (
        "GET / HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "\r\n"
    )
    sock.sendall(request.encode("ascii"))
    buffer = b""
    deadline = time.monotonic() + timeout
    while b"\r\n\r\n" not in buffer:
        if time.monotonic() > deadline:
            raise socket.timeout("handshake deadline")
        sock.settimeout(max(0.01, deadline - time.monotonic()))
        chunk = sock.recv(4096)
        if not chunk:
            raise EOFError("handshake eof")
        buffer += chunk
        if len(buffer) > 65536:
            raise ValueError("handshake headers too large")
    head, buffer = buffer.split(b"\r\n\r\n", 1)
    lines = head.split(b"\r\n")
    if not lines[0].startswith(b"HTTP/1.1 101"):
        raise ValueError(f"handshake rejected: {lines[0]!r}")
    headers = {}
    for line in lines[1:]:
        name, sep, value = line.partition(b":")
        if sep:
            headers[name.strip().lower()] = value.strip()
    expected = base64.b64encode(hashlib.sha1(key.encode("ascii") + GUID).digest())
    if headers.get(b"sec-websocket-accept") != expected:
        raise ValueError("bad Sec-WebSocket-Accept")
    return sock, buffer


def put_pixel(
    port: int,
    x: int,
    y: int,
    color: str,
    timeout: float = 4.0,
    host: str = "127.0.0.1",
    raw_limit: int = 8,
) -> dict:
    """Attempt one browser PUT and return the client-visible result.

    result:
      ``ack``            matched pixel echo (quorum succeeded, broadcast seen)
      ``nak``            application error frame (e.g. write quorum timeout)
      ``timeout``        no ack/nak before the client deadline
      ``closed``         server closed the WebSocket/TCP first
      ``connect_error``  TCP connect/handshake failed
    """
    started = time.monotonic()
    request = {"op": "put", "x": int(x), "y": int(y), "color": normalize_color(color)}
    wanted_color = request["color"]
    raw_messages: list[dict] = []
    result = {
        "ok": False,
        "result": "connect_error",
        "reason": "",
        "latency_ms": None,
        "total_ms": None,
        "response": None,
        "messages": raw_messages,
        "request": request,
    }
    sock = None
    try:
        sock, buffer = _open(host, port, timeout)
        _send_frame(sock, 0x1, json.dumps(request, separators=(",", ":")).encode("utf-8"))
        sent = time.monotonic()
        deadline = sent + timeout
        while True:
            opcode, payload, buffer = _read_message(sock, buffer, deadline)
            if opcode != 0x1:
                continue
            try:
                message = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(message, dict):
                continue
            if len(raw_messages) < raw_limit:
                raw_messages.append(message)
            kind = str(message.get("op", ""))
            if kind == "pixel":
                try:
                    matches = (
                        int(message.get("x")) == int(x)
                        and int(message.get("y")) == int(y)
                        and normalize_color(message.get("color")) == wanted_color
                    )
                except (TypeError, ValueError):
                    matches = False
                if matches:
                    result.update(
                        ok=True,
                        result="ack",
                        reason="pixel_echo",
                        latency_ms=round((time.monotonic() - sent) * 1000, 3),
                        response=message,
                    )
                    return result
                continue
            if kind == "error":
                result.update(
                    result="nak",
                    reason="error:" + str(message.get("message", ""))[:160],
                    latency_ms=round((time.monotonic() - sent) * 1000, 3),
                    response=message,
                )
                return result
    except socket.timeout:
        result.update(result="timeout", reason="ack_timeout")
    except EOFError as exc:
        result.update(result="closed", reason=str(exc))
    except (OSError, ValueError) as exc:
        result.update(result="connect_error", reason=f"{type(exc).__name__}:{exc}"[:160])
    finally:
        result["total_ms"] = round((time.monotonic() - started) * 1000, 3)
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
    return result


def get_canvas(
    port: int,
    timeout: float = 25.0,
    host: str = "127.0.0.1",
) -> dict:
    """Request a full canvas snapshot and return {pixels: {(x,y): "#RRGGBB"}}.

    The server streams ``canvas_begin``, ``canvas_chunk`` frames and
    ``canvas_end``.  ``complete`` is False if an error frame arrived, the
    deadline expired, or ``canvas_end`` was never seen.
    """
    started = time.monotonic()
    pixels: dict[tuple[int, int], str] = {}
    errors: list[str] = []
    result = {
        "ok": False,
        "complete": False,
        "pixels": pixels,
        "pixel_count": 0,
        "errors": errors,
        "latency_ms": None,
        "reason": "",
    }
    sock = None
    try:
        sock, buffer = _open(host, port, timeout)
        _send_frame(sock, 0x1, b'{"op":"get_canvas"}')
        sent = time.monotonic()
        deadline = sent + timeout
        while True:
            opcode, payload, buffer = _read_message(sock, buffer, deadline)
            if opcode != 0x1:
                continue
            try:
                message = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(message, dict):
                continue
            kind = str(message.get("op", ""))
            if kind == "canvas_begin":
                pixels.clear()
                errors.clear()
            elif kind == "canvas_chunk":
                entries = message.get("pixels")
                if isinstance(entries, list):
                    for entry in entries:
                        if not isinstance(entry, dict):
                            continue
                        try:
                            pixels[(int(entry["x"]), int(entry["y"]))] = normalize_color(entry["color"])
                        except (KeyError, TypeError, ValueError):
                            continue
            elif kind == "pixel":
                try:
                    pixels[(int(message["x"]), int(message["y"]))] = normalize_color(message["color"])
                except (KeyError, TypeError, ValueError):
                    continue
            elif kind == "error":
                errors.append(str(message.get("message", ""))[:160])
            elif kind == "canvas_end":
                result.update(
                    ok=not errors,
                    complete=not errors,
                    latency_ms=round((time.monotonic() - sent) * 1000, 3),
                    reason="" if not errors else ";".join(errors[:3]),
                )
                result["pixel_count"] = len(pixels)
                return result
    except socket.timeout:
        result["reason"] = "snapshot_timeout"
    except EOFError as exc:
        result["reason"] = str(exc)
    except (OSError, ValueError) as exc:
        result["reason"] = f"{type(exc).__name__}:{exc}"[:160]
    finally:
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        result["pixel_count"] = len(pixels)
        if result["latency_ms"] is None:
            result["latency_ms"] = round((time.monotonic() - started) * 1000, 3)
    return result


__all__ = ["put_pixel", "get_canvas", "normalize_color"]
