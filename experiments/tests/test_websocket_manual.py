#!/usr/bin/env python3
"""P2Place Lab 03: manual WebSocket integration tests (Python standard library)."""

import base64
import hashlib
import os
import socket
import struct
import sys

HOST = "127.0.0.1"
PORT = 9001
TIMEOUT = 3.0
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class WebSocketClient:
    def __init__(self, host=HOST, port=PORT):
        self.host = host
        self.port = port
        self.sock = None
        self.buffer = b""

    def connect(self):
        self.sock = socket.create_connection((self.host, self.port), timeout=TIMEOUT)
        self.sock.settimeout(TIMEOUT)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            "GET / HTTP/1.1\r\n"
            f"Host: {self.host}:{self.port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "\r\n"
        )
        self.sock.sendall(request.encode("ascii"))

        while b"\r\n\r\n" not in self.buffer:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("Server disconnected during handshake")
            self.buffer += chunk
            if len(self.buffer) > 8192:
                raise ValueError("HTTP response headers exceed 8 KiB")

        headers_bytes, self.buffer = self.buffer.split(b"\r\n\r\n", 1)
        lines = headers_bytes.decode("ascii").split("\r\n")
        assert lines[0] == "HTTP/1.1 101 Switching Protocols", lines[0]
        headers = {}
        for line in lines[1:]:
            name, separator, value = line.partition(":")
            if separator:
                headers[name.strip().lower()] = value.strip()
        expected = base64.b64encode(
            hashlib.sha1((key + GUID).encode("ascii")).digest()
        ).decode("ascii")
        assert headers.get("sec-websocket-accept") == expected, "Incorrect accept key"
        assert headers.get("upgrade", "").lower() == "websocket"
        assert "upgrade" in [
            token.strip().lower()
            for token in headers.get("connection", "").split(",")
        ]
        print("PASS: HTTP WebSocket handshake")

    def recv_exact(self, size):
        while len(self.buffer) < size:
            chunk = self.sock.recv(max(4096, size - len(self.buffer)))
            if not chunk:
                raise ConnectionError("TCP connection closed")
            self.buffer += chunk
        result = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return result

    def send_frame(self, opcode, payload=b"", masked=True):
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        length = len(payload)
        mask_bit = 0x80 if masked else 0
        header = bytes([0x80 | opcode])
        if length < 126:
            header += bytes([mask_bit | length])
        elif length <= 65535:
            header += bytes([mask_bit | 126]) + struct.pack("!H", length)
        else:
            header += bytes([mask_bit | 127]) + struct.pack("!Q", length)

        if masked:
            mask = os.urandom(4)
            payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
            self.sock.sendall(header + mask + payload)
        else:
            self.sock.sendall(header + payload)

    def recv_frame(self):
        b0, b1 = self.recv_exact(2)
        assert b0 & 0x80, "Unexpected fragmented server frame"
        assert not (b1 & 0x80), "Server frame must not be masked"
        opcode = b0 & 0x0F
        length = b1 & 0x7F
        if length == 126:
            length = struct.unpack("!H", self.recv_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self.recv_exact(8))[0]
        return opcode, self.recv_exact(length)

    def wait_for_tcp_close(self):
        """Return when the server closes TCP; reject bytes after a Close response."""
        try:
            extra = self.recv_exact(1)
        except ConnectionError:
            return
        raise AssertionError(f"Unexpected bytes while waiting for TCP close: {extra!r}")

    def close(self):
        if self.sock is not None:
            self.sock.close()
            self.sock = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def test_handshake():
    with WebSocketClient():
        pass


def test_text_frame():
    with WebSocketClient() as ws:
        message = '{"op":"get_canvas"}'
        ws.send_frame(0x1, message)
        print("PASS: Masked text frame sent")
        print(f"      Verify the C++ server log received: {message}")
        print("      (Application response not required until Step 9)")


def test_ping_pong():
    with WebSocketClient() as ws:
        ws.send_frame(0x9, b"hello")
        print("Waiting for Pong response...")
        opcode, payload = ws.recv_frame()
        assert opcode == 0xA, f"Expected Pong (0xA); got {opcode:#x}"
        assert payload == b"hello", f"Unexpected Pong payload: {payload!r}"
        print("PASS: Ping/Pong")


def test_close_frame():
    with WebSocketClient() as ws:
        close_payload = struct.pack("!H", 1000)
        ws.send_frame(0x8, close_payload)
        print("Waiting for Close response...")
        try:
            opcode, payload = ws.recv_frame()
        except socket.timeout as exc:
            raise AssertionError("Server did not send a Close response within 3 seconds") from exc
        assert opcode == 0x8, f"Expected Close (0x8); got {opcode:#x}"
        assert payload == close_payload, f"Unexpected Close payload: {payload!r}"
        print("PASS: Close frame received")
        print("Waiting for TCP disconnect...")
        try:
            ws.wait_for_tcp_close()
        except socket.timeout as exc:
            raise AssertionError("Server sent Close frame but did not close TCP within 3 seconds") from exc
        print("PASS: TCP disconnected after Close")


def test_unmasked_frame_rejected():
    with WebSocketClient() as ws:
        ws.send_frame(0x1, b"{}", masked=False)
        print("Waiting for invalid-frame rejection...")
        try:
            opcode, payload = ws.recv_frame()
        except (ConnectionError, ConnectionResetError):
            print("PASS: Invalid unmasked frame rejected by TCP disconnect")
            return
        except socket.timeout as exc:
            raise AssertionError("Server did not reject unmasked frame within 3 seconds") from exc
        assert opcode == 0x8, f"Expected Close (0x8); got {opcode:#x}"
        print("PASS: Invalid unmasked frame rejected with Close frame")
        try:
            ws.wait_for_tcp_close()
        except (ConnectionResetError, ConnectionAbortedError):
            pass
        except socket.timeout as exc:
            raise AssertionError("Server sent Close but did not close TCP") from exc
        print("PASS: TCP disconnected after invalid frame")


def main():
    tests = [
        ("Handshake", test_handshake),
        ("Text frame", test_text_frame),
        ("Ping/Pong", test_ping_pong),
        ("Close", test_close_frame),
        ("Invalid unmasked frame", test_unmasked_frame_rejected),
    ]
    print(f"Testing P2Place WebSocket at {HOST}:{PORT}")
    print("-" * 48)
    failures = 0
    for name, test in tests:
        try:
            test()
        except Exception as exc:
            failures += 1
            print(f"FAIL: {name}: {type(exc).__name__}: {exc}")
    print("-" * 48)
    print(f"RESULT: {len(tests) - failures}/{len(tests)} tests passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
