# P2Place — API Reference {#mainpage}

**P2Place** is a Dynamo-inspired decentralized collaborative pixel canvas
(r/place style) built in C++20 for TCSS 558 Applied Distributed Computing.

## Current state — Labs 1–3 implemented

- **Lab 1** — RAII move-only `Socket`, 2-byte big-endian length framing
  (`prefix_message` / `sendFramed`), synchronous `Logger`.
- **Lab 2** — single-threaded edge-triggered epoll `EventLoop` (the
  "gomoku pattern"), `TcpServer` / `TcpClient` / `TcpConnection`,
  full-mesh HELLO/PING/PONG heartbeats, `PeerTable` membership.
- **Lab 3** — `VersionedPixel` per-pixel last-write-wins canvas
  (`Region`, `RegionStore`), `LamportClock`, browser painting UI over a
  minimal RFC 6455 `WebSocketCodec` / `WebSocketServer`.

Labs 4–9 (consistent hashing, quorum replication, failure detection,
recovery, metrics, Docker experiments) are planned but not yet
implemented.

## Module map

| Module     | Contents |
|------------|----------|
| `core`     | Canvas data model: `VersionedPixel`, `Region`, `RegionStore`, `PeerTable`, `NodeInfo`, `LamportClock` |
| `messages` | 6-byte binary envelope: `Message`, `Type`, `encode` / `decode` |
| `net`      | `EventLoop`, `Socket`, `TcpServer`, `TcpClient`, `TcpConnection`, `FrameCodec` → `ProtocolCodec` / `WebSocketCodec`, 2-byte framing |
| `peer`     | `PeerNode` (composition root), `Config`, `main` |
| `ui`       | `WebSocketServer` — browser channel, JSON commands |
| `util`     | `Logger`, SHA-1, Base64 |

## Channels

| Channel               | Codec             | Frame cap | Payload |
|-----------------------|-------------------|-----------|---------|
| peer↔peer TCP :9000   | `ProtocolCodec`   | 16 KiB    | binary 6-byte envelope |
| browser↔peer WS :9001 | `WebSocketCodec`  | 16 KiB    | JSON text |

## Build

```sh
cmake -S . -B build && cmake --build build -j && ctest --test-dir build
```

Generated with Doxygen from `include/` and `src/`.
