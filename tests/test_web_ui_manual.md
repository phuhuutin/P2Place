# P2Place Lab 03 — Manual Web UI Test Checklist

Run the server and static file host first (see steps 1-2), then walk the
checklist. Each item has an expected result.

## Setup

Terminal A (WSL):

```bash
cd ~/projects/p2place
./build/bin/p2place-peer --id 1 --port 9000 --ws-port 9001
```

Terminal B (WSL):

```bash
cd ~/projects/p2place
python3 -m http.server 8080 --directory web
```

In Windows Chrome: open `http://localhost:8080/?ws=9001`

(Equivalent overrides: `?ws=<port>` changes the WebSocket port;
`?ws_url=ws://host:port` sets the full URL. Both are validated and fall
back to `ws://<page-hostname>:9001`.)

## Checklist

| # | Action | Expected |
|---|--------|----------|
| 1 | Open the page | Title "P2Place"; status becomes **Connected**; canvas fits the viewport, centered; Console has no fatal JS errors. |
| 2 | Click a pixel | DevTools -> Network -> WS -> Messages shows `{"op":"put","x":…,"y":…,"color":"#RRGGBB"}`. |
| 3 | Wait for acceptance | After the server accepts, the clicked pixel is painted (server-sent `pixel` broadcast). |
| 4 | Open a second tab | It connects and restores all existing pixels via the chunked snapshot. |
| 5 | Draw in tab A | Tab B shows the same pixels via `pixel` broadcasts (no refresh needed). |
| 6 | Refresh tab B | Refresh Canvas re-requests `get_canvas`; pixels persist from C++ RAM. |
| 7 | Stop the peer (Ctrl-C in A) | Status becomes Disconnected, then Reconnecting; a single reconnect timer runs with growing backoff (1s -> 2s -> 4s -> … max 10s). Refresh is disabled while disconnected. |
| 8 | Restart the peer | Status becomes Connected and a fresh snapshot is requested. Canvas is blank (storage is RAM-only). |
| 9 | Plain drag (left or middle/right button) | The canvas pans under the cursor; no pixels are placed. Cursor becomes a grabbing hand during the pan. |
| 10 | Shift+drag quickly across the canvas | A stroke is painted in the selected color; the line has no gaps (client interpolates between pointer samples); no duplicate spam for a stationary pointer. |
| 11 | Hover (no button) | Coordinates readout shows the logical pixel under the cursor as `x, y` (0-255 each axis); `—` when outside the canvas. |
| 12 | Mouse wheel up / down | Canvas zooms in / out anchored at the cursor; the ×N label updates. Zoom never goes below "fit" and caps at ×32. |
| 13 | Pinch on a touch screen | Two-finger pinch zooms anchored between the fingers and pans with the midpoint. |
| 14 | Double-click | Zooms in one step at the click point. |
| 15 | `+` / `-` / `0` keys (when no input has focus) | Zoom in / zoom out / fit-to-viewport. |
| 16 | `−` `+` `⚓` buttons (bottom center) | Zoom out / zoom in / fit, centered on the viewport. |
| 17 | Zoom in far, then click a pixel | The click maps to the correct logical pixel (screen -> canvas coordinate mapping under transform); a single 1px cell is painted. |
| 18 | Send junk | Use a raw client (see below) to send malformed JSON / unknown op — the page logs it in Diagnostics and stays responsive; the server replies with `{"op":"error",…}` for unknown ops. |
| 19 | Color input | Clicking a palette swatch or changing the picker updates the next drawn pixels. Default is `#367f63`. |

## Raw-client protocol probe (no browser needed)

While the peer is running, exercise the server contract directly
(validates handshake/frames and the JSON protocol independently of the UI):

```bash
# In WSL, with the peer running:
python3 tests/test_websocket_manual.py   # handshake, ping/pong, close, unmasked rejection
```

Malformed-JSON probe with Node (Node >= 22 has a built-in WebSocket client):

```bash
node -e '
const ws = new WebSocket("ws://localhost:9001");
ws.onopen = () => {
  ws.send("{this is not json");
  ws.send(JSON.stringify({op: "bogus"}));
  ws.send(JSON.stringify({op: "put", x: -5, y: 0, color: "#FF0000"}));
  ws.send(JSON.stringify({op: "get_canvas"}));
};
ws.onmessage = (e) => console.log("<-", e.data.slice(0, 120));
ws.onclose = (e) => { console.log("closed", e.code); process.exit(0); };
setTimeout(() => process.exit(0), 3000);
'
```

Expected: `{"op":"error",...}` responses for the malformed JSON, the bogus
op, and the out-of-range put; then `canvas_begin` … `canvas_end`. No crash.

## Known Lab 03 limitations

- Snapshot streaming is bursty, not flow-controlled: `handle_get_canvas`
  queues every chunk (100 pixels per message) in one pass. A fully dense
  256x256 canvas is ~2.2 MB of queued output, which fits the 8 MiB
  per-connection output limit but is sent without pacing.
- No client-side persistence: refresh restores only what is in the peer's
  RAM (RegionStore), as intended.
- Lab 02 peer TCP (port 9000) and Lab 03 browser WebSocket (port 9001)
  are separate; browsers never replicate peers.
