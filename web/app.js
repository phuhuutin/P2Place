/*
 * P2Place Lab 03 — browser client (r/place-style viewport).
 *
 * Plain WebSocket client: no frameworks, no build step, no localStorage.
 * The C++ peer's in-memory RegionStore is authoritative; this page only
 * paints what the server has accepted.
 *
 * Browser -> server:
 *   {"op":"get_canvas"}                  request chunked snapshot
 *   {"op":"put","x":n,"y":n,"color":"#RRGGBB"}
 * Server -> browser:
 *   {"op":"canvas_begin"}
 *   {"op":"canvas_chunk","pixels":[{"x","y","color"},...]}
 *   {"op":"canvas_end"}
 *   {"op":"pixel","x","y","color"}       accepted write, broadcast
 *   {"op":"error","message":"..."}
 * Legacy form (if an older server build is running):
 *   {"op":"canvas","pixels":[...]}
 *
 * Interaction (r/place conventions):
 *   click / tap ........ place one pixel
 *   drag ............... pan the canvas
 *   Shift+drag ......... paint a stroke (line interpolation, no gaps)
 *   wheel / pinch ...... zoom (cursor-anchored)
 *   middle / right drag  pan
 *   double-click ....... zoom in
 *   +/-/0 keys ......... zoom in / out / fit
 */
(() => {
  'use strict';

  // ---------- DOM ----------
  const viewport = document.getElementById('viewport');
  const canvas = document.getElementById('canvas');
  const ctx = canvas.getContext('2d');
  const colorInput = document.getElementById('color');
  const paletteEl = document.getElementById('palette');
  const refreshButton = document.getElementById('refresh');
  const statusEl = document.getElementById('status');
  const coordsEl = document.getElementById('coordinates');
  const responseEl = document.getElementById('response');
  const zoomInBtn = document.getElementById('zoom-in');
  const zoomOutBtn = document.getElementById('zoom-out');
  const zoomFitBtn = document.getElementById('zoom-fit');
  const zoomLabel = document.getElementById('zoom-label');

  // Must match core::RegionStore::CANVAS_SIDE on the server.
  const CANVAS_SIDE = 256;
  const MAX_ZOOM = 32; // one logical pixel = up to 32 screen px

  ctx.imageSmoothingEnabled = false;

  // ---------- Diagnostics log ----------
  const MAX_LOG_LINES = 200;

  function log(text) {
    const line = new Date().toLocaleTimeString() + '  ' + text;
    responseEl.textContent = responseEl.textContent
      ? responseEl.textContent + '\n' + line
      : line;
    const lines = responseEl.textContent.split('\n');
    if (lines.length > MAX_LOG_LINES) {
      responseEl.textContent = lines.slice(-MAX_LOG_LINES).join('\n');
    }
    responseEl.scrollTop = responseEl.scrollHeight;
  }

  let lastThrottledLog = 0;
  function logThrottled(text) {
    const now = Date.now();
    if (now - lastThrottledLog > 2000) {
      lastThrottledLog = now;
      log(text);
    }
  }

  // ---------- Status display (text carries the state) ----------
  const STATUS_LABELS = {
    connecting: 'Connecting',
    connected: 'Connected',
    reconnecting: 'Reconnecting',
    disconnected: 'Disconnected',
  };

  function setStatus(state, suffix) {
    statusEl.textContent =
      (STATUS_LABELS[state] || state) + (suffix ? ' ' + suffix : '');
    statusEl.className = 'status ' + state;
  }

  // ---------- WebSocket URL resolution ----------
  // Default: ws://<page-hostname>:9001
  //   ?ws=9001        port override on the page's hostname
  //   ?ws_url=ws://...  full URL override (ws:// or wss:// only)
  function resolveWsUrl() {
    const params = new URLSearchParams(window.location.search);

    const explicit = params.get('ws_url');
    if (explicit !== null) {
      let url = null;
      try {
        url = new URL(explicit);
      } catch (_) {
        /* fall through to default */
      }
      if (
        url &&
        (url.protocol === 'ws:' || url.protocol === 'wss:') &&
        url.host
      ) {
        return url.toString();
      }
      log(
        'Invalid ?ws_url=' + explicit +
        ' (need ws://host[:port] or wss://host[:port]) — using default'
      );
    }

    const port = params.get('ws');
    if (port !== null) {
      const n = Number(port);
      if (Number.isInteger(n) && n >= 1 && n <= 65535) {
        return 'ws://' + window.location.hostname + ':' + n;
      }
      log('Invalid ?ws=' + port + ' (expected 1-65535) — using default');
    }

    return 'ws://' + window.location.hostname + ':9001';
  }

  // ---------- Connection state ----------
  let socket = null;
  let reconnectTimer = null;
  let reconnectDelayMs = 1000;
  const MAX_RECONNECT_DELAY_MS = 10000;
  let shutdown = false; // page unloading: never reconnect again

  // Snapshot state (chunked get_canvas response in progress).
  let snapshotInProgress = false;
  let snapshotPixelCount = 0;
  let pendingPixels = []; // live broadcasts buffered during a snapshot

  function requestSnapshot() {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    socket.send(JSON.stringify({ op: 'get_canvas' }));
    return true;
  }

  function connect() {
    if (shutdown) return;
    // Never stack WebSocket instances or reconnect timers.
    if (
      socket &&
      (socket.readyState === WebSocket.CONNECTING ||
        socket.readyState === WebSocket.OPEN)
    ) {
      return;
    }

    setStatus('connecting');
    refreshButton.disabled = true;

    let ws;
    try {
      ws = new WebSocket(resolveWsUrl());
    } catch (err) {
      log('WebSocket constructor failed: ' + err);
      scheduleReconnect();
      return;
    }
    socket = ws;

    ws.addEventListener('open', () => {
      if (ws !== socket) return; // stale instance from a previous attempt
      reconnectDelayMs = 1000; // reset backoff after a successful open
      setStatus('connected');
      log('Connected to ' + ws.url);
      refreshButton.disabled = false;
      requestSnapshot(); // restore server-side pixels on every (re)connect
    });

    ws.addEventListener('message', (event) => {
      if (ws !== socket) return;
      handleMessage(String(event.data));
    });

    ws.addEventListener('close', (event) => {
      if (ws !== socket) return;
      socket = null;
      refreshButton.disabled = true;
      endGesture(); // pointer capture state is meaningless now
      snapshotInProgress = false;
      pendingPixels = []; // partial snapshots are stale; reconnect refetches
      setStatus('disconnected');
      log('Connection closed (code=' + event.code + ')');
      scheduleReconnect();
    });

    ws.addEventListener('error', () => {
      // Log only: the close event that follows owns reconnect scheduling.
      log('WebSocket error (close event will follow)');
    });
  }

  function scheduleReconnect() {
    if (shutdown) return;
    if (reconnectTimer !== null) return; // single timer, no accumulation
    setStatus('reconnecting', 'in ' + reconnectDelayMs / 1000 + 's');
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      const delay = reconnectDelayMs;
      reconnectDelayMs = Math.min(delay * 2, MAX_RECONNECT_DELAY_MS);
      connect();
    }, reconnectDelayMs);
  }

  // ---------- Message handling ----------
  function handleMessage(raw) {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch (_) {
      log('Ignored malformed server JSON');
      return;
    }
    if (msg === null || typeof msg !== 'object' || Array.isArray(msg)) {
      return;
    }
    if (typeof msg.op !== 'string') return;

    switch (msg.op) {
      case 'canvas_begin':
        snapshotInProgress = true;
        snapshotPixelCount = 0;
        pendingPixels = [];
        paintBackground();
        break;

      case 'canvas_chunk': {
        if (!Array.isArray(msg.pixels)) {
          log('Ignored canvas_chunk without a pixels array');
          return;
        }
        for (const entry of msg.pixels) {
          const p = validPixel(entry);
          if (p) {
            paintPixel(p.x, p.y, p.color);
            ++snapshotPixelCount;
          }
        }
        break;
      }

      case 'canvas_end':
        snapshotInProgress = false;
        log('Snapshot complete: ' + snapshotPixelCount + ' pixels restored');
        // Replay live writes that arrived mid-snapshot so stale snapshot
        // pixels cannot overwrite them. Ordering is safe: all frames for
        // this client arrive in server-send order on one socket.
        for (const p of pendingPixels) {
          paintPixel(p.x, p.y, p.color);
        }
        pendingPixels = [];
        break;

      case 'pixel': {
        const p = validPixel(msg);
        if (!p) {
          log('Ignored malformed pixel broadcast');
          return;
        }
        if (snapshotInProgress) {
          pendingPixels.push(p);
        } else {
          paintPixel(p.x, p.y, p.color);
        }
        break;
      }

      // Legacy dense snapshot from older server builds (kept for
      // compatibility; chunked form above is the current protocol).
      case 'canvas': {
        if (!Array.isArray(msg.pixels)) {
          log('Ignored legacy canvas without a pixels array');
          return;
        }
        paintBackground();
        let applied = 0;
        for (const entry of msg.pixels) {
          const p = validPixel(entry);
          if (p) {
            paintPixel(p.x, p.y, p.color);
            ++applied;
          }
        }
        log('Legacy snapshot applied: ' + applied + ' pixels');
        break;
      }

      case 'error':
        log('Server error: ' + String(msg.message));
        break;

      case 'status':
      case 'metrics':
        try {
          log('Server ' + msg.op + ': ' + JSON.stringify(msg));
        } catch (_) {
          /* never crash on diagnostics */
        }
        break;

      default:
        log('Ignored unknown op "' + msg.op + '"');
    }
  }

  // Validate one pixel entry; return {x, y, color} or null.
  function validPixel(entry) {
    if (entry === null || typeof entry !== 'object') return null;
    const x = entry.x;
    const y = entry.y;
    if (!Number.isInteger(x) || !Number.isInteger(y)) return null;
    if (x < 0 || x >= CANVAS_SIDE || y < 0 || y >= CANVAS_SIDE) return null;
    const color = pixelColor(entry.color);
    if (color === null) return null;
    return { x, y, color };
  }

  // Accept "#RRGGBB" strings and optional numeric 0xRRGGBB. Returns a
  // normalized uppercase "#RRGGBB" or null. Server strings are never
  // trusted as arbitrary fillStyle input.
  function pixelColor(value) {
    if (typeof value === 'string') {
      const match = /^#([0-9a-fA-F]{6})$/.exec(value);
      if (!match) return null;
      return '#' + match[1].toUpperCase();
    }
    if (
      typeof value === 'number' &&
      Number.isInteger(value) &&
      value >= 0 &&
      value <= 0xffffff
    ) {
      return '#' + value.toString(16).padStart(6, '0').toUpperCase();
    }
    return null;
  }

  // ---------- Canvas painting (authoritative display only) ----------
  function paintBackground() {
    ctx.fillStyle = '#FFFFFF';
    ctx.fillRect(0, 0, CANVAS_SIDE, CANVAS_SIDE);
  }

  function paintPixel(x, y, color) {
    ctx.fillStyle = color;
    ctx.fillRect(x, y, 1, 1);
  }

  // ---------- View: zoom & pan (transform-based, canvas bitmap untouched) ----------
  let zoom = 1;
  let minZoom = 1;
  const pan = { x: 0, y: 0 };

  function viewportSize() {
    return { w: viewport.clientWidth, h: viewport.clientHeight };
  }

  function fitZoomValue() {
    const { w, h } = viewportSize();
    const pad = 16;
    return Math.max(
      0.5,
      Math.min((w - pad * 2) / CANVAS_SIDE, (h - pad * 2) / CANVAS_SIDE)
    );
  }

  // Keep the canvas covering the viewport; center it when fully visible.
  function clampPan(p) {
    const { w, h } = viewportSize();
    const cw = CANVAS_SIDE * zoom;
    const ch = CANVAS_SIDE * zoom;
    p.x = cw <= w ? (w - cw) / 2 : Math.min(0, Math.max(w - cw, p.x));
    p.y = ch <= h ? (ch - h) / 2 : Math.min(0, Math.max(h - ch, p.y));
  }

  function applyTransform() {
    canvas.style.transform =
      'translate(' + pan.x + 'px, ' + pan.y + 'px) scale(' + zoom + ')';
    zoomLabel.textContent =
      '×' + (zoom < 10 ? zoom.toFixed(1) : zoom.toFixed(0));
  }

  // Zoom keeping the logical point under (anchorX, anchorY) fixed on screen.
  function setZoom(newZoom, anchorX, anchorY) {
    const next = Math.max(minZoom, Math.min(MAX_ZOOM, newZoom));
    if (anchorX !== undefined && anchorY !== undefined && next !== zoom) {
      const rect = canvas.getBoundingClientRect();
      const lx = (anchorX - rect.left) / zoom;
      const ly = (anchorY - rect.top) / zoom;
      zoom = next;
      pan.x = anchorX - lx * zoom;
      pan.y = anchorY - ly * zoom;
    } else {
      zoom = next;
    }
    clampPan(pan);
    applyTransform();
  }

  function fitToView() {
    zoom = fitZoomValue();
    minZoom = zoom;
    clampPan(pan);
    applyTransform();
  }

  // ---------- Coordinate mapping ----------
  // Transformed display coordinates -> logical canvas pixels.
  function toLogical(event) {
    const rect = canvas.getBoundingClientRect();
    if (rect.width <= 0 || rect.height <= 0) return null;
    const x = Math.floor(
      (event.clientX - rect.left) * (canvas.width / rect.width)
    );
    const y = Math.floor(
      (event.clientY - rect.top) * (canvas.height / rect.height)
    );
    if (x < 0 || x >= CANVAS_SIDE || y < 0 || y >= CANVAS_SIDE) return null;
    return { x, y };
  }

  // ---------- Sending puts ----------
  function sendPut(x, y, color) {
    if (!socket || socket.readyState !== WebSocket.OPEN) return false;
    const normalized = pixelColor(color);
    if (normalized === null) return false;
    socket.send(
      JSON.stringify({ op: 'put', x: x, y: y, color: normalized })
    );
    return true;
  }

  // ---------- Strokes (Shift+drag paint) ----------
  let paintLast = null; // last logical pixel painted in the current stroke

  // Paint the raster line from paintLast to point (endpoints included,
  // consecutive duplicates skipped). Step count is at most 255 on a
  // 256x256 canvas, so this is naturally bounded without an arbitrary cap.
  function paintStrokeTo(point) {
    if (!point) return;
    if (paintLast && (paintLast.x !== point.x || paintLast.y !== point.y)) {
      const dx = point.x - paintLast.x;
      const dy = point.y - paintLast.y;
      const steps = Math.max(Math.abs(dx), Math.abs(dy));
      if (steps > 1) {
        for (let i = 1; i < steps; ++i) {
          const x = paintLast.x + Math.round((dx * i) / steps);
          const y = paintLast.y + Math.round((dy * i) / steps);
          sendPut(x, y, colorInput.value);
        }
      }
      sendPut(point.x, point.y, colorInput.value);
    } else if (!paintLast) {
      sendPut(point.x, point.y, colorInput.value);
    }
    paintLast = { x: point.x, y: point.y };
  }

  function beginStrokeAt(point) {
    paintLast = null;
    paintStrokeTo(point);
  }

  function endStroke() {
    paintLast = null;
  }

  // ---------- Gestures: click-to-place, drag-to-pan, Shift+drag-to-paint, pinch ----------
  const PAN_THRESHOLD = 5; // screen px before a press becomes a pan
  const pointers = new Map(); // active pointers, for pinch tracking
  let gesture = null; // { id, mode: null|'pan'|'paint', lastX, lastY, originX, originY }
  let pinch = null; // { dist, zoom, rect, mid }

  function placePixel(event) {
    const p = toLogical(event);
    if (!p) return;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      logThrottled('Not connected — pixel not sent');
      return;
    }
    sendPut(p.x, p.y, colorInput.value);
  }

  function onPointerDown(event) {
    const isMouse = event.pointerType === 'mouse';
    if (isMouse && event.button !== 0 && event.button !== 1 && event.button !== 2) {
      return;
    }

    pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });

    if (pointers.size === 2) {
      // Second finger: switch to pinch; abandon the first-finger gesture.
      if (gesture) {
        endStroke();
        gesture = null;
        canvas.classList.remove('panning');
      }
      const pts = [...pointers.values()];
      const rect = canvas.getBoundingClientRect();
      pinch = {
        dist: Math.hypot(pts[1].x - pts[0].x, pts[1].y - pts[0].y),
        zoom,
        rect: { left: rect.left, top: rect.top },
        mid: {
          x: (pts[0].x + pts[1].x) / 2,
          y: (pts[0].y + pts[1].y) / 2,
        },
      };
      return;
    }

    try {
      viewport.setPointerCapture(event.pointerId);
    } catch (_) {
      /* capture is best-effort */
    }

    gesture = {
      id: event.pointerId,
      mode:
        isMouse && (event.button === 1 || event.button === 2)
          ? 'pan' // middle / right drag always pans
          : event.shiftKey
            ? 'paint' // Shift+drag paints a stroke
            : null, // left click / tap: place on release (no drag)
      lastX: event.clientX,
      lastY: event.clientY,
      originX: event.clientX,
      originY: event.clientY,
    };

    if (gesture.mode === 'paint') {
      beginStrokeAt(toLogical(event));
      if (!socket || socket.readyState !== WebSocket.OPEN) {
        logThrottled('Not connected — pixel not sent');
      }
    }

    event.preventDefault();
  }

  function onPointerMove(event) {
    updateCoords(event);

    const held = pointers.get(event.pointerId);
    if (held) {
      held.x = event.clientX;
      held.y = event.clientY;
    }

    // Pinch: zoom anchored at the moving midpoint (which also pans).
    if (pinch && pointers.size === 2) {
      const pts = [...pointers.values()];
      const dist = Math.hypot(pts[1].x - pts[0].x, pts[1].y - pts[0].y);
      const mid = {
        x: (pts[0].x + pts[1].x) / 2,
        y: (pts[0].y + pts[1].y) / 2,
      };
      if (dist > 0 && pinch.dist > 0) {
        const nextZoom = Math.max(
          minZoom,
          Math.min(MAX_ZOOM, pinch.zoom * (dist / pinch.dist))
        );
        const lx = (pinch.mid.x - pinch.rect.left) / pinch.zoom;
        const ly = (pinch.mid.y - pinch.rect.top) / pinch.zoom;
        zoom = nextZoom;
        pan.x = mid.x - lx * zoom;
        pan.y = mid.y - ly * zoom;
        clampPan(pan);
        applyTransform();
      }
      event.preventDefault();
      return;
    }

    if (!gesture || gesture.id !== event.pointerId) return;

    const dx = event.clientX - gesture.originX;
    const dy = event.clientY - gesture.originY;

    if (gesture.mode === null) {
      if (Math.hypot(dx, dy) <= PAN_THRESHOLD) return; // still a click
      gesture.mode = 'pan';
      canvas.classList.add('panning');
    }

    if (gesture.mode === 'pan') {
      pan.x += event.clientX - gesture.lastX;
      pan.y += event.clientY - gesture.lastY;
      clampPan(pan);
      applyTransform();
    } else if (gesture.mode === 'paint') {
      paintStrokeTo(toLogical(event));
    }

    gesture.lastX = event.clientX;
    gesture.lastY = event.clientY;
    event.preventDefault();
  }

  function onPointerUp(event) {
    pointers.delete(event.pointerId);
    if (pinch && pointers.size < 2) pinch = null;

    if (gesture && gesture.id === event.pointerId) {
      const g = gesture;
      gesture = null;
      canvas.classList.remove('panning');
      if (g.mode === 'paint') {
        endStroke();
      } else if (g.mode === null) {
        placePixel(event); // click / tap: no drag beyond the threshold
      }
    }
    updateCoords(event);
  }

  function onPointerCancel(event) {
    pointers.delete(event.pointerId);
    if (pinch && pointers.size < 2) pinch = null;
    if (gesture && gesture.id === event.pointerId) {
      gesture = null;
      endStroke();
      canvas.classList.remove('panning');
    }
  }

  function endGesture() {
    pointers.clear();
    pinch = null;
    gesture = null;
    endStroke();
    canvas.classList.remove('panning');
  }

  function updateCoords(event) {
    const p = toLogical(event);
    coordsEl.textContent = p ? p.x + ', ' + p.y : '—';
  }

  viewport.addEventListener('pointerdown', onPointerDown);
  viewport.addEventListener('pointermove', onPointerMove);
  viewport.addEventListener('pointerup', onPointerUp);
  viewport.addEventListener('pointercancel', onPointerCancel);
  viewport.addEventListener('lostpointercapture', onPointerCancel);
  viewport.addEventListener('contextmenu', (event) => event.preventDefault());
  viewport.addEventListener('pointerleave', () => {
    if (pointers.size === 0) coordsEl.textContent = '—';
  });

  // Wheel zoom, anchored at the cursor.
  viewport.addEventListener(
    'wheel',
    (event) => {
      event.preventDefault();
      const factor = Math.pow(1.0015, -event.deltaY);
      setZoom(zoom * factor, event.clientX, event.clientY);
    },
    { passive: false }
  );

  viewport.addEventListener('dblclick', (event) => {
    setZoom(zoom * 1.5, event.clientX, event.clientY);
  });

  // ---------- Zoom controls ----------
  function zoomFromCenter(factor) {
    const rect = viewport.getBoundingClientRect();
    setZoom(zoom * factor, rect.left + rect.width / 2, rect.top + rect.height / 2);
  }

  zoomInBtn.addEventListener('click', () => zoomFromCenter(1.5));
  zoomOutBtn.addEventListener('click', () => zoomFromCenter(1 / 1.5));
  zoomFitBtn.addEventListener('click', fitToView);

  window.addEventListener('keydown', (event) => {
    const tag = event.target && event.target.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'BUTTON' || tag === 'SELECT') {
      return;
    }
    if (event.key === '+' || event.key === '=') zoomFromCenter(1.5);
    else if (event.key === '-' || event.key === '_') zoomFromCenter(1 / 1.5);
    else if (event.key === '0') fitToView();
  });

  window.addEventListener('resize', () => {
    const fit = fitZoomValue();
    minZoom = fit;
    if (zoom < fit) zoom = fit;
    clampPan(pan);
    applyTransform();
  });

  // ---------- Color palette ----------
  const PALETTE = [
    '#367f63', // P2Place default
    '#6D001A', '#BE0039', '#FF4500', '#FFA800', '#FFD635', '#FFF8B8',
    '#00A368', '#00CC78', '#7EED56', '#00756F', '#009EAA', '#00CCC0',
    '#2450A4', '#3690EA', '#51E9F4', '#493AC1', '#6A5CFF', '#811E9F',
    '#B44AC0', '#FF3881', '#FF99AA', '#9C6926', '#898D90', '#D4D7D9',
    '#FFFFFF', '#000000',
  ];

  const swatches = new Map();
  for (const color of PALETTE) {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'swatch';
    btn.title = color;
    btn.style.background = color;
    btn.setAttribute('aria-label', 'Select color ' + color);
    btn.addEventListener('click', () => setColor(color));
    paletteEl.appendChild(btn);
    swatches.set(color.toLowerCase(), btn);
  }

  function setColor(color) {
    colorInput.value = color;
    syncSwatchSelection();
  }

  function syncSwatchSelection() {
    const current = colorInput.value.toLowerCase();
    for (const [color, btn] of swatches) {
      const selected = color === current;
      btn.classList.toggle('selected', selected);
      btn.setAttribute('aria-pressed', String(selected));
    }
  }

  colorInput.addEventListener('input', syncSwatchSelection);
  syncSwatchSelection();

  // ---------- Refresh button ----------
  refreshButton.addEventListener('click', () => {
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    if (requestSnapshot()) log('Snapshot requested');
  });

  // ---------- Shutdown ----------
  window.addEventListener('pagehide', () => {
    shutdown = true;
    if (reconnectTimer !== null) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (socket) {
      try {
        socket.close();
      } catch (_) {
        /* already closed */
      }
    }
  });

  // ---------- Startup ----------
  paintBackground();
  fitToView();
  setStatus('connecting');
  log('P2Place client ready — target ' + resolveWsUrl());
  connect();

  // Small hook for reproducible manual/automated checks.
  window.p2place = {
    sendPut,
    requestSnapshot,
    setZoom,
    fitToView,
    getState: () => ({
      status: statusEl.textContent,
      snapshotInProgress,
      pendingCount: pendingPixels.length,
      socketState: socket ? socket.readyState : null,
      zoom,
      pan: { x: pan.x, y: pan.y },
      minZoom,
    }),
  };
})();
