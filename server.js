'use strict';
const http   = require('http');
const fs     = require('fs');
const path   = require('path');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');
const { version } = require('./package.json');

const PORT        = process.env.PORT || 3000;
const MAX_PAYLOAD = 4096;          // bytes per message
const RATE_MAX    = 30;            // messages per second per client
const ROOM_TTL    = 30 * 60_000;  // 30-minute room expiry

// ── HTTP server — serves index.html ─────────────────────────────────────────
const htmlFile = path.join(__dirname, 'index.html');
const server = http.createServer((req, res) => {
  if (req.method !== 'GET') { res.writeHead(405); res.end(); return; }
  const url = (req.url || '/').split('?')[0];
  if (url !== '/' && url !== '/index.html') { res.writeHead(404); res.end('Not found'); return; }
  fs.readFile(htmlFile, (err, data) => {
    if (err) { res.writeHead(500); res.end(); return; }
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'X-Content-Type-Options': 'nosniff',
      'X-Frame-Options': 'SAMEORIGIN',
      'Referrer-Policy': 'no-referrer',
      'Cache-Control': 'no-store',
      // Uncomment for production HTTPS deployments:
      // 'Strict-Transport-Security': 'max-age=31536000',
    });
    res.end(data.toString().replace('__VERSION__', version));
  });
});

// ── WebSocket server ─────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server, maxPayload: MAX_PAYLOAD });
const rooms = new Map(); // code → Room

// ── Helpers ──────────────────────────────────────────────────────────────────
const VALID_CELLS = new Set(['I','O','T','S','Z','J','L','G',null]);

function genCode() {
  let c;
  do { c = crypto.randomBytes(3).toString('hex').toUpperCase(); } while (rooms.has(c));
  return c;
}
function genToken() { return crypto.randomBytes(32).toString('hex'); }

function sanitize(s, max = 12) {
  if (typeof s !== 'string') return '';
  return s.replace(/[^\w\s-]/g, '').trim().slice(0, max);
}
function clamp(v, lo, hi) {
  const n = Math.floor(+v);
  return isNaN(n) ? lo : Math.max(lo, Math.min(hi, n));
}

function safeSend(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function bcast(room, obj, skipToken = null) {
  const d = JSON.stringify(obj);
  for (const p of room.players)
    if (p.token !== skipToken && p.ws.readyState === 1) p.ws.send(d);
}

function playerList(room) {
  return room.players.map(p => ({ slot: p.slot, name: p.name }));
}

// ── Connection handler ───────────────────────────────────────────────────────
wss.on('connection', (ws /*, req */) => {
  // For production: verify req.headers.origin against an allowed-origins list here.

  let player = null;
  let msgCount = 0, rlReset = Date.now() + 1000;

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    // ── Rate limiting ──────────────────────────────────────────────────────
    const now = Date.now();
    if (now > rlReset) { msgCount = 0; rlReset = now + 1000; }
    if (++msgCount > RATE_MAX) {
      safeSend(ws, { type: 'ERROR', msg: 'Rate limit exceeded' });
      return;
    }

    // ── Parse ──────────────────────────────────────────────────────────────
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.type !== 'string') return;

    // ── Pre-auth messages ──────────────────────────────────────────────────
    if (m.type === 'CREATE_ROOM') {
      if (player) return;
      const code  = genCode();
      const token = genToken();
      const name  = sanitize(m.name) || 'P1';
      player = { ws, token, slot: 1, name, score: 0, level: 1, lines: 0, dead: false, roomCode: code };
      rooms.set(code, { code, players: [player], started: false, lastActivity: now });
      safeSend(ws, { type: 'ROOM_CREATED', roomCode: code, slot: 1, token, name });
      return;
    }

    if (m.type === 'JOIN_ROOM') {
      if (player) return;
      const code = sanitize(String(m.roomCode ?? ''), 6).toUpperCase();
      const room = rooms.get(code);
      if (!room)            { safeSend(ws, { type: 'ERROR', msg: 'Room not found' }); return; }
      if (room.started)     { safeSend(ws, { type: 'ERROR', msg: 'Game already in progress' }); return; }
      if (room.players.length >= 4) { safeSend(ws, { type: 'ERROR', msg: 'Room is full (4/4)' }); return; }

      const slot  = room.players.length + 1;
      const token = genToken();
      const name  = sanitize(m.name) || `P${slot}`;
      player = { ws, token, slot, name, score: 0, level: 1, lines: 0, dead: false, roomCode: code };
      room.players.push(player);
      room.lastActivity = now;

      safeSend(ws, { type: 'ROOM_JOINED', slot, token, name, players: playerList(room) });
      bcast(room, { type: 'PLAYER_JOINED', slot, name }, token);
      return;
    }

    // ── Auth guard ─────────────────────────────────────────────────────────
    if (!player || m.token !== player.token) return;
    const room = rooms.get(player.roomCode);
    if (!room) return;
    room.lastActivity = now;

    switch (m.type) {

      case 'START_GAME':
        if (player.slot !== 1) { safeSend(ws, { type: 'ERROR', msg: 'Only the host can start' }); return; }
        if (room.started) return;
        if (room.players.length < 1) { safeSend(ws, { type: 'ERROR', msg: 'No players in room' }); return; }
        room.started = true;
        bcast(room, { type: 'GAME_STARTED', players: playerList(room) });
        return;

      case 'SCORE_UPDATE':
        if (!room.started) return;
        player.score = clamp(m.score, 0, 99_999_999);
        player.level = clamp(m.level, 1, 99);
        player.lines = clamp(m.lines, 0, 99_999);
        bcast(room, { type: 'SCORE_UPDATE', slot: player.slot,
          score: player.score, level: player.level, lines: player.lines }, player.token);
        return;

      case 'BOARD_UPDATE': {
        if (!room.started) return;
        const b = m.board;
        if (!Array.isArray(b) || b.length !== 20) return;
        for (const row of b) {
          if (!Array.isArray(row) || row.length !== 10) return;
          for (const cell of row) if (!VALID_CELLS.has(cell)) return;
        }
        bcast(room, {
          type: 'BOARD_UPDATE', slot: player.slot, board: b,
          cur: typeof m.cur === 'string' ? m.cur.slice(0, 1) : null,
          pos: (m.pos && typeof m.pos.x === 'number' && typeof m.pos.y === 'number')
            ? { x: clamp(m.pos.x, -5, 14), y: clamp(m.pos.y, -2, 19) } : null,
          rot: clamp(m.rot, 0, 3),
        }, player.token);
        return;
      }

      case 'GARBAGE': {
        if (!room.started) return;
        const amount     = clamp(m.amount, 1, 4);
        const targetSlot = clamp(m.targetSlot, 1, 4);
        const target = room.players.find(p => p.slot === targetSlot && !p.dead);
        if (target) safeSend(target.ws, { type: 'GARBAGE', amount, fromSlot: player.slot });
        return;
      }

      case 'ELIMINATED':
        if (player.dead) return;
        player.dead = true;
        bcast(room, { type: 'PLAYER_ELIMINATED', slot: player.slot, score: player.score });
        {
          const living = room.players.filter(p => !p.dead);
          if (living.length <= 1) endGame(room, living[0] ?? null);
        }
        return;

      case 'PING':
        safeSend(ws, { type: 'PONG' });
        return;
    }
  });

  ws.on('close', () => {
    if (!player) return;
    const room = rooms.get(player.roomCode);
    if (!room) return;
    room.players = room.players.filter(p => p.token !== player.token);
    if (room.players.length === 0) { rooms.delete(room.code); return; }
    bcast(room, { type: 'PLAYER_LEFT', slot: player.slot, name: player.name });
    if (room.started && !player.dead) {
      player.dead = true;
      const living = room.players.filter(p => !p.dead);
      if (living.length <= 1) endGame(room, living[0] ?? null);
    }
  });

  ws.on('error', () => {});
});

function endGame(room, winner) {
  const rankings = [...room.players]
    .sort((a, b) => b.score - a.score)
    .map((p, i) => ({ rank: i + 1, slot: p.slot, name: p.name,
      score: p.score, level: p.level, lines: p.lines }));
  bcast(room, { type: 'GAME_OVER', winnerSlot: winner?.slot ?? null, rankings });
  setTimeout(() => rooms.delete(room.code), 30_000);
}

// ── Heartbeat (detect dead connections) ─────────────────────────────────────
const hbInterval = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 30_000);
wss.on('close', () => clearInterval(hbInterval));

// ── Room TTL cleanup ─────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms)
    if (now - room.lastActivity > ROOM_TTL) rooms.delete(code);
}, 60_000);

// ── Start ────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`\n  NEON TETRIS — ONLINE SERVER`);
  console.log(`  ──────────────────────────────`);
  console.log(`  Local:   http://localhost:${PORT}`);
  console.log(`  WS:      ws://localhost:${PORT}`);
  console.log(`\n  Share the URL with all players (same network or use a tunnel).\n`);
});
