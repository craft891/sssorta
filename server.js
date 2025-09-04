// server.js
// Robust orchestrator for Factor Visualizer cluster (optimized, defensive, logged)
//
// Features:
//  - atomic, debounced saves to state.json (safe + efficient)
//  - immediate save on critical events (found/progress/task-create/release)
//  - unique seed generation using crypto
//  - prevents duplicate task claims; returns tasks already claimed by requester first
//  - admin endpoints for inspection and maintenance
//  - detailed logging to console and found.log on successful factor discovery
//  - graceful shutdown (SIGINT/SIGTERM) with state flush
//
// Usage: node server.js
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const PORT = process.env.PORT || 3000;
const STATE_FILE = path.join(process.cwd(), 'state.json');
const TMP_STATE = STATE_FILE + '.tmp';
const FOUND_LOG = path.join(process.cwd(), 'found.log');

const CHECK_INTERVAL_MS = 3000;
const CLIENT_TIMEOUT_SEC = 20;
const SAVE_DEBOUNCE_MS = 600;
const MAX_SERVER_TASKS = 10000; // safety cap to avoid runaway creation

// ---------- Basic server state ----------
let state = {
  meta: { createdAt: new Date().toISOString(), version: 2 },
  N_original: null,    // canonical original N (string)
  N_work: null,        // N workers should factor (string)
  totalIterations: "0",// decimal string
  found: null,         // null or { p, q, foundBy, at }
  tasks: [],           // array of tasks {taskId,c,offset,claimedBy,lastUpdate,N}
  clients: {},         // clientId -> { lastSeen, capacity, meta }
  lastSaved: null
};

// ---------- Logging helpers ----------
function isoTs(){ return new Date().toISOString(); }
function log(...args){
  console.log(`[${isoTs()}]`, ...args);
}
function warn(...args){
  console.warn(`[${isoTs()}] WARN:`, ...args);
}
function errlog(...args){
  console.error(`[${isoTs()}] ERROR:`, ...args);
}
function appendFoundLog(entry){
  try{
    fs.appendFileSync(FOUND_LOG, entry + '\n', 'utf8');
  }catch(e){
    errlog('Failed to append to found.log', e);
  }
}

// ---------- Load / Save state (debounced + atomic) ----------
function safeParseJSON(s){
  try { return JSON.parse(s); } catch(e){ return null; }
}
function loadStateFromFile(){
  try{
    if(fs.existsSync(STATE_FILE)){
      const raw = fs.readFileSync(STATE_FILE, 'utf8');
      const parsed = safeParseJSON(raw);
      if(parsed && typeof parsed === 'object'){
        // migrate legacy N -> N_original
        if(parsed.N && !parsed.N_original) parsed.N_original = parsed.N;
        parsed.tasks = Array.isArray(parsed.tasks) ? parsed.tasks : [];
        // Merge parsed onto default state but preserve runtime fields
        state = Object.assign(state, parsed);
        log('Loaded state from', STATE_FILE);
      } else {
        warn('state.json exists but invalid JSON — starting fresh state');
      }
    } else {
      log('No state.json found; starting with fresh state');
    }
  }catch(e){
    errlog('Failed to load state.json:', e);
  }
}

let saveTimer = null;
let pendingSave = false;
function scheduleSave(debounceMs = SAVE_DEBOUNCE_MS){
  pendingSave = true;
  if(saveTimer) clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    _doSave();
    pendingSave = false;
    saveTimer = null;
  }, debounceMs);
}
function _doSave(){
  try{
    // ensure numeric values are strings
    if(typeof state.totalIterations !== 'string') state.totalIterations = String(state.totalIterations || '0');
    const json = JSON.stringify(state, null, 2);
    fs.writeFileSync(TMP_STATE, json, 'utf8');
    fs.renameSync(TMP_STATE, STATE_FILE);
    state.lastSaved = new Date().toISOString();
    log('State saved to', STATE_FILE);
  }catch(e){
    errlog('Failed to save state:', e);
  }
}
function saveImmediate(){
  if(saveTimer){ clearTimeout(saveTimer); saveTimer = null; }
  _doSave();
  pendingSave = false;
}

// ---------- Utilities ----------
function nowSec(){ return Math.floor(Date.now()/1000); }
function mkTaskId(){ return 't-' + crypto.randomBytes(6).toString('hex'); }
function uniqueSeed(existingSet){
  // try random numbers, and fallback to randomBytes hex if collision
  for(let i=0;i<64;i++){
    const candidate = String(Math.floor(Math.random()*1e9)+1);
    if(!existingSet.has(candidate)) return candidate;
  }
  // fallback: 8 bytes hex
  return crypto.randomBytes(8).toString('hex');
}
function existingSeedsSet(){
  const s = new Set();
  for(const t of state.tasks) s.add(String(t.c));
  return s;
}
function normalizeTask(t){
  // ensure required fields exist and are strings where expected
  return {
    taskId: String(t.taskId),
    c: String(t.c),
    offset: String(t.offset || '0'),
    claimedBy: t.claimedBy === null ? null : String(t.claimedBy),
    lastUpdate: Number(t.lastUpdate || nowSec()),
    N: t.N == null ? null : String(t.N)
  };
}

// ---------- Load existing state on startup ----------
loadStateFromFile();

// ensure tasks are normalized
state.tasks = state.tasks.map(normalizeTask);
if(state.N && !state.N_original) state.N_original = state.N;
if(state.N_original && !state.N_work) state.N_work = state.N_original;

// initial save to populate lastSaved if necessary
saveImmediate();

// ---------- Express + REST admin endpoints ----------
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(process.cwd())); // serve index.html if present

app.get('/state', (req,res) => { res.json(state); });
app.get('/download', (req,res) => {
  if(fs.existsSync(STATE_FILE)) return res.download(STATE_FILE);
  res.status(404).send('no state file');
});

// admin: list clients
app.get('/admin/clients', (req,res) => { res.json(state.clients); });

// admin: list tasks
app.get('/admin/tasks', (req,res) => { res.json(state.tasks); });

// admin: unclaim tasks for a client
app.post('/admin/unclaim/:clientId', (req,res) => {
  const cid = req.params.clientId;
  let changed = false;
  for(const t of state.tasks){
    if(t.claimedBy === cid){
      t.claimedBy = null;
      t.lastUpdate = nowSec();
      changed = true;
    }
  }
  if(changed) { scheduleSave(0); res.json({ ok:true, msg:'released tasks for ' + cid }); }
  else res.json({ ok:false, msg:'no tasks claimed by ' + cid });
});

// admin: clear tasks (dangerous)
app.post('/admin/clear-tasks', (req,res) => {
  const keepN = req.body && req.body.keepN;
  state.tasks = [];
  if(keepN && state.N_original) {
    // create single fresh task with unique seed
    const seed = uniqueSeed(new Set());
    state.tasks.push(normalizeTask({ taskId: mkTaskId(), c: seed, offset: "0", claimedBy: null, lastUpdate: nowSec(), N: state.N_work || state.N_original }));
  }
  scheduleSave(0);
  res.json({ ok:true, tasks: state.tasks.length });
});

// admin: create task(s)
app.post('/admin/create-tasks', (req,res) => {
  let count = Math.max(1, parseInt(req.body && req.body.count) || 1);
  const existing = existingSeedsSet();
  const created = [];
  // safety cap
  const totalAfter = state.tasks.length + count;
  if(totalAfter > MAX_SERVER_TASKS){
    count = Math.max(0, MAX_SERVER_TASKS - state.tasks.length);
    if(count === 0) return res.status(400).json({ ok:false, msg:'max tasks reached' });
  }
  for(let i=0;i<count;i++){
    const c = uniqueSeed(existing);
    existing.add(c);
    const t = normalizeTask({ taskId: mkTaskId(), c, offset: '0', claimedBy: null, lastUpdate: nowSec(), N: state.N_work || state.N_original || null });
    state.tasks.push(t);
    created.push(t);
  }
  scheduleSave(0);
  res.json({ ok:true, created });
});

// post a single task (compatible with previous API)
app.post('/task', (req,res) => {
  const { c, offset, N } = req.body || {};
  const existing = existingSeedsSet();
  const seed = c ? String(c) : uniqueSeed(existing);
  const task = normalizeTask({ taskId: mkTaskId(), c: seed, offset: String(offset || '0'), claimedBy: null, lastUpdate: nowSec(), N: N ? String(N) : (state.N_work || state.N_original || null) });
  state.tasks.push(task);
  scheduleSave(0);
  res.json({ ok:true, task });
});

// ---------- HTTP server + WebSocket ----------
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Broadcast helper
function broadcastToClients(obj){
  const s = JSON.stringify(obj);
  wss.clients.forEach(c => { if(c.readyState === WebSocket.OPEN) c.send(s); });
}

// reclaim tasks from timed-out clients (periodic)
setInterval(() => {
  const now = nowSec();
  let changed = false;
  for(const cid of Object.keys(state.clients)){
    const client = state.clients[cid];
    if(!client) continue;
    if(now - (client.lastSeen || 0) > CLIENT_TIMEOUT_SEC){
      log('Client timed out:', cid);
      // unclaim tasks belonging to this client
      for(const t of state.tasks){
        if(t.claimedBy === cid){
          t.claimedBy = null;
          t.lastUpdate = nowSec();
          changed = true;
        }
      }
      delete state.clients[cid];
      changed = true;
    }
  }
  if(changed) scheduleSave(0);
}, CHECK_INTERVAL_MS);

// assign tasks to client: ensure tasks claimed atomically (single thread)
function assignTasksToClient(clientId, capacity){
  if(!clientId) return [];
  if(state.found) return []; // no work if already found

  // ensure client record
  state.clients[clientId] = state.clients[clientId] || { lastSeen: nowSec(), capacity: capacity || 1, meta: {} };
  state.clients[clientId].lastSeen = nowSec();
  state.clients[clientId].capacity = capacity || state.clients[clientId].capacity || 1;

  // 1) return tasks already claimed by this client
  const assigned = [];
  for(const t of state.tasks){
    if(t.claimedBy === clientId){
      assigned.push({ taskId: t.taskId, c: t.c, offset: t.offset, N: t.N });
      if(assigned.length >= capacity) return assigned;
    }
  }

  // 2) collect unclaimed tasks
  const unclaimed = state.tasks.filter(t => !t.claimedBy);
  // create new tasks if insufficient
  const need = capacity - assigned.length;
  if(need > 0){
    // safety: avoid creating new tasks if N_work is missing
    if(!state.N_work && !state.N_original){
      // no canonical N known; nothing to create
      return assigned;
    }
    const existing = existingSeedsSet();
    if(state.tasks.length < MAX_SERVER_TASKS && unclaimed.length < need){
      const createCount = Math.min(need - unclaimed.length, Math.max(0, MAX_SERVER_TASKS - state.tasks.length));
      for(let i=0;i<createCount;i++){
        const c = uniqueSeed(existing);
        existing.add(c);
        const nt = normalizeTask({ taskId: mkTaskId(), c, offset: "0", claimedBy: null, lastUpdate: nowSec(), N: state.N_work || state.N_original || null });
        state.tasks.push(nt);
        unclaimed.push(nt);
      }
      scheduleSave();
    }
    // now pick up to 'need' unclaimed tasks and claim them
    let picked = 0;
    for(const t of state.tasks){
      if(picked >= need) break;
      if(!t.claimedBy){
        t.claimedBy = clientId;
        t.lastUpdate = nowSec();
        assigned.push({ taskId: t.taskId, c: t.c, offset: t.offset, N: t.N });
        picked++;
      }
    }
    if(picked > 0) scheduleSave();
  }
  return assigned;
}

// ---------- WebSocket message handling ----------
wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.on('pong', () => ws.isAlive = true);

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch(e){
      ws.send(JSON.stringify({ type:'error', message:'bad json' }));
      return;
    }
    const type = msg.type;

    if(type === 'register'){
      const clientId = msg.clientId || ('c-' + crypto.randomBytes(5).toString('hex'));
      state.clients[clientId] = state.clients[clientId] || { lastSeen: nowSec(), capacity: msg.capacity || 1, meta: msg.meta || {} };
      state.clients[clientId].lastSeen = nowSec();
      ws.clientId = clientId;
      ws.send(JSON.stringify({ type:'ack', clientId }));
      // send canonical state snapshot
      ws.send(JSON.stringify({ type: 'state', state: {
        N_original: state.N_original || null,
        N_work: state.N_work || null,
        totalIterations: state.totalIterations,
        found: state.found
      }}));
      log('Registered client', clientId, 'capacity', msg.capacity || 1);
    }

    else if(type === 'requestTasks'){
      const clientId = msg.clientId;
      if(!clientId) return ws.send(JSON.stringify({ type:'error', message:'no clientId' }));

      // optionally set canonical N from client if server hasn't yet
      if(msg.wantN && !state.N_original){
        try {
          state.N_original = String(msg.wantN);
          state.N_work = state.N_work || state.N_original;
          log('Server N_original set from client requestTasks:', state.N_original);
          scheduleSave(0);
        } catch(e){
          warn('Invalid wantN provided by client');
        }
      }

      // If found, return empty and state
      if(state.found){
        ws.send(JSON.stringify({ type:'assign', tasks: [] }));
        ws.send(JSON.stringify({ type:'state', state: { N_original: state.N_original, N_work: state.N_work, totalIterations: state.totalIterations, found: state.found }}));
        return;
      }

      const capacity = Math.max(1, Number(msg.capacity) || 1);
      // update client record heartbeat
      state.clients[clientId] = state.clients[clientId] || { lastSeen: nowSec(), capacity, meta: {} };
      state.clients[clientId].lastSeen = nowSec();
      state.clients[clientId].capacity = capacity;

      const assignedList = assignTasksToClient(clientId, capacity);

      ws.send(JSON.stringify({ type:'assign', tasks: assignedList }));
      ws.send(JSON.stringify({ type:'state', state: { N_original: state.N_original, N_work: state.N_work, totalIterations: state.totalIterations, found: state.found }}));
      log(`Assigned ${assignedList.length} tasks to ${clientId}`);
    }

    else if(type === 'progress'){
      const clientId = msg.clientId, taskId = msg.taskId, deltaStr = msg.delta;
      if(!taskId || !deltaStr) return ws.send(JSON.stringify({ type:'error', message:'bad progress' }));
      const task = state.tasks.find(t => t.taskId === taskId);
      if(!task) return ws.send(JSON.stringify({ type:'error', message:'unknown task' }));
      try {
        const delta = BigInt(String(deltaStr));
        const prev = BigInt(task.offset || '0');
        task.offset = (prev + delta).toString();
        task.lastUpdate = nowSec();
        const tot = BigInt(state.totalIterations || '0');
        state.totalIterations = (tot + delta).toString();
        scheduleSave();
        // ack back
        ws.send(JSON.stringify({ type:'ack', taskId }));
      } catch(e){
        ws.send(JSON.stringify({ type:'error', message:'bad delta' }));
      }
    }

    else if(type === 'found'){
      const factorStr = msg.factor;
      const clientId = msg.clientId || ws.clientId;
      if(!factorStr) return ws.send(JSON.stringify({ type:'error', message:'no factor provided' }));
      const origStr = state.N_original || state.N || null;
      if(!origStr) return ws.send(JSON.stringify({ type:'error', message:'server has no original N to validate against' }));
      try {
        const p = BigInt(String(factorStr));
        const orig = BigInt(String(origStr));
        if(orig % p !== 0n){
          ws.send(JSON.stringify({ type:'error', message:'reported factor does not divide original N' }));
          warn('Rejected invalid factor', p.toString(), 'from', clientId);
          return;
        }
        const q = orig / p;
        state.found = { p: p.toString(), q: q.toString(), foundBy: clientId, at: new Date().toISOString() };
        // log and persist immediately
        const foundMsg = `[${isoTs()}] FOUND by ${clientId}: p=${p.toString()} q=${q.toString()} N=${orig.toString()}`;
        log(foundMsg);
        appendFoundLog(foundMsg);
        saveImmediate(); // immediate flush for important event
        // broadcast canonical state to all clients
        broadcastToClients({ type:'state', state: { N_original: state.N_original, N_work: state.N_work, totalIterations: state.totalIterations, found: state.found }});
      } catch(e){
        ws.send(JSON.stringify({ type:'error', message:'invalid factor format' }));
      }
    }

    else if(type === 'status'){
      if(msg.clientId && state.clients[msg.clientId]) state.clients[msg.clientId].lastSeen = nowSec();
    }

    else if(type === 'getState'){
      ws.send(JSON.stringify({ type:'state', state: { N_original: state.N_original, N_work: state.N_work, totalIterations: state.totalIterations, found: state.found } }));
    }

    else {
      ws.send(JSON.stringify({ type:'error', message:'unknown type' }));
    }
  });

  ws.on('close', () => {
    if(ws.clientId) log('Connection closed for', ws.clientId);
    // tasks remain claimed until reclamation loop unclaims them
  });

  ws.on('error', (e) => {
    warn('ws error', e);
  });
});

// periodic ping / health sweep on sockets
const pinger = setInterval(() => {
  wss.clients.forEach(ws => {
    try {
      if(ws.isAlive === false) return ws.terminate();
      ws.isAlive = false;
      ws.ping(() => {});
    } catch(e){
      // ignore
    }
  });
}, 30000);

// graceful shutdown
function shutdown(signal){
  log('Shutting down due to', signal);
  try { saveImmediate(); } catch(e){ /*ignore*/ }
  try { server.close(() => { log('HTTP server closed'); process.exit(0); }); } catch(e){}
  setTimeout(()=>{ process.exit(0); }, 5000);
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// start server
server.listen(PORT, () => {
  log(`Server listening on port ${PORT}`);
});
