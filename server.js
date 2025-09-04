// server.js
// WebSocket + REST orchestrator for the Factor Visualizer
// - Atomic state.json writes
// - Validates "found" against N_original
// - Assigns tasks and persists offsets as decimal strings
// - Exposes /state and /download endpoints
//
// Usage: node server.js
const fs = require('fs');
const path = require('path');
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const PORT = process.env.PORT || 3000;
const STATE_FILE = path.join(process.cwd(), 'state.json');
const TMP_STATE = STATE_FILE + '.tmp';
const CHECK_INTERVAL_MS = 3000;
const CLIENT_TIMEOUT_SEC = 20;

let state = {
  meta: { createdAt: new Date().toISOString(), version: 1 },
  N_original: null,    // canonical original N (string)
  N_work: null,        // N workers should factor (string)
  totalIterations: "0",// decimal string
  found: null,         // null or { p, q, foundBy, at }
  tasks: [],           // array of tasks {taskId,c,offset,claimedBy,lastUpdate,N}
  clients: {},         // clientId -> { lastSeen, capacity, meta }
  lastSaved: null
};

function safeParseJSON(s){
  try { return JSON.parse(s); } catch(e){ return null; }
}

function loadStateFromFile(){
  try{
    if(fs.existsSync(STATE_FILE)){
      const raw = fs.readFileSync(STATE_FILE, 'utf8');
      const parsed = safeParseJSON(raw);
      if(parsed && typeof parsed === 'object'){
        // migrate legacy keys if present
        if(parsed.N && !parsed.N_original) parsed.N_original = parsed.N;
        state = Object.assign(state, parsed);
        console.log('Loaded state from', STATE_FILE);
      } else {
        console.warn('state.json invalid JSON, starting fresh');
      }
    } else {
      console.log('No existing state.json, starting with empty state');
    }
  }catch(e){
    console.warn('Failed to load state.json:', e);
  }
}

function saveStateToFile(){
  try{
    // stringify with deterministic ordering not necessary but readable
    fs.writeFileSync(TMP_STATE, JSON.stringify(state, null, 2), 'utf8');
    fs.renameSync(TMP_STATE, STATE_FILE); // atomic on many OSes
    state.lastSaved = new Date().toISOString();
    // don't log on every save to avoid excessive noise
  }catch(e){
    console.warn('Failed to save state.json:', e);
  }
}

function mkTaskId(){ return 't-' + Math.random().toString(36).slice(2,9); }
function nowSec(){ return Math.floor(Date.now()/1000); }

loadStateFromFile();
saveStateToFile(); // ensure lastSaved exists

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(process.cwd())); // serves index.html

app.get('/state', (req,res) => {
  res.json(state);
});
app.get('/download', (req,res) => {
  if(fs.existsSync(STATE_FILE)) return res.download(STATE_FILE);
  res.status(404).send('no state file');
});
app.post('/task', (req,res) => {
  const { c, offset, N } = req.body || {};
  const task = {
    taskId: mkTaskId(),
    c: String(c || (Math.floor(Math.random()*1e9)+1)),
    offset: String(offset || '0'),
    claimedBy: null,
    lastUpdate: nowSec(),
    N: String(N || (state.N_work || state.N_original || ''))
  };
  state.tasks.push(task);
  saveStateToFile();
  res.json({ ok:true, task });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// helpful broadcast
function broadcastToClients(obj){
  const s = JSON.stringify(obj);
  wss.clients.forEach(c => { if(c.readyState === WebSocket.OPEN) c.send(s); });
}

// reclaim tasks from stale clients
setInterval(()=>{
  const now = nowSec();
  for(const cid of Object.keys(state.clients)){
    const client = state.clients[cid];
    if(!client) continue;
    if(now - (client.lastSeen || 0) > CLIENT_TIMEOUT_SEC){
      console.log('Client timed out:', cid);
      // unclaim tasks
      state.tasks.forEach(t => { if(t.claimedBy === cid) t.claimedBy = null; });
      delete state.clients[cid];
    }
  }
  saveStateToFile();
}, CHECK_INTERVAL_MS);

// websocket handlers
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
      const clientId = msg.clientId || ('c-' + Math.random().toString(36).slice(2,9));
      state.clients[clientId] = state.clients[clientId] || { lastSeen: nowSec(), capacity: msg.capacity || 1, meta: msg.meta || {} };
      state.clients[clientId].lastSeen = nowSec();
      ws.clientId = clientId;
      ws.send(JSON.stringify({ type:'ack', clientId }));
      // Immediately send canonical state snapshot
      ws.send(JSON.stringify({ type: 'state', state: {
        N_original: state.N_original || state.N || null,
        N_work: state.N_work || state.N_original || null,
        totalIterations: state.totalIterations,
        found: state.found
      }}));
      console.log('Registered client', clientId, 'capacity', msg.capacity || 1);
    }
    else if(type === 'requestTasks'){
      const clientId = msg.clientId;
      if(!clientId) return ws.send(JSON.stringify({ type:'error', message:'no clientId' }));
      // update client
      state.clients[clientId] = state.clients[clientId] || { lastSeen: nowSec(), capacity: msg.capacity || 1, meta: {} };
      state.clients[clientId].lastSeen = nowSec();
      state.clients[clientId].capacity = msg.capacity || state.clients[clientId].capacity || 1;

      // If client provided N and server hasn't set N_original yet, set it
      if(msg.wantN && !state.N_original){
        state.N_original = String(msg.wantN);
        state.N_work = state.N_work || state.N_original;
        console.log('Server N_original set from client requestTasks:', state.N_original);
      }

      // build assignments: unclaimed tasks (or stale)
      const nowS = nowSec();
      const unclaimed = [];
      for(const t of state.tasks){
        if(!t.claimedBy) unclaimed.push(t);
        else {
          const clientInfo = state.clients[t.claimedBy];
          if(!clientInfo || (nowS - (clientInfo.lastSeen || 0) > CLIENT_TIMEOUT_SEC)){
            t.claimedBy = null;
            unclaimed.push(t);
          }
        }
      }
      // ensure there are at least capacity many unclaimed tasks by creating new ones
      const want = Math.max(1, msg.capacity || 1);
      while(state.tasks.filter(t=>!t.claimedBy).length < want){
        const nt = { taskId: mkTaskId(), c: (Math.floor(Math.random()*1e9)+1).toString(), offset: "0", claimedBy: null, lastUpdate: nowSec(), N: state.N_work || state.N_original || null };
        state.tasks.push(nt);
      }
      // pick up to capacity unclaimed tasks and assign to this client
      const assign = [];
      for(const t of state.tasks){
        if(assign.length >= want) break;
        if(!t.claimedBy){
          t.claimedBy = clientId;
          t.lastUpdate = nowSec();
          assign.push({ taskId: t.taskId, c: t.c, offset: t.offset, N: t.N });
        }
      }
      // send assigned tasks
      ws.send(JSON.stringify({ type:'assign', tasks: assign }));
      saveStateToFile();
      console.log('Assigned', assign.length, 'tasks to', clientId);
    }
    else if(type === 'progress'){
      const clientId = msg.clientId, taskId = msg.taskId, deltaStr = msg.delta;
      if(!taskId || !deltaStr) return ws.send(JSON.stringify({ type:'error', message:'bad progress' }));
      const task = state.tasks.find(t => t.taskId === taskId);
      if(!task) return ws.send(JSON.stringify({ type:'error', message:'unknown task' }));
      try {
        const delta = BigInt(deltaStr);
        const prev = BigInt(task.offset || '0');
        task.offset = (prev + delta).toString();
        task.lastUpdate = nowSec();
        // update global total
        const tot = BigInt(state.totalIterations || '0');
        state.totalIterations = (tot + delta).toString();
        saveStateToFile();
        ws.send(JSON.stringify({ type:'ack', taskId }));
      } catch(e){
        ws.send(JSON.stringify({ type:'error', message:'bad delta' }));
      }
    }
    else if(type === 'found'){
      const factorStr = msg.factor;
      const clientId = msg.clientId || ws.clientId;
      if(!factorStr) return ws.send(JSON.stringify({ type:'error', message:'no factor' }));
      const origStr = state.N_original || state.N || null;
      if(!origStr) return ws.send(JSON.stringify({ type:'error', message:'server has no original N to validate against' }));
      try{
        const p = BigInt(String(factorStr));
        const orig = BigInt(String(origStr));
        if(orig % p !== 0n){
          ws.send(JSON.stringify({ type:'error', message:'reported factor does not divide original N' }));
          console.warn('Rejected invalid factor', p.toString());
          return;
        }
        const q = orig / p;
        state.found = { p: p.toString(), q: q.toString(), foundBy: clientId, at: new Date().toISOString() };
        saveStateToFile();
        broadcastToClients({ type:'state', state: { N_original: state.N_original, N_work: state.N_work, totalIterations: state.totalIterations, found: state.found }});
        console.log('FOUND factor accepted:', state.found);
      } catch(e){
        ws.send(JSON.stringify({ type:'error', message:'invalid factor' }));
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
    // leave tasks claimed until timeout detection loop cleans them up
    if(ws.clientId) console.log('Connection closed for', ws.clientId);
  });

  ws.on('error', (e) => {
    console.warn('ws error', e);
  });
});

const pinger = setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch(e) {}
  });
}, 30000);

server.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
