// server.js
// Node orchestrator for Factor Visualizer
// Usage: node server.js
const fs = require('fs');
const path = require('path');
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const PORT = process.env.PORT || 3000;
const STATE_FILE = path.join(process.cwd(), 'state.json');
const SAVE_INTERVAL = 10000; // 10s checkpoint
const CLIENT_TIMEOUT = 20; // seconds without heartbeat => mark disconnected

// In-memory server state
let state = {
  meta: { createdAt: new Date().toISOString(), version: 1 },
  N: null,
  totalIterations: "0",
  found: null, // {p,q}
  tasks: [],  // tasks: { taskId, c, offset: string, claimedBy: clientId|null, lastUpdate: epoch_sec }
  clients: {}, // clientId -> { lastSeen, capacity, meta }
  lastSaved: null
};

// Load state.json if exists
function loadStateFromFile(){
  try{
    if(fs.existsSync(STATE_FILE)){
      const data = fs.readFileSync(STATE_FILE, 'utf8');
      const parsed = JSON.parse(data);
      // minimal validation
      if(parsed && typeof parsed === 'object') {
        state = parsed;
        console.log('Loaded state from', STATE_FILE);
      }
    }
  }catch(e){ console.warn('Failed to load state.json', e); }
}
function saveStateToFile(){
  try{
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
    state.lastSaved = new Date().toISOString();
  }catch(e){ console.warn('Failed to save state.json', e); }
}

// helper: generate unique task id
function mkTaskId(){ return 't-' + Math.random().toString(36).slice(2,9); }

loadStateFromFile();
saveStateToFile();

const app = express();
app.use(cors());
app.use(express.json());

// REST endpoints
app.get('/state', (req,res) => {
  res.json(state);
});
app.get('/download', (req,res) => {
  res.download(STATE_FILE, 'state.json', (err) => { if(err) res.status(500).send('fail'); });
});
app.post('/task', (req,res) => {
  // create a single new task (optional). body: { c: 'seed', offset: '0', N: '...' }
  const { c, offset } = req.body || {};
  const task = { taskId: mkTaskId(), c: c || (Math.floor(Math.random()*1e9)+1).toString(), offset: offset || '0', claimedBy: null, lastUpdate: Date.now()/1000 };
  state.tasks.push(task);
  saveStateToFile();
  res.json({ ok:true, task });
});

// Serve static index.html if present (useful for a simple deploy)
app.use(express.static(path.join(process.cwd())));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

function broadcastToClients(obj){
  const s = JSON.stringify(obj);
  wss.clients.forEach(c => { if(c.readyState === WebSocket.OPEN) c.send(s); });
}

// Periodic client liveness check and reassign tasks if needed
setInterval(() => {
  const now = Date.now()/1000;
  // prune inactive clients
  for(const cid of Object.keys(state.clients)){
    const client = state.clients[cid];
    if(now - client.lastSeen > CLIENT_TIMEOUT){
      console.log('Client timed out:', cid);
      // mark tasks claimed by this client as unclaimed (so they can be reassigned)
      state.tasks.forEach(t => { if(t.claimedBy === cid) { t.claimedBy = null; } });
      delete state.clients[cid];
    }
  }
  // autosave checkpoint
  saveStateToFile();
}, 3000);

// WebSocket handling
wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.on('pong', () => ws.isAlive = true);

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch(e){ ws.send(JSON.stringify({ type:'error', message:'bad json' })); return; }
    const type = msg.type;
    if(type === 'register'){
      const clientId = msg.clientId || ('c-' + Math.random().toString(36).slice(2,9));
      state.clients[clientId] = { lastSeen: Date.now()/1000, capacity: msg.capacity || 1, meta: msg.meta || {} };
      ws.clientId = clientId;
      ws.send(JSON.stringify({ type:'ack', clientId }));
      console.log('Client registered', clientId, 'capacity', msg.capacity);
    } else if(type === 'requestTasks'){
      const clientId = msg.clientId;
      if(!clientId) return ws.send(JSON.stringify({ type:'error', message:'no clientId' }));
      // update client last seen
      state.clients[clientId] = state.clients[clientId] || { lastSeen: Date.now()/1000, capacity: msg.capacity || 1, meta: {} };
      state.clients[clientId].lastSeen = Date.now()/1000;
      // maybe set N from client if server has none
      if(msg.wantN && !state.N){ state.N = msg.wantN; console.log('Server N set from client', state.N); }
      // build list of unclaimed tasks (or tasks claimed but stale)
      const unclaimed = [];
      const nowSec = Date.now()/1000;
      state.tasks.forEach(t => {
        if(!t.claimedBy) unclaimed.push(t);
        else {
          const clientInfo = state.clients[t.claimedBy];
          if(!clientInfo || (nowSec - (clientInfo.lastSeen || 0) > CLIENT_TIMEOUT)) {
            // considered stale
            t.claimedBy = null;
            unclaimed.push(t);
          }
        }
      });
      // if not enough tasks, create new tasks to match capacity
      const want = Math.max(1, msg.capacity || 1);
      while(state.tasks.filter(t=>!t.claimedBy).length < want){
        const nt = { taskId: mkTaskId(), c: (Math.floor(Math.random()*1e9)+1).toString(), offset: "0", claimedBy: null, lastUpdate: Date.now()/1000 };
        state.tasks.push(nt);
      }
      // pick up to capacity unclaimed tasks and assign to client
      const assign = [];
      for(const t of state.tasks){
        if(assign.length >= want) break;
        if(!t.claimedBy){
          t.claimedBy = clientId;
          t.lastUpdate = Date.now()/1000;
          assign.push({ taskId: t.taskId, c: t.c, offset: t.offset, N: state.N || null });
        }
      }
      // reply with assignments
      ws.send(JSON.stringify({ type:'assign', tasks: assign }));
      saveStateToFile();
      console.log('Assigned', assign.length, 'tasks to', clientId);
    } else if(type === 'progress'){
      // msg: clientId, taskId, delta (string)
      const clientId = msg.clientId;
      const taskId = msg.taskId;
      const delta = BigInt(msg.delta || '0');
      const t = state.tasks.find(x => x.taskId === taskId);
      if(!t) return ws.send(JSON.stringify({ type:'error', message:'unknown task' }));
      // advance offset
      try {
        const prev = BigInt(t.offset || '0');
        t.offset = (prev + delta).toString();
        t.lastUpdate = Date.now()/1000;
        // update global total
        const prevTot = BigInt(state.totalIterations || '0');
        state.totalIterations = (prevTot + delta).toString();
        // ack
        // optionally, if offset is huge, you might rotate tasks or checkpoint
        ws.send(JSON.stringify({ type:'ack', taskId }));
        saveStateToFile();
      } catch(e){
        ws.send(JSON.stringify({ type:'error', message:'bad delta' }));
      }
    } else if(type === 'found'){
      // client reported factor
      const factor = msg.factor;
      if(factor){
        // server records it
        try{
          const p = BigInt(factor);
          // attempt to compute other
          let q = null;
          if(state.N) {
            try { q = (BigInt(state.N)/p).toString(); } catch(e) { q = null; }
          }
          state.found = { p: p.toString(), q: q || null, foundBy: msg.clientId || ws.clientId, at: new Date().toISOString() };
          saveStateToFile();
          broadcastToClients({ type:'state', state: { N: state.N, totalIterations: state.totalIterations, found: state.found } });
          console.log('Found factor reported', factor);
        }catch(e){
          ws.send(JSON.stringify({ type:'error', message:'invalid factor' }));
        }
      }
    } else if(type === 'status'){
      if(msg.clientId && state.clients[msg.clientId]) state.clients[msg.clientId].lastSeen = Date.now()/1000;
    } else if(type === 'getState'){
      ws.send(JSON.stringify({ type:'state', state }));
    } else {
      ws.send(JSON.stringify({ type:'error', message:'unknown type' }));
    }
  });

  ws.on('close', () => {
    // cleanup client; but tasks remain with claimedBy until timeout detection
    console.log('ws closed', ws.clientId);
  });

});

const Interval = setInterval(function ping(){
  wss.clients.forEach(function each(ws) {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

server.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
