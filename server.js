// server.js — orchestrator for factor hive
const fs = require("fs");
const path = require("path");
const express = require("express");
const cors = require("cors");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const STATE_FILE = path.join(__dirname, "state.json");

let state = {
  meta: { createdAt: new Date().toISOString(), version: 1 },
  N_original: null,
  N_work: null,
  totalIterations: "0",
  found: null,
  tasks: [],
  clients: {},
  lastSaved: new Date().toISOString()
};

// --- Helpers ---
function saveState() {
  state.lastSaved = new Date().toISOString();
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}
function loadState() {
  if (fs.existsSync(STATE_FILE)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
      if (parsed.N && !parsed.N_original) parsed.N_original = parsed.N; // backward compat
      state = parsed;
      console.log("Loaded state.json with", state.tasks.length, "tasks.");
    } catch (e) {
      console.error("Failed to load state.json", e);
    }
  }
}
function broadcast(msg) {
  const data = JSON.stringify(msg);
  wss.clients.forEach(c => {
    if (c.readyState === 1) c.send(data);
  });
}

// --- Express API ---
const app = express();
app.use(cors());
app.use(express.json());

app.get("/state", (req, res) => res.json(state));
app.get("/download", (req, res) => {
  res.setHeader("Content-Disposition", "attachment; filename=state.json");
  res.json(state);
});

const server = app.listen(PORT, () =>
  console.log("HTTP listening on", PORT)
);

// --- WebSocket ---
const wss = new WebSocketServer({ server });

wss.on("connection", ws => {
  ws.on("message", raw => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    const { type } = msg;

    if (type === "register") {
      const id = msg.clientId || "c-" + Math.random().toString(36).slice(2, 8);
      ws.clientId = id;
      state.clients[id] = {
        lastSeen: Date.now() / 1000,
        capacity: msg.capacity || 1,
        meta: msg.meta || {}
      };
      ws.send(JSON.stringify({ type: "ack", clientId: id }));
      // send canonical state immediately
      ws.send(
        JSON.stringify({
          type: "state",
          state: {
            N_original: state.N_original,
            N_work: state.N_work,
            totalIterations: state.totalIterations,
            found: state.found
          }
        })
      );
      console.log("Client registered", id);
    }

    else if (type === "requestTasks") {
      const id = ws.clientId;
      if (!id) return;
      const capacity = msg.capacity || 1;
      const unclaimed = state.tasks.filter(t => !t.claimedBy);
      const assign = unclaimed.slice(0, capacity);
      assign.forEach(t => {
        t.claimedBy = id;
        t.lastUpdate = Date.now() / 1000;
      });
      ws.send(JSON.stringify({ type: "assign", tasks: assign }));
    }

    else if (type === "progress") {
      const { taskId, offset } = msg;
      const task = state.tasks.find(t => t.taskId === taskId);
      if (task) {
        task.offset = String(offset);
        task.lastUpdate = Date.now() / 1000;
      }
    }

    else if (type === "found") {
      const factorStr = msg.factor;
      if (!factorStr) return;
      const origStr = state.N_original;
      if (!origStr) return;
      try {
        const p = BigInt(factorStr);
        const orig = BigInt(origStr);
        if (orig % p !== 0n) {
          console.warn("Rejected invalid factor", p.toString());
          return;
        }
        const q = orig / p;
        state.found = {
          p: p.toString(),
          q: q.toString(),
          foundBy: msg.clientId || ws.clientId,
          at: new Date().toISOString()
        };
        saveState();
        broadcast({
          type: "state",
          state: {
            N_original: state.N_original,
            totalIterations: state.totalIterations,
            found: state.found
          }
        });
        console.log("Factor found:", state.found);
      } catch (e) {
        console.error("Invalid factor from client", e);
      }
    }

    else if (type === "heartbeat") {
      if (ws.clientId && state.clients[ws.clientId]) {
        state.clients[ws.clientId].lastSeen = Date.now() / 1000;
      }
    }
  });

  ws.on("close", () => {
    const id = ws.clientId;
    if (id) {
      Object.values(state.tasks).forEach(t => {
        if (t.claimedBy === id) t.claimedBy = null;
      });
      delete state.clients[id];
    }
  });
});

// --- Periodic save ---
setInterval(() => saveState(), 10000);

// --- Init ---
loadState();
