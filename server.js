const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');

// Configuration
const PORT = process.env.PORT || 8080;
const STALE_TASK_TIMEOUT = 120 * 1000; // 2 minutes
const TASK_CLEANUP_INTERVAL = 30 * 1000; // Every 30 seconds

// Server state
let currentN = null;
let totalIterations = 0n;
let foundFactor = null;
let usedC = new Set();
let tasks = new Map();
let clients = new Map();

// Generate unique c values (1-1e9)
function generateC() {
  let c;
  do {
    c = Math.floor(Math.random() * 1e9) + 1;
  } while (usedC.has(c));
  usedC.add(c);
  return c;
}

// Create new factoring task
function createTask() {
  const c = generateC();
  const taskId = uuidv4().slice(0, 8);
  return {
    taskId,
    c: c.toString(),
    offset: '0',
    lastProgress: Date.now(),
    status: 'pending'
  };
}

// Assign tasks to client
function assignTasks(clientId, capacity) {
  const client = clients.get(clientId);
  if (!client) return [];

  const assignments = [];
  for (let i = 0; i < capacity; i++) {
    const task = createTask();
    task.assignedClientId = clientId;
    task.status = 'assigned';
    tasks.set(task.taskId, task);
    assignments.push(task);
    client.tasks.add(task.taskId);
  }
  return assignments;
}

// Broadcast state to all clients
function broadcastState() {
  const state = {
    N_original: currentN,
    totalIterations: totalIterations.toString(),
    found: foundFactor
  };
  
  clients.forEach(client => {
    if (client.socket.readyState === WebSocket.OPEN) {
      client.socket.send(JSON.stringify({ 
        type: 'state', 
        state 
      }));
    }
  });
}

// Cleanup stale tasks
function cleanupStaleTasks() {
  const now = Date.now();
  tasks.forEach((task, taskId) => {
    if (task.status === 'assigned' && (now - task.lastProgress > STALE_TASK_TIMEOUT)) {
      console.log(`Reassigning stale task: ${taskId}`);
      const client = clients.get(task.assignedClientId);
      if (client) client.tasks.delete(taskId);
      task.status = 'pending';
      task.assignedClientId = null;
    }
  });
}

// Reset job for new N
function resetJob(newN) {
  currentN = newN;
  totalIterations = 0n;
  foundFactor = null;
  usedC = new Set();
  tasks = new Map();
  broadcastState();
}

// Initialize WebSocket server
const wss = new WebSocket.Server({ port: PORT });

wss.on('connection', (socket) => {
  let clientId = null;
  let clientCapacity = 1;

  socket.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      
      // Registration
      if (msg.type === 'register') {
        clientId = msg.clientId || uuidv4();
        clientCapacity = Math.max(1, Math.min(128, msg.capacity || 4));
        
        if (clients.has(clientId)) {
          const oldClient = clients.get(clientId);
          if (oldClient.socket !== socket) oldClient.socket.close();
        }
        
        clients.set(clientId, {
          capacity: clientCapacity,
          socket,
          tasks: new Set(),
          lastSeen: Date.now()
        });
        
        socket.send(JSON.stringify({ type: 'ack', clientId }));
        console.log(`Registered client: ${clientId} (capacity: ${clientCapacity})`);
      }
      
      // Task requests
      else if (msg.type === 'requestTasks') {
        if (!clientId) return;
        
        const wantN = (msg.wantN || '').trim();
        if (!wantN) {
          socket.send(JSON.stringify({ 
            type: 'error', 
            message: 'Missing N value' 
          }));
          return;
        }

        // Initialize or reset job
        if (!currentN) {
          resetJob(wantN);
        } 
        else if (currentN !== wantN && totalIterations === 0n) {
          resetJob(wantN);
        }
        else if (currentN !== wantN) {
          socket.send(JSON.stringify({ 
            type: 'error', 
            message: `Job in progress for N=${currentN}` 
          }));
          return;
        }

        // Handle completed job
        if (foundFactor) {
          socket.send(JSON.stringify({ type: 'assign', tasks: [] }));
          return;
        }

        // Assign new tasks
        const assignments = assignTasks(clientId, clientCapacity);
        socket.send(JSON.stringify({ type: 'assign', tasks: assignments }));
      }
      
      // Progress updates
      else if (msg.type === 'progress') {
        if (!clientId) return;
        
        const task = tasks.get(msg.taskId);
        if (!task || task.assignedClientId !== clientId) return;
        
        task.lastProgress = Date.now();
        try {
          totalIterations += BigInt(msg.delta);
          broadcastState();
        } catch (e) {
          console.error('Invalid progress delta:', msg.delta);
        }
      }
      
      // Factor found
      else if (msg.type === 'found') {
        if (!clientId || foundFactor) return;
        foundFactor = msg.factor;
        console.log(`FACTOR FOUND: ${msg.factor}`);
        broadcastState();
      }
      
      // State requests
      else if (msg.type === 'getState') {
        if (clientId) {
          socket.send(JSON.stringify({ 
            type: 'state', 
            state: {
              N_original: currentN,
              totalIterations: totalIterations.toString(),
              found: foundFactor
            }
          }));
        }
      }
    } catch (e) {
      console.error('Message error:', e);
    }
  });

  socket.on('close', () => {
    if (clientId && clients.has(clientId)) {
      const client = clients.get(clientId);
      client.tasks.forEach(taskId => {
        const task = tasks.get(taskId);
        if (task) {
          task.status = 'pending';
          task.assignedClientId = null;
        }
      });
      clients.delete(clientId);
    }
  });
});

// Periodic cleanup
setInterval(cleanupStaleTasks, TASK_CLEANUP_INTERVAL);

console.log(`Server running on port ${PORT}`);
