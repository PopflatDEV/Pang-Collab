const WebSocket = require('ws');

const port = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: port });

// State Management
const rooms = {};        // Maps roomId -> Set of WebSocket clients
const globalUsers = {};  // Maps username -> WebSocket client (for routing invites)

wss.on('connection', (ws) => {
    let currentRoom = null;
    let playerId = Math.random().toString(36).substring(2, 9);
    let currentUser = null; 

    ws.on('message', (message) => {
        let data;
        try {
            data = JSON.parse(message);
        } catch (err) {
            return; // Ignore malformed JSON
        }

        // 1. Handle joining a room
        if (data.type === 'join') {
            const roomId = data.roomId;
            
            // Register the user globally for invites
            if (data.username) {
                currentUser = data.username;
                globalUsers[currentUser] = ws;
            }

            if (!rooms[roomId]) rooms[roomId] = new Set();
            
            // Enforce the 5-person collaboration limit
            if (rooms[roomId].size >= 5) {
                ws.send(JSON.stringify({ type: 'error', message: 'Room full. Limit is 5 people.' }));
                return;
            }
            
            rooms[roomId].add(ws);
            currentRoom = roomId;
            
            ws.send(JSON.stringify({ type: 'joined', playerId, roomId }));
            console.log(`[JOIN] ${currentUser || playerId} joined room: ${roomId}`);
        }

        // 2. Broadcast High-Frequency Events (Cursors & Blocks)
        if (data.type === 'cursor' || data.type === 'block_change') {
            if (currentRoom && rooms[currentRoom]) {
                rooms[currentRoom].forEach(client => {
                    // Send to everyone else EXCEPT the person who made the change
                    if (client !== ws && client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({ ...data, playerId }));
                    }
                });
            }
        }

        // 3. Route Real-Time Invites
        if (data.type === 'send_invite') {
            const targetUser = data.targetUser;
            const roomId = data.roomId;
            
            const targetSocket = globalUsers[targetUser];
            
            if (targetSocket && targetSocket.readyState === WebSocket.OPEN) {
                targetSocket.send(JSON.stringify({
                    type: 'receive_invite',
                    roomId: roomId,
                    fromId: currentUser || playerId
                }));
                console.log(`[INVITE] Sent from ${currentUser || playerId} to ${targetUser}`);
            } else {
                ws.send(JSON.stringify({ type: 'error', message: `User ${targetUser} is not online.` }));
            }
        }
    });

    // 4. Cleanup on Disconnect
    ws.on('close', () => {
        if (currentRoom && rooms[currentRoom]) {
            rooms[currentRoom].delete(ws);
            
            rooms[currentRoom].forEach(client => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(JSON.stringify({ type: 'leave', playerId }));
                }
            });
            
            if (rooms[currentRoom].size === 0) {
                delete rooms[currentRoom];
            }
        }
        
        if (currentUser && globalUsers[currentUser] === ws) {
            delete globalUsers[currentUser];
        }
    });
});

console.log(`Live Collab Server running on port ${port}...`);
