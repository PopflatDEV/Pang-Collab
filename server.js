const WebSocket = require('ws');

// Render automatically assigns a PORT environment variable
const port = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: port });

// State Management
const rooms = {};        // Maps roomId -> Set of WebSocket clients
const globalUsers = {};  // Maps username -> WebSocket client (for routing invites)

wss.on('connection', (ws) => {
    let currentRoom = null;
    let playerId = Math.random().toString(36).substring(2, 9);
    let currentUser = null; // Stores the player's username if they provide it

    ws.on('message', (message) => {
        let data;
        try {
            data = JSON.parse(message);
        } catch (err) {
            return; // Ignore malformed JSON
        }

        // 1. Handle user joining a specific project room
        if (data.type === 'join') {
            const roomId = data.roomId;
            
            // If the client sends their username, register them globally so they can receive invites
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

        // 2. Broadcast High-Frequency Events (Cursors & Block Syncs)
        if (data.type === 'cursor' || data.type === 'block_change') {
            if (currentRoom && rooms[currentRoom]) {
                rooms[currentRoom].forEach(client => {
                    // Broadcast to everyone else in the room EXCEPT the sender
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
                // Route the invite directly to the target user's WebSocket connection
                targetSocket.send(JSON.stringify({
                    type: 'receive_invite',
                    roomId: roomId,
                    fromId: currentUser || playerId
                }));
                console.log(`[INVITE] Sent from ${currentUser || playerId} to ${targetUser}`);
            } else {
                ws.send(JSON.stringify({ type: 'error', message: `User ${targetUser} is not currently online.` }));
            }
        }
    });

    // 4. Cleanup on Disconnect
    ws.on('close', () => {
        // Remove the user from their active room
        if (currentRoom && rooms[currentRoom]) {
            rooms[currentRoom].delete(ws);
            
            // Notify remaining players so they can clear this user's cursor from the DOM
            rooms[currentRoom].forEach(client => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(JSON.stringify({ type: 'leave', playerId }));
                }
            });
            
            // Destroy the room if it is empty to save server memory
            if (rooms[currentRoom].size === 0) {
                delete rooms[currentRoom];
                console.log(`[CLOSE] Room ${currentRoom} destroyed (empty)`);
            }
        }
        
        // Remove the user from the global invite registry
        if (currentUser && globalUsers[currentUser] === ws) {
            delete globalUsers[currentUser];
        }
        
        console.log(`[DISCONNECT] ${currentUser || playerId} left.`);
    });
});

console.log(`Live Collab Server is running on port ${port}...`);
