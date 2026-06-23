const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const { makeWASocket, useMultiFileAuthState, DisconnectReason, downloadMediaMessage, extractMessageContent } = require('@whiskeysockets/baileys');
const qrcode = require('qrcode-terminal');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const frontendPath = path.join(__dirname, 'frontend');
app.use(express.static(frontendPath));
app.get('/', (req, res) => res.sendFile(path.join(frontendPath, 'index.html')));

let sock, connected = false;
const mediaStore = new Map();
const chatStore = {};
const presenceStore = {};
const frontendClients = new Set();
let myStatus = 'Hey there! I am using Ramya Messenger';
let myPresence = 'available';

function broadcast(data) {
    const str = JSON.stringify(data);
    frontendClients.forEach(c => {
        if (c.readyState === WebSocket.OPEN) c.send(str);
    });
}

async function startWhatsApp() {
    const { state, saveCreds } = await useMultiFileAuthState('auth_info');
    sock = makeWASocket({ auth: state });
    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;
        if (qr) {
            qrcode.generate(qr, { small: true });
            broadcast({ type: 'qr', qr });
        }
        if (connection === 'close') {
            connected = false;
            broadcast({ type: 'connection', status: 'disconnected' });
            if (lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut) startWhatsApp();
        } else if (connection === 'open') {
            connected = true;
            console.log('WhatsApp connected!');
            broadcast({ type: 'connection', status: 'connected' });
            fetchAndBroadcastChats();
            broadcast({ type: 'myStatus', status: myStatus, presence: myPresence });
        }
    });

    // Handle incoming messages
    sock.ev.on('messages.upsert', async (m) => {
        const msg = m.messages[0];
        if (!msg.message) return;

        const content = extractMessageContent(msg.message);
        
        // Download media if present
        if (content && ['imageMessage', 'videoMessage', 'audioMessage', 'documentMessage'].includes(content.type)) {
            try {
                const buffer = await downloadMediaMessage(msg, 'buffer', {});
                if (buffer) {
                    mediaStore.set(msg.key.id, {
                        buffer,
                        mimetype: content[content.type]?.mimetype || 'application/octet-stream'
                    });
                }
            } catch (e) {
                console.error('Media download error:', e.message);
            }
        }

        const jid = msg.key.remoteJid;
        if (!jid) return;
        
        if (!chatStore[jid]) chatStore[jid] = { messages: [] };

        const formatted = formatMessage(msg);
        const store = chatStore[jid].messages;
        const idx = store.findIndex(m => m.key && m.key.id === msg.key.id);
        if (idx >= 0) {
            store[idx] = formatted;
        } else {
            store.push(formatted);
        }

        // Broadcast the message to all frontend clients
        broadcast({ type: 'message', message: formatted });
        
        // Update chat list
        fetchAndBroadcastChats();
    });

    // Handle presence updates
    sock.ev.on('presence.update', (json) => {
        const { presences } = json;
        if (presences) {
            for (const jid in presences) {
                const pres = presences[jid].lastKnownPresence || 'unavailable';
                presenceStore[jid] = pres;
                broadcast({ type: 'presence', jid, presence: pres });
            }
        }
    });
}

function formatMessage(msg) {
    const content = extractMessageContent(msg.message);
    const obj = {
        key: { id: msg.key.id, fromMe: !!msg.key.fromMe, remoteJid: msg.key.remoteJid },
        fromMe: !!msg.key.fromMe,
        remoteJid: msg.key.remoteJid,
        timestamp: msg.messageTimestamp || Math.floor(Date.now() / 1000),
        pushName: msg.pushName || '',
        messageType: content?.type || 'text',
        text: '',
        mediaUrl: null,
        mimeType: null
    };

    if (!content) return obj;

    if (content.type === 'conversation' || content.type === 'extendedTextMessage') {
        obj.text = content.text || content.conversation || '';
    } else if (content.type === 'imageMessage') {
        obj.text = content.caption || '';
        obj.mimeType = content.imageMessage?.mimetype || 'image/jpeg';
        if (mediaStore.has(msg.key.id)) obj.mediaUrl = '/media/' + msg.key.id;
    } else if (content.type === 'videoMessage') {
        obj.text = content.caption || '';
        obj.mimeType = content.videoMessage?.mimetype || 'video/mp4';
        if (mediaStore.has(msg.key.id)) obj.mediaUrl = '/media/' + msg.key.id;
    } else if (content.type === 'audioMessage') {
        obj.text = '';
        obj.mimeType = content.audioMessage?.mimetype || 'audio/ogg';
        if (mediaStore.has(msg.key.id)) obj.mediaUrl = '/media/' + msg.key.id;
    } else if (content.type === 'documentMessage') {
        obj.text = content.caption || '';
        obj.mimeType = content.documentMessage?.mimetype || 'application/octet-stream';
        if (mediaStore.has(msg.key.id)) obj.mediaUrl = '/media/' + msg.key.id;
    }

    return obj;
}

wss.on('connection', (ws) => {
    console.log('Frontend client connected');
    frontendClients.add(ws);
    
    ws.send(JSON.stringify({ type: 'connection', status: connected ? 'connected' : 'connecting' }));
    if (connected) {
        ws.send(JSON.stringify({ type: 'myStatus', status: myStatus, presence: myPresence }));
    }

    ws.on('message', async (raw) => {
        let data;
        try { data = JSON.parse(raw); } catch (e) { return; }
        
        try {
            if (!connected && !['fetchChats', 'startChat'].includes(data.type)) {
                ws.send(JSON.stringify({ type: 'error', error: 'WhatsApp not connected yet' }));
                return;
            }

            switch (data.type) {
                case 'text':
                    if (data.to && data.body) {
                        await sock.sendMessage(data.to, { text: data.body });
                    }
                    break;
                    
                case 'startChat':
                    if (data.jid) {
                        if (!chatStore[data.jid]) chatStore[data.jid] = { messages: [] };
                        await fetchAndBroadcastChats();
                        ws.send(JSON.stringify({ type: 'messages', jid: data.jid, messages: [] }));
                    }
                    break;
                    
                case 'fetchChats':
                    await fetchAndBroadcastChats();
                    break;
                    
                case 'fetchMessages':
                    if (data.jid && chatStore[data.jid]) {
                        const msgs = chatStore[data.jid].messages.slice(-(data.limit || 100));
                        ws.send(JSON.stringify({ type: 'messages', jid: data.jid, messages: msgs }));
                    } else if (data.jid) {
                        ws.send(JSON.stringify({ type: 'messages', jid: data.jid, messages: [] }));
                    }
                    break;
                    
                case 'fetchProfilePicture':
                    if (data.jid) {
                        try {
                            const pp = await sock.profilePictureUrl(data.jid, 'image');
                            ws.send(JSON.stringify({ type: 'profilePicture', jid: data.jid, url: pp }));
                        } catch (e) {
                            ws.send(JSON.stringify({ type: 'profilePicture', jid: data.jid, url: null }));
                        }
                    }
                    break;
                    
                case 'presence':
                    if (data.to && data.presence) {
                        await sock.sendPresenceUpdate(data.presence, data.to);
                    }
                    break;
                    
                case 'updateMyStatus':
                    if (data.status !== undefined) myStatus = data.status;
                    if (data.presence !== undefined) {
                        myPresence = data.presence;
                        await sock.sendPresenceUpdate(myPresence);
                    }
                    broadcast({ type: 'myStatus', status: myStatus, presence: myPresence });
                    break;
                    
                case 'fetchMyStatus':
                    ws.send(JSON.stringify({ type: 'myStatus', status: myStatus, presence: myPresence }));
                    break;
                    
                case 'fetchPresence':
                    if (data.jid) {
                        ws.send(JSON.stringify({ type: 'presence', jid: data.jid, presence: presenceStore[data.jid] || 'unavailable' }));
                    }
                    break;
            }
        } catch (err) {
            console.error('Error:', err);
            ws.send(JSON.stringify({ type: 'error', error: err.message }));
        }
    });

    ws.on('close', () => {
        frontendClients.delete(ws);
        console.log('Frontend client disconnected');
    });
});

async function fetchAndBroadcastChats() {
    const chats = [];
    const seen = new Set();
    
    for (const jid in chatStore) {
        if (seen.has(jid)) continue;
        seen.add(jid);
        
        const store = chatStore[jid];
        let name = jid;
        
        try {
            const contact = sock.contacts?.[jid] || (await sock.getContact(jid).catch(() => null));
            if (contact?.name || contact?.notify || contact?.verifiedName) {
                name = contact.name || contact.notify || contact.verifiedName;
            }
        } catch (e) {}
        
        const lastMsg = store.messages[store.messages.length - 1];
        chats.push({
            jid,
            name,
            lastMessage: lastMsg ? (lastMsg.text || lastMsg.messageType || '') : '',
            timestamp: lastMsg ? lastMsg.timestamp : 0,
            presence: presenceStore[jid] || 'unavailable'
        });
    }
    
    chats.sort((a, b) => b.timestamp - a.timestamp);
    broadcast({ type: 'chats', chats });
}

app.get('/media/:id', (req, res) => {
    const entry = mediaStore.get(req.params.id);
    if (!entry) return res.status(404).send('Media not found');
    res.setHeader('Content-Type', entry.mimetype);
    res.send(entry.buffer);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log('Server running on port ' + PORT);
    startWhatsApp();
});