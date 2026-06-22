const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const {
    makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    downloadMediaMessage,
    extractMessageContent,
} = require('@whiskeysockets/baileys');
const qrcode = require('qrcode-terminal');

// ---------- Express + HTTP ----------
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Serve static files from the "frontend" folder
app.use(express.static(path.join(__dirname, 'frontend')));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'frontend', 'index.html')));

// ---------- WhatsApp client ----------
let sock;
let connected = false;

// Media store: messageId -> { buffer, mimetype }
const mediaStore = new Map();
// Messages store per JID: { [jid]: { messages: [], lastSeen } }
const chatStore = {};
// Presence cache
const presenceCache = {};

// All frontend WebSocket clients
const frontendClients = new Set();

// Broadcast to all connected UIs
function broadcast(data) {
    const str = JSON.stringify(data);
    frontendClients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) client.send(str);
    });
}

// ---------- Baileys Initialization ----------
async function startWhatsApp() {
    const { state, saveCreds } = await useMultiFileAuthState('auth_info');

    sock = makeWASocket({
        auth: state,
        // We handle QR manually
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            console.log('Scan this QR code with WhatsApp (Linked Devices):\n');
            qrcode.generate(qr, { small: true });
            // Also send to frontend
            broadcast({ type: 'qr', qr });
        }

        if (connection === 'close') {
            connected = false;
            broadcast({ type: 'connection', status: 'disconnected' });
            const shouldReconnect =
                lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log('Connection closed. Reconnecting:', shouldReconnect);
            if (shouldReconnect) {
                startWhatsApp();
            } else {
                console.log('Logged out. Delete auth_info folder to re-link.');
            }
        } else if (connection === 'open') {
            connected = true;
            console.log('WhatsApp connected successfully!');
            broadcast({ type: 'connection', status: 'connected' });
            fetchAndBroadcastChats();
        }
    });

    // Incoming messages
    sock.ev.on('messages.upsert', async (m) => {
        const msg = m.messages[0];
        if (!msg.message) return;

        // Download media if present
        const content = extractMessageContent(msg.message);
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
                console.error('Media download failed:', e);
            }
        }

        // Save to chat store
        const jid = msg.key.remoteJid;
        if (!chatStore[jid]) chatStore[jid] = { messages: [], lastSeen: Date.now() };

        const formatted = formatMessage(msg);
        const store = chatStore[jid].messages;
        const idx = store.findIndex(m => m.key?.id === msg.key.id);
        if (idx >= 0) store[idx] = formatted;
        else store.push(formatted);

        broadcast({ type: 'message', message: formatted });
    });

    // Presence updates
    sock.ev.on('presence.update', (json) => {
        const { presences } = json;
        if (presences) {
            for (const jid in presences) {
                const pres = presences[jid].lastKnownPresence || 'unavailable';
                presenceCache[jid] = pres;
                broadcast({ type: 'presence', jid, presence: pres });
            }
        }
    });
}

// Format a Baileys message for frontend
function formatMessage(msg) {
    const content = extractMessageContent(msg.message);
    const obj = {
        key: msg.key,
        fromMe: msg.key.fromMe,
        remoteJid: msg.key.remoteJid,
        timestamp: msg.messageTimestamp,
        pushName: msg.pushName || '',
        messageType: content?.type || 'text',
        text: '',
        mediaUrl: null,
        mimeType: null,
        location: null,
        contact: null
    };

    if (!content) return obj;

    if (content.type === 'conversation' || content.type === 'extendedTextMessage') {
        obj.text = content.text || content.conversation || '';
    } else if (['imageMessage', 'videoMessage', 'audioMessage', 'documentMessage'].includes(content.type)) {
        obj.text = content.caption || '';
        obj.mimeType = content[content.type]?.mimetype || '';
        if (mediaStore.has(msg.key.id)) {
            obj.mediaUrl = `/media/${msg.key.id}`;
        }
    } else if (content.type === 'locationMessage') {
        obj.text = `📍 Location: ${content.degreesLatitude}, ${content.degreesLongitude}`;
        obj.location = { lat: content.degreesLatitude, lng: content.degreesLongitude };
    } else if (content.type === 'contactMessage') {
        obj.text = `👤 Contact: ${content.displayName}`;
        obj.contact = { displayName: content.displayName, vcard: content.vcard };
    }
    return obj;
}

// ---------- WebSocket handling ----------
wss.on('connection', (ws) => {
    console.log('Frontend client connected');
    frontendClients.add(ws);

    // Send current state
    ws.send(JSON.stringify({ type: 'connection', status: connected ? 'connected' : 'connecting' }));

    ws.on('message', async (raw) => {
        let data;
        try { data = JSON.parse(raw); } catch (e) { return; }
        try {
            await handleFrontendMessage(ws, data);
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

// ---------- Frontend request handler ----------
async function handleFrontendMessage(ws, data) {
    if (!connected && data.type !== 'fetchChats') {
        ws.send(JSON.stringify({ type: 'error', error: 'WhatsApp not connected yet' }));
        return;
    }

    switch (data.type) {
        case 'text':
            if (data.to && data.body) {
                await sock.sendMessage(data.to, { text: data.body });
            }
            break;

        case 'media':
            if (data.to && data.data && data.mediaType) {
                const buffer = Buffer.from(data.data, 'base64');
                const opts = { caption: data.caption || '' };
                if (data.mediaType === 'image') opts.image = buffer;
                else if (data.mediaType === 'video') opts.video = buffer;
                else if (data.mediaType === 'audio') opts.audio = buffer;
                else if (data.mediaType === 'document') {
                    opts.document = buffer;
                    opts.fileName = data.fileName || 'file';
                    opts.mimetype = data.mimetype || 'application/octet-stream';
                }
                await sock.sendMessage(data.to, opts);
            }
            break;

        case 'presence':
            if (data.to && data.presence) {
                await sock.sendPresenceUpdate(data.presence, data.to);
            }
            break;

        case 'fetchChats':
            await fetchAndBroadcastChats();
            break;

        case 'fetchMessages':
            if (data.jid && chatStore[data.jid]) {
                const limit = data.limit || 50;
                const msgs = chatStore[data.jid].messages.slice(-limit);
                ws.send(JSON.stringify({ type: 'messages', jid: data.jid, messages: msgs }));
            }
            break;

        case 'fetchProfilePicture':
            if (data.jid) {
                try {
                    const ppUrl = await sock.profilePictureUrl(data.jid, 'image');
                    ws.send(JSON.stringify({ type: 'profilePicture', jid: data.jid, url: ppUrl }));
                } catch (e) {
                    ws.send(JSON.stringify({ type: 'profilePicture', jid: data.jid, url: null }));
                }
            }
            break;

        default:
            console.log('Unknown frontend request:', data.type);
    }
}

// Build chat list from chatStore
async function fetchAndBroadcastChats() {
    const chats = [];
    for (const jid in chatStore) {
        const store = chatStore[jid];
        let name = jid;
        try {
            const contact = sock.contacts?.[jid] || (await sock.getContact(jid));
            if (contact?.name || contact?.notify || contact?.verifiedName) {
                name = contact.name || contact.notify || contact.verifiedName;
            }
        } catch (e) {}
        const lastMsg = store.messages[store.messages.length - 1];
        chats.push({
            jid,
            name,
            lastMessage: lastMsg ? (lastMsg.text || lastMsg.messageType) : '',
            timestamp: lastMsg ? lastMsg.timestamp : 0,
        });
    }
    chats.sort((a, b) => b.timestamp - a.timestamp);
    broadcast({ type: 'chats', chats });
}

// Serve stored media
app.get('/media/:id', (req, res) => {
    const entry = mediaStore.get(req.params.id);
    if (!entry) return res.status(404).send('Media not found');
    res.setHeader('Content-Type', entry.mimetype);
    res.send(entry.buffer);
});

// ---------- START SERVER (ensure port 3000 is free) ----------
const PORT = 3000;
server.listen(PORT, () => {
    console.log(`Server running at http://localhost:${PORT}`);
    startWhatsApp();
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\nShutting down...');
    server.close();
    process.exit(0);
});