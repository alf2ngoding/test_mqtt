'use strict';

/**
 * server.js — SIMPLE DEBUG VERSION
 * Tujuan: pastikan data MQTT dari ESP32 bisa masuk ke internet
 * 
 * TIDAK ADA:
 * - Throttle / slot
 * - statusRunning check
 * - state management
 * - Data drop logic
 * 
 * Semua data MQTT langsung diteruskan ke frontend via SSE.
 */

require('dotenv').config();

const mqtt    = require('mqtt');
const express = require('express');
const cors    = require('cors');
const http    = require('http');
const path    = require('path');

const app = express();
const srv = http.createServer(app);

app.use(cors({ origin: '*' }));
app.use(express.json());
app.use(express.static(__dirname));

// ── SSE ───────────────────────────────────────────────────────────────────────
const sseClients = new Set();

function broadcast(event, data) {
    const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    sseClients.forEach(res => {
        try { res.write(payload); } catch { sseClients.delete(res); }
    });
}

app.get('/api/events', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();
    sseClients.add(res);
    console.log(`[SSE] Client terhubung. Total: ${sseClients.size}`);
    req.on('close', () => { sseClients.delete(res); });
});

// ── MQTT LOG ──────────────────────────────────────────────────────────────────
const mqttLog = [];   // Simpan 50 pesan terakhir
let mqttConnected = false;
let totalReceived = 0;

// ── MQTT ──────────────────────────────────────────────────────────────────────
const MQTT_HOST = process.env.MQTT_HOST;
const MQTT_USER = process.env.MQTT_USER;
const MQTT_PASS = process.env.MQTT_PASS;

const TOPICS = [
    'inkubator/sensor',
    'inkubator/state',
    'inkubator/notif',
    'inkubator/log',
];

if (!MQTT_HOST) {
    console.error('❌ MQTT_HOST tidak di-set di .env!');
} else {
    console.log(`[MQTT] Connecting ke mqtts://${MQTT_HOST}:8883 ...`);
    console.log(`[MQTT] User: ${MQTT_USER}, Pass: ${MQTT_PASS ? '***' : '(kosong)'}`);

    const client = mqtt.connect(`mqtts://${MQTT_HOST}:8883`, {
        username:           MQTT_USER,
        password:           MQTT_PASS,
        rejectUnauthorized: true,
        reconnectPeriod:    5000,
        keepalive:          30,
        connectTimeout:     15000,
    });

    client.on('connect', () => {
        mqttConnected = true;
        console.log(`✅ [MQTT CONNECT OK]`);
        broadcast('status', { mqttConnected: true, msg: 'MQTT terhubung ke broker' });

        client.subscribe(TOPICS, { qos: 0 }, (err, granted) => {
            if (err) {
                console.error('[MQTT SUBSCRIBE ERROR]', err.message);
            } else {
                granted.forEach(g => console.log(`✅ [SUBSCRIBED] ${g.topic}`));
            }
        });
    });

    client.on('message', (topic, message) => {
        const rawStr = message.toString();
        totalReceived++;
        console.log(`📨 [MQTT MSG #${totalReceived}] ${topic}: ${rawStr.slice(0, 200)}`);

        // Simpan ke log
        const entry = {
            no:    totalReceived,
            ts:    new Date().toISOString(),
            topic,
            raw:   rawStr.slice(0, 500),
            parsed: null,
            error: null,
        };

        try {
            entry.parsed = JSON.parse(rawStr);
        } catch (e) {
            entry.error = e.message;
        }

        mqttLog.unshift(entry);
        if (mqttLog.length > 50) mqttLog.pop();

        // Broadcast ke semua SSE client — TANPA filtering apapun
        broadcast('mqtt', entry);
    });

    client.on('error', err => {
        mqttConnected = false;
        console.error(`[MQTT ERROR] ${err.message}`);
        broadcast('status', { mqttConnected: false, msg: `Error: ${err.message}` });
    });

    client.on('offline', () => {
        mqttConnected = false;
        console.warn('[MQTT OFFLINE]');
        broadcast('status', { mqttConnected: false, msg: 'MQTT offline' });
    });

    client.on('reconnect', () => {
        console.log('[MQTT RECONNECT] Mencoba reconnect...');
        broadcast('status', { mqttConnected: false, msg: 'MQTT reconnecting...' });
    });

    client.on('close', () => {
        mqttConnected = false;
        console.warn('[MQTT CLOSE]');
    });
}

// ── REST ──────────────────────────────────────────────────────────────────────
app.get('/api/debug', (_req, res) => res.json({
    mqttConnected,
    totalReceived,
    mqttHost: MQTT_HOST || '(tidak di-set)',
    topics: TOPICS,
    recentLog: mqttLog.slice(0, 10),
    sseClients: sseClients.size,
    uptime: process.uptime(),
}));

app.get('/api/log', (_req, res) => res.json(mqttLog));

// ESP32 fallback: kirim data via HTTP POST (tanpa MQTT)
app.post('/api/push', (req, res) => {
    const data = req.body;
    totalReceived++;
    console.log(`📨 [HTTP PUSH #${totalReceived}]`, JSON.stringify(data).slice(0, 200));

    const entry = {
        no: totalReceived,
        ts: new Date().toISOString(),
        topic: 'http/push',
        raw: JSON.stringify(data),
        parsed: data,
        error: null,
    };

    mqttLog.unshift(entry);
    if (mqttLog.length > 50) mqttLog.pop();

    broadcast('mqtt', entry);
    res.json({ ok: true });
});

app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));

// ── START ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
srv.listen(PORT, () => {
    console.log(`🚀 Server SIMPLE berjalan di port ${PORT}`);
    console.log(`🔍 Debug: GET /api/debug`);
    console.log(`📋 Log:   GET /api/log`);
    console.log(`📡 SSE:   GET /api/events`);
});
