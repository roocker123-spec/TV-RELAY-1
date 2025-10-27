// server.js (CommonJS)
// Node 18+ (native fetch). If your Node is older, install node-fetch and use it instead.

const express = require("express");
const crypto = require("crypto");

const app = express();
app.use(express.json({ limit: "1mb" }));

// --- ENV ---
// Set these in Render (or your host) dashboard → Environment:
const DELTA_BASE = process.env.DELTA_BASE || "https://api.india.delta.exchange"; // testnet: https://cdn-ind.testnet.deltaex.org
const API_KEY = process.env.DELTA_API_KEY;
const API_SECRET = (process.env.DELTA_API_SECRET || "").toString();
const WEBHOOK_TOKEN = process.env.WEBHOOK_TOKEN || ""; // optional shared-secret

if (!API_KEY || !API_SECRET) {
  console.error("Missing DELTA_API_KEY / DELTA_API_SECRET");
}

// ---------- Delta signing + request helper ----------
async function deltaRequest(method, path, { params = null, body = null } = {}) {
  const ts = Math.floor(Date.now() / 1000).toString();

  let query = "";
  if (params && Object.keys(params).length > 0) {
    const usp = new URLSearchParams(params);
    query = `?${usp.toString()}`;
  }

  const payload = body ? JSON.stringify(body) : "";
  const msg = method.toUpperCase() + ts + path + query + payload;
  const signature = crypto.createHmac("sha256", API_SECRET).update(msg).digest("hex");

  const url = `${DELTA_BASE}${path}${query}`;
  const res = await fetch(url, {
    method,
    headers: {
      "api-key": API_KEY,
      "timestamp": ts,
      "signature": signature,
      "Accept": "application/json",
      "Content-Type": "application/json",
    },
    body: payload || undefined,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Delta ${method} ${path} -> ${res.status} ${text}`);
  }
  return res.json();
}

// ---------- Delta actions we need ----------
const cancelAllOrders = () => deltaRequest("DELETE", "/v2/orders/all", { body: {} });
const closeAllPositions = () => deltaRequest("POST", "/v2/positions/close_all", { body: {} });
const listOpenOrders = () => deltaRequest("GET", "/v2/orders", { params: { states: "open,pending" } });
const listPositions = () => deltaRequest("GET", "/v2/positions");

// tiny poller to reduce race conditions
async function waitUntilFlat(timeoutMs = 6000, pollMs = 250) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    try {
      const oo = await listOpenOrders();
      const hasOrders = (oo.result ?? oo).length > 0;

      const pos = await listPositions();
      const arr = pos.result ?? pos;
      const hasPos = arr.some(p => Math.abs(Number(p.size || 0)) > 0);

      if (!hasOrders && !hasPos) return true;
    } catch (_) { /* ignore transient errors */ }
    await new Promise(r => setTimeout(r, pollMs));
  }
  return false;
}

// ---------- Health ----------
app.get("/", (req, res) => res.json({ ok: true, service: "tv-relay" }));
app.get("/healthz", (req, res) => res.send("ok"));

// ---------- TradingView webhook ----------
app.post("/tv", async (req, res) => {
  try {
    // Optional shared-secret header
    if (WEBHOOK_TOKEN) {
      const got = req.headers["x-webhook-token"];
      if (got !== WEBHOOK_TOKEN) return res.status(401).json({ ok: false, error: "unauthorized" });
    }

    const payload = req.body || {};
    const action = String(payload.action || "").toUpperCase();
    console.log("TV payload:", JSON.stringify(payload));

    // 1) The three actions your Pine emits for cleanup:
    if (action === "DELTA_CANCEL_ALL" || action === "CANCEL_ALL") {
      const out = await cancelAllOrders();
      return res.json({ ok: true, did: "cancel_all_orders", delta: out });
    }

    if (action === "CLOSE_POSITION") {
      const out = await closeAllPositions();
      return res.json({ ok: true, did: "close_all_positions", delta: out });
    }

    // 2) For sequential safety: before placing any new entry/flip, cancel + wait flat
    if (action === "ENTER" || action === "FLIP") {
      try { await cancelAllOrders(); } catch (_) {}
      await waitUntilFlat(6000, 250); // don’t block forever; proceed after best effort

      // >>>> PLACE YOUR EXISTING ORDER HERE <<<<
      // If you already have code that converts this payload to a Delta order,
      // call it now. Otherwise, just ack so you can add it later.
      return res.json({ ok: true, note: "ENTER/FLIP received (cancelled+waited); forward to your placer" });
    }

    // 3) For your TP/SL builders (DELTA_BRACKET / DELTA_BATCH / EXIT)
    // If you already handle them elsewhere, great. If not, ack:
    if (["DELTA_BRACKET", "DELTA_BATCH", "EXIT"].includes(action)) {
      return res.json({ ok: true, note: `ack ${action}` });
    }

    // Unknown or log-only
    return res.json({ ok: true, ignored: true, action });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

// Render/Heroku-style port
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`tv-relay running on :${PORT}`));
