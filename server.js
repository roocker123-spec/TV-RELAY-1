// server.js (FULL) — LOTS sizing + STRICT chain + TP ladder one-by-one + PROTECTIVE SL/TRAIL
// ✅ Existing fixes preserved
// ✅ Added:
//   1) PLACE_SL_INTENT
//   2) TRAIL_SL_INTENT
//   3) CANCEL_PROTECTIVE
//   4) waits for live position before placing protection
//   5) cancels only old protective orders before replacing SL/trailing
//   6) TP ladder now places ONE-BY-ONE via /v2/orders (not /v2/orders/batch)
//   7) cancels old TP/protective orders before placing fresh TP ladder
//   8) CLOSE_SL — candle-close confirmed SL (software SL from Pine)
//
// ✅ PATCH 2026-03-29: INSTANT WEBHOOK RESPONSE
//   - All webhook handlers now respond 200 immediately to TradingView
//   - Processing happens asynchronously AFTER response
//   - Prevents TradingView "request took too long and timed out" errors
//   - SL, TPs, and all protection orders now reliably reach the server
//   - Queue serialization preserved (CANCAL → ENTER → BATCH_TPS → SL in order)
//
// ✅ PATCH 2026-03-30: CHAIN TTL + SL SYNC + TP VALIDATION
//   FIX 1: CHAIN_TTL bumped 2min → 10min; lastTouch refreshed during long ops
//          Root cause: cleanup was deleting active chains mid-flight when entry
//          took >2min (DEEP/FIL took ~126s), so BATCH_TPS got a blank chain.
//   FIX 2: SL/Trail intent now waits for the entry chain's didEnter flag instead
//          of a blind 10s sleep. Polls up to PROTECTION_WAIT_FOR_ENTRY_MS (180s).
//          This guarantees SL is placed AFTER entry, regardless of entry duration.
//   FIX 3: TP price validation — rejects TPs that would fill instantly at a loss
//          (sell limit below entry for longs, buy limit above entry for shorts).
//
// STRICT sequencing remains ONLY for:
//   CANCAL(seq0) -> ENTER(seq1) -> BATCH_TPS(seq2)
//
// Protection actions are handled OUTSIDE strict seq chain.

require('dotenv').config();
const express = require('express');
const crypto  = require('crypto');
const fetch   = global.fetch; // Node 18+

// -------------------- utils --------------------
function nowTsSec(){ return Math.floor(Date.now()/1000).toString(); }
function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }
function clamp(n,min,max){ return Math.min(Math.max(n,min),max); }
function nnum(x, d=0){ const n = Number(x); return Number.isFinite(n) ? n : d; }

// ✅ robust numeric parser (supports "10 ARC", "0.1 LINK", "100 H", etc.)
function parseNum(v){
  if (v === null || typeof v === 'undefined') return null;
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  const s = String(v).trim();
  const m = s.match(/-?\d+(\.\d+)?/);
  if (!m) return null;
  const n = Number(m[0]);
  return Number.isFinite(n) ? n : null;
}

function toProductSymbol(sym){
  if(!sym) return sym;
  let s = String(sym).replace('.P','');
  if(s.includes(':')) s = s.split(':').pop();
  return s;
}
function isScopeAll(msg){
  return String(msg.scope||'').toUpperCase() === 'ALL' || !!msg.close_all;
}
function safeUpper(x){ return String(x||'').toUpperCase(); }
function sigKey(sigId, psym){ return `${String(sigId||'')}|${safeUpper(psym||'')}`; }
function oppositeSide(side){ return (String(side||'').toLowerCase()==='buy') ? 'sell' : 'buy'; }

// ---------- protection helpers ----------
function protectiveKey(psym){
  return `PROTECT:${safeUpper(toProductSymbol(psym || ''))}`;
}

// ---------- queue (serializes webhook execution) ----------
const QUEUE = new Map(); // key -> Promise chain
function enqueue(key, fn) {
  const prev = QUEUE.get(key) || Promise.resolve();
  const next = prev
    .catch(() => {})
    .then(fn)
    .finally(() => {
      if (QUEUE.get(key) === next) QUEUE.delete(key);
    });
  QUEUE.set(key, next);
  return next;
}

// -------------------- app --------------------
const app = express();
process.env.__STARTED_AT = new Date().toISOString();

// ---------- parsing ----------
app.use(express.json({ type: '*/*' }));
app.use(express.urlencoded({ extended: true }));
app.use((req, _res, next) => {
  if (typeof req.body === 'string') {
    try { req.body = JSON.parse(req.body); }
    catch {
      const qs = require('querystring');
      req.body = qs.parse(req.body);
    }
  }
  if (req.body && typeof req.body.qty !== 'undefined') {
    const q = parseInt(req.body.qty, 10);
    if (!Number.isNaN(q)) req.body.qty = q;
  }
  next();
});

// ---------- env / auth ----------
const API_KEY       = process.env.DELTA_API_KEY || '';
const API_SECRET    = process.env.DELTA_API_SECRET || '';
const BASE_URL      = (process.env.DELTA_BASE || process.env.DELTA_BASE_URL || 'https://api.india.delta.exchange').replace(/\/+$/,'');
const WEBHOOK_TOKEN = process.env.WEBHOOK_TOKEN || '';
const PORT          = process.env.PORT || 3000;

const AUTH_MODE     = (process.env.DELTA_AUTH || 'hmac').toLowerCase();
const HDR_API_KEY   = process.env.DELTA_HDR_API_KEY || 'api-key';
const HDR_SIG       = process.env.DELTA_HDR_SIG     || 'signature';
const HDR_TS        = process.env.DELTA_HDR_TS      || 'timestamp';

// Amount-based sizing defaults
const DEFAULT_LEVERAGE   = nnum(process.env.DEFAULT_LEVERAGE, 10);
const FX_INR_FALLBACK    = nnum(process.env.FX_INR_FALLBACK, 85);
const MARGIN_BUFFER_PCT  = nnum(process.env.MARGIN_BUFFER_PCT, 0.03);
const MAX_LOTS_PER_ORDER = nnum(process.env.MAX_LOTS_PER_ORDER, 200000);

const FLAT_TIMEOUT_MS    = nnum(process.env.FLAT_TIMEOUT_MS, 15000);
const FLAT_POLL_MS       = nnum(process.env.FLAT_POLL_MS, 400);

// ---------- FAST ENTER ----------
const FAST_ENTER = String(process.env.FAST_ENTER || 'false').toLowerCase() === 'true';
const FAST_ENTER_WAIT_MS  = nnum(process.env.FAST_ENTER_WAIT_MS, 2000);
const FAST_ENTER_RETRY_MS = nnum(process.env.FAST_ENTER_RETRY_MS, 8000);

// ---------- STRICT sequence ----------
const STRICT_SEQUENCE = String(process.env.STRICT_SEQUENCE || 'true').toLowerCase() !== 'false';
const SIGNAL_CHAIN_WINDOW_MS = nnum(process.env.SIGNAL_CHAIN_WINDOW_MS, 120_000);

const AUTO_CANCEL_ON_ENTER = String(process.env.AUTO_CANCEL_ON_ENTER || 'false').toLowerCase() === 'true';

// Defaults for cancel step ONLY
const FORCE_CANCEL_ORDERS_ON_CANCEL = String(process.env.FORCE_CANCEL_ORDERS_ON_CANCEL || 'true').toLowerCase() !== 'false';
const FORCE_CLOSE_ON_CANCEL         = String(process.env.FORCE_CLOSE_ON_CANCEL || 'true').toLowerCase() !== 'false';

// ---------- protection / stop management ----------
const POSITION_WAIT_MS   = nnum(process.env.POSITION_WAIT_MS, 30000);
const POSITION_POLL_MS   = nnum(process.env.POSITION_POLL_MS, 350);
const PROTECTIVE_PREFIX  = String(process.env.PROTECTIVE_PREFIX || 'PRT');

// ✅ FIX 2: How long SL/Trail waits for entry chain to set didEnter (default 180s)
const PROTECTION_WAIT_FOR_ENTRY_MS = nnum(process.env.PROTECTION_WAIT_FOR_ENTRY_MS, 180_000);
const PROTECTION_ENTRY_POLL_MS     = nnum(process.env.PROTECTION_ENTRY_POLL_MS, 1000);

// FIX 6: Minimum TP distance from entry (% of entry price)
const MIN_TP_DISTANCE_PCT = nnum(process.env.MIN_TP_DISTANCE_PCT, 0.15);

// ---------- idempotency ----------
const SEEN = new Map();
const SEEN_TTL_MS = 60_000;

function seenKey(msg){
  const sig = String(msg.sig_id || msg.signal_id || '');
  const act = String(msg.action || '').toUpperCase();
  const seq = (typeof msg.seq !== 'undefined') ? String(msg.seq) : '';
  const psym = safeUpper(toProductSymbol(msg.product_symbol || msg.symbol || ''));

  let ordersHash = '';
  if (Array.isArray(msg.orders)) {
    try {
      ordersHash = crypto.createHash('sha1').update(JSON.stringify(msg.orders)).digest('hex');
    } catch { ordersHash = 'orders_hash_fail'; }
  }
  // ✅ FIX 5: Include action in dedup key so different action types
  //   with the same sig_id/psym/seq never collide.
  //   Also prevents retry-dedup from eating the only copy when
  //   the first attempt is still in-flight on a different queue.
  const keyStr = [act, sig, psym, seq, ordersHash].join('|');
  return crypto.createHash('sha1').update(keyStr).digest('hex');
}

function rememberSeen(k){
  SEEN.set(k, Date.now());
  for (const [kk, ts] of SEEN) {
    if (Date.now()-ts > SEEN_TTL_MS) SEEN.delete(kk);
  }
  if (SEEN.size > 300) {
    for (const kk of SEEN.keys()) { SEEN.delete(kk); if (SEEN.size <= 200) break; }
  }
}

// ---------- STRICT sequence state ----------
const SIG_STATE = new Map();
const SIG_STATE_TTL_MS = 10 * 60 * 1000;

function cleanupSigState(){
  const now = Date.now();
  for (const [k,v] of SIG_STATE) {
    if (!v || (now - v.ts) > SIG_STATE_TTL_MS) SIG_STATE.delete(k);
  }
}
function setSigState(sig_id, psym, patch){
  cleanupSigState();
  if (!sig_id || !psym) return;
  const k = sigKey(sig_id, psym);
  const prev = SIG_STATE.get(k) || { lastSeq: -1, ts: 0 };
  SIG_STATE.set(k, { ...prev, ...patch, ts: Date.now() });
}

// -------------------- CHAIN BUFFER --------------------
const CHAIN = new Map();
// ✅ FIX 1: Bumped from 2 min to 10 min — entry chains can take 2+ min on slow fills
const CHAIN_TTL_MS = nnum(process.env.CHAIN_TTL_MS, 10 * 60 * 1000);

function cleanupChain(){
  const now = Date.now();
  for (const [k,v] of CHAIN) {
    if (!v || (now - (v.lastTouch || v.createdAt || now)) > CHAIN_TTL_MS) CHAIN.delete(k);
  }
}
function getChain(sigId, psym){
  cleanupChain();
  if (!sigId || !psym) return null;
  const k = sigKey(sigId, psym);
  let c = CHAIN.get(k);
  if (!c) {
    c = {
      createdAt: Date.now(),
      lastTouch: Date.now(),
      cancelMsg: null,
      enterMsg: null,
      batchMsg: null,
      didCancel: false,
      didEnterPrep: false,
      didEnter: false,
      didBatch: false
    };
    CHAIN.set(k, c);
  } else {
    c.lastTouch = Date.now();
  }
  return c;
}
function upsertChainMsg(sigId, psym, seq, msg){
  const c = getChain(sigId, psym);
  if (!c) return null;
  if (seq === 0) c.cancelMsg = msg;
  if (seq === 1) c.enterMsg  = msg;
  if (seq === 2) c.batchMsg  = msg;
  c.lastTouch = Date.now();
  return c;
}

// ✅ FIX 1 helper: touch chain to prevent TTL expiry during long operations
function touchChain(sigId, psym) {
  if (!sigId || !psym) return;
  const k = sigKey(sigId, psym);
  const c = CHAIN.get(k);
  if (c) c.lastTouch = Date.now();
}

// ✅ FIX 2 helper: peek at chain's didEnter without creating/touching it
function peekChainDidEnter(sigId, psym) {
  if (!sigId || !psym) return false;
  const k = sigKey(sigId, psym);
  const c = CHAIN.get(k);
  return !!(c && c.didEnter);
}

// ---------- Delta request helper ----------
async function dcall(method, path, payload=null, query='') {
  const body = payload ? JSON.stringify(payload) : '';
  const MAX_TRIES = 3;

  for (let attempt = 1; attempt <= MAX_TRIES; attempt++) {
    const ts   = nowTsSec();
    const url  = BASE_URL + path + (query||'');
    const headers = {
      'Content-Type':'application/json',
      'Accept':'application/json',
      'User-Agent':'tv-relay-node'
    };

    if (AUTH_MODE === 'hmac') {
      const prehash = method + ts + path + (query||'') + body;
      const signature = crypto.createHmac('sha256', API_SECRET).update(prehash).digest('hex');
      headers[HDR_API_KEY] = API_KEY;
      headers[HDR_SIG]     = signature;
      headers[HDR_TS]      = ts;
    } else {
      headers[HDR_API_KEY] = API_KEY;
    }

    try {
      const res  = await fetch(url,{ method, headers, body: body || undefined });
      const text = await res.text(); let json;
      try { json = JSON.parse(text); } catch { json = { raw: text }; }

      if (!res.ok || json?.success === false) {
        const code = Number(json?.error?.code || res.status);
        if ([429,500,502,503,504].includes(code) && attempt < MAX_TRIES) {
          await sleep(300*attempt);
          continue;
        }
        throw new Error(`Delta API error: ${JSON.stringify({ method, url, status: res.status, json })}`);
      }
      return json;
    } catch (e) {
      if (attempt === MAX_TRIES) throw e;
      await sleep(300*attempt);
    }
  }
}

// ---------- product helpers ----------
let _products = null, _products_ts = 0;
async function getProducts(){
  const STALE_MS = 5*60*1000;
  if(!_products || (Date.now()-_products_ts) > STALE_MS){
    const r = await dcall('GET','/v2/products');
    const arr = Array.isArray(r?.result) ? r.result
            : Array.isArray(r?.products) ? r.products
            : Array.isArray(r) ? r : [];
    _products = arr;
    _products_ts = Date.now();
  }
  return _products;
}
async function getProductMeta(product_symbol){
  const ps = String(product_symbol||'').toUpperCase();
  const list = await getProducts();
  return list.find(p => String(p?.symbol||p?.product_symbol||'').toUpperCase() === ps);
}
async function getProductIdBySymbol(psym){
  const meta = await getProductMeta(psym);
  const pid = meta?.id ?? meta?.product_id;
  return Number.isFinite(+pid) ? +pid : null;
}

const LOT_MULT_CACHE = new Map();

function lotMultiplierFromMeta(meta){
  const candidates = [
    meta?.lot_size,
    meta?.contract_size,
    meta?.contract_value,
    meta?.contract_unit,
  ];
  for (const v of candidates) {
    const n = parseNum(v);
    if (n && n > 0) return n;
  }
  const step = parseNum(meta?.qty_step);
  if (step && step > 0 && step >= 1) return step;
  return 1;
}

function getCachedLotMult(psym){ return LOT_MULT_CACHE.get(psym)?.m || null; }
function setCachedLotMult(psym, m){
  const n = Number(m);
  if (Number.isFinite(n) && n > 0 && n <= 1e9) LOT_MULT_CACHE.set(psym,{m: n, ts: Date.now()});
}

async function getLotMult(psym){
  psym = String(psym||'').toUpperCase();
  const cached = getCachedLotMult(psym);
  if (cached) return cached;

  const meta = await getProductMeta(psym);
  const m = lotMultiplierFromMeta(meta);

  setCachedLotMult(psym, m);

  console.log('lotMult resolved', {
    psym,
    lotMult: m,
    lot_size: meta?.lot_size,
    contract_size: meta?.contract_size,
    contract_value: meta?.contract_value,
    contract_unit: meta?.contract_unit,
    qty_step: meta?.qty_step
  });

  return m;
}

async function getTickerPriceUSD(psym){
  try {
    const q = `?symbol=${encodeURIComponent(psym)}`;
    const r = await dcall('GET','/v2/tickers', null, q);
    const arr = Array.isArray(r?.result) ? r.result : Array.isArray(r) ? r : [];
    const t = arr.find(x => (x?.symbol||x?.product_symbol)===psym);
    const px = nnum(t?.mark_price || t?.last_price || t?.index_price, 0);
    return px > 0 ? px : null;
  } catch { return null; }
}

// ---------- sizing ----------
function lotsFromAmount({ amount, ccy='INR', leverage=DEFAULT_LEVERAGE, entryPxUSD, lotMult=1, fxInrPerUsd=FX_INR_FALLBACK }){
  leverage = Math.max(1, Math.floor(nnum(leverage, DEFAULT_LEVERAGE)));
  lotMult  = Math.max(1e-12, nnum(lotMult, 1));

  const fx  = nnum(fxInrPerUsd, FX_INR_FALLBACK);
  const px  = nnum(entryPxUSD, 0);
  const amt = nnum(amount, 0);
  if (amt <= 0 || px <= 0) return 0;

  const marginUSD   = (ccy.toUpperCase()==='USD') ? amt : (amt / fx);
  const notionalUSD = marginUSD * leverage * (1 - MARGIN_BUFFER_PCT);
  const coinsWanted = notionalUSD / px;

  const lots = Math.floor(coinsWanted / lotMult);
  return Math.max(1, lots);
}

// ---------- last entry side ----------
const LAST_SIDE = new Map();
function rememberSide(productSymbol, side){
  if (!productSymbol) return;
  const s = String(side||'').toLowerCase()==='buy' ? 'buy' : 'sell';
  LAST_SIDE.set(productSymbol, s);
}

// ---------- positions ----------
async function listPositionsArray(){
  try {
    const pos = await dcall('GET','/v2/positions');
    const arr = Array.isArray(pos?.result?.positions) ? pos.result.positions
              : Array.isArray(pos?.result) ? pos.result
              : Array.isArray(pos?.positions) ? pos.positions
              : Array.isArray(pos) ? pos : [];
    if (arr.length || pos?.success !== false) return arr;
  } catch(e) {}
  try {
    const pos2 = await dcall('GET','/v2/positions/margined');
    const arr2 = Array.isArray(pos2?.result?.positions) ? pos2.result.positions
               : Array.isArray(pos2?.result) ? pos2.result
               : Array.isArray(pos2?.positions) ? pos2.positions
               : Array.isArray(pos2) ? pos2 : [];
    if (arr2.length) return arr2;
  } catch(e) {}
  return [];
}

async function inferPositionUnits({ psym, rawSize, lotMult, posRow }){
  const abs = Math.abs(Number(rawSize || 0));
  if (!(abs > 0)) return { units: 'unknown', lots: 0 };

  const notional = nnum(posRow?.notional || posRow?.position_notional || posRow?.value || 0, 0);
  const px = nnum(posRow?.mark_price || posRow?.entry_price || 0, 0) || nnum(await getTickerPriceUSD(psym), 0);

  if (notional > 0 && px > 0) {
    const coinsEst = notional / px;
    const lotsEst  = coinsEst / lotMult;

    const rel = (a,b)=> Math.abs(a-b) / Math.max(1e-9, Math.abs(b));

    if (rel(abs, lotsEst) < 0.25) return { units:'lots',  lots: Math.max(1, Math.round(abs)) };
    if (rel(abs, coinsEst) < 0.25) return { units:'coins', lots: Math.max(1, Math.floor(coinsEst / lotMult)) };
  }

  if (lotMult > 1 && Number.isInteger(abs) && (abs % lotMult) !== 0) return { units:'lots', lots: Math.max(1, Math.round(abs)) };
  if (lotMult > 1 && abs > MAX_LOTS_PER_ORDER) return { units:'coins', lots: Math.max(1, Math.floor(abs / lotMult)) };
  if (lotMult > 1) return { units:'coins', lots: Math.max(1, Math.floor(abs / lotMult)) };

  return { units:'lots', lots: Math.max(1, Math.round(abs)) };
}

async function getPositionCloseSideAndLots(psym){
  const sym = safeUpper(toProductSymbol(psym));
  const pos = await listPositionsArray();
  const row = pos.find(p => safeUpper(p?.product_symbol || p?.symbol) === sym);

  const rawSize = Number(row?.size || row?.position_size || 0);
  if (!rawSize || Math.abs(rawSize) < 1e-12) {
    return { hasPos:false, closeSide:null, lots:0, rawSize:0, row:null };
  }

  const lotMult = await getLotMult(sym);
  const inferred = await inferPositionUnits({ psym: sym, rawSize, lotMult, posRow: row });

  const closeSide = rawSize > 0 ? 'sell' : 'buy';
  return { hasPos:true, closeSide, lots: inferred.lots, rawSize, row };
}

// ---------- order helpers ----------
const LAST_ENTRY_SENT = new Map();

async function learnLotMultFromPositions(psym){
  const last = LAST_ENTRY_SENT.get(psym);
  if (!last || (Date.now()-last.ts) > 15_000) return;

  try {
    const pos = await listPositionsArray();
    const row = pos.find(p => String(p?.product_symbol||p?.symbol||'').toUpperCase() === psym.toUpperCase());
    if (!row) return;

    const lotMultMeta = await getLotMult(psym);
    const rawSize = Number(row.size||row.position_size||0);
    const inferred = await inferPositionUnits({ psym, rawSize, lotMult: lotMultMeta, posRow: row });

    const coinsAbs = inferred.units === 'lots'
      ? Math.abs(rawSize) * lotMultMeta
      : Math.abs(rawSize);

    const lotsSent = Math.max(1, Number(last.lots||0));
    if (!coinsAbs || !lotsSent) return;

    const m = coinsAbs / lotsSent;
    const nearInt = Math.abs(m - Math.round(m)) < 1e-6;

    if ((nearInt && Math.round(m) >= 1) || (m > 0 && m < 1)) {
      const learned = nearInt ? Math.round(m) : m;

      if (Math.abs(learned - lotMultMeta) / Math.max(1, lotMultMeta) < 0.5) {
        setCachedLotMult(psym, learned);
        console.log('learned lot multiplier', { product_symbol: psym, learned, coinsAbs, lotsSent, inferred_units: inferred.units });
      } else {
        console.log('learn rejected (conflicts with meta)', { psym, learned, lotMultMeta, inferred_units: inferred.units });
      }

      LAST_ENTRY_SENT.delete(psym);
    }
  } catch {}
}

async function maxLotsFromMsgBudget(m, product_symbol, lotMult){
  const fxHint   = nnum(m.fxQuoteToINR || m.fx_quote_to_inr || m.fx || FX_INR_FALLBACK, FX_INR_FALLBACK);
  const leverage = Math.max(1, Math.floor(nnum(m.leverage || m.leverage_x || DEFAULT_LEVERAGE, DEFAULT_LEVERAGE)));
  const ccy      = String(m.amount_ccy || m.ccy || (typeof m.amount_usd !== 'undefined' ? 'USD' : 'INR')).toUpperCase();

  let amount = null;
  if (typeof m.amount_inr !== 'undefined') amount = nnum(m.amount_inr, 0);
  else if (typeof m.amount_usd !== 'undefined') amount = nnum(m.amount_usd, 0);
  else if (typeof m.order_amount !== 'undefined') amount = nnum(m.order_amount, 0);
  else if (typeof m.amount !== 'undefined') amount = nnum(m.amount, 0);

  if (!(amount > 0)) return null;

  let entryPxUSD = nnum(m.entry, 0);
  if (!(entryPxUSD > 0)) entryPxUSD = nnum(await getTickerPriceUSD(product_symbol), 0);
  if (!(entryPxUSD > 0)) return null;

  const lots = lotsFromAmount({ amount, ccy, leverage, entryPxUSD, lotMult, fxInrPerUsd: fxHint });
  return clamp(lots, 1, MAX_LOTS_PER_ORDER);
}

async function placeEntry(m){
  const side = (m.side||'').toLowerCase()==='buy' ? 'buy' : 'sell';
  const product_symbol = toProductSymbol(m.symbol || m.product_symbol);
  const lotMult = await getLotMult(product_symbol);

  let sizeLots = parseInt(m.qty,10);
  let usedMode = 'qty';

  const budgetMaxLots = await maxLotsFromMsgBudget(m, product_symbol, lotMult);
  if (Number.isFinite(budgetMaxLots) && budgetMaxLots > 0) {
    if (!sizeLots || sizeLots < 1) {
      sizeLots = budgetMaxLots;
      usedMode = 'amount_budget';
    } else {
      const before = sizeLots;
      sizeLots = Math.min(sizeLots, budgetMaxLots);
      usedMode = (sizeLots !== before) ? 'qty_clamped_to_budget' : 'qty_within_budget';
    }
  } else {
    if (!sizeLots || sizeLots < 1) {
      const fxHint   = nnum(m.fxQuoteToINR || m.fx_quote_to_inr || m.fx || FX_INR_FALLBACK, FX_INR_FALLBACK);
      const leverage = Math.max(1, Math.floor(nnum(m.leverage || m.leverage_x || DEFAULT_LEVERAGE, DEFAULT_LEVERAGE)));
      const ccy      = String(m.amount_ccy || m.ccy || (typeof m.amount_usd !== 'undefined' ? 'USD' : 'INR')).toUpperCase();

      let entryPxUSD = nnum(m.entry, 0);
      if (!(entryPxUSD > 0)) entryPxUSD = nnum(await getTickerPriceUSD(product_symbol), 0);
      if (!(entryPxUSD > 0)) throw new Error(`No price available for ${product_symbol}`);

      let amount = undefined;
      if (typeof m.amount_inr   !== 'undefined') amount = nnum(m.amount_inr, 0);
      else if (typeof m.amount_usd !== 'undefined') amount = nnum(m.amount_usd, 0);
      else if (typeof m.order_amount !== 'undefined') amount = nnum(m.order_amount, 0);
      else if (typeof m.amount !== 'undefined') amount = nnum(m.amount, 0);

      if (!(amount > 0)) throw new Error('Entry requires qty OR amount_inr/amount_usd/order_amount/amount');

      sizeLots = lotsFromAmount({ amount, ccy, leverage, entryPxUSD, lotMult, fxInrPerUsd: fxHint });
      usedMode = `${ccy==='USD'?'amount_usd':'amount_inr'}`;

      console.log('amount sizing debug', { product_symbol, amount, ccy, leverage, entryPxUSD, lotMult, sizeLots });
    }
  }

  sizeLots = clamp(sizeLots, 1, MAX_LOTS_PER_ORDER);

  console.log('entry size normalization', { product_symbol, side, lotMult, usedMode, sizeLots, budgetMaxLots });

  const out = await dcall('POST','/v2/orders',{
    product_symbol,
    order_type:'market_order',
    side,
    size: sizeLots
  });

  rememberSide(product_symbol, side);
  LAST_ENTRY_SENT.set(product_symbol, { lots: sizeLots, ts: Date.now(), side, lotMult });
  learnLotMultFromPositions(product_symbol).catch(()=>{});
  return out;
}

// -------------------- TP SIZE NORMALIZATION --------------------
function normalizeTpSizeLots({ psym, lotMult, order, lastEntry }) {
  const coins = nnum(order?.size_coins ?? order?.coins, 0);
  if (coins > 0) return Math.max(1, Math.floor(coins / lotMult));

  const s = nnum(order?.size, 0);
  if (!(s > 0)) return 0;

  const sInt = Number.isInteger(s);
  const lastLots = lastEntry?.lots ? nnum(lastEntry.lots, 0) : 0;
  const lastCoins = (lastLots > 0) ? (lastLots * lotMult) : 0;

  if (lotMult > 1 && sInt && s >= lotMult && (s % lotMult) === 0) {
    return Math.max(1, Math.floor(s / lotMult));
  }

  if (sInt && lastLots > 0 && s <= Math.max(lastLots, 1) * 2) return Math.max(1, Math.round(s));
  if (lastCoins > 0 && s >= Math.max(lastCoins * 0.5, lotMult * 2)) return Math.max(1, Math.floor(s / lotMult));
  if (lotMult > 1 && sInt && (s % lotMult) !== 0) return Math.max(1, Math.round(s));
  if (lotMult > 1 && s > MAX_LOTS_PER_ORDER) return Math.max(1, Math.floor(s / lotMult));

  if (lotMult > 1) return Math.max(1, Math.round(s));
  return Math.max(1, Math.round(s));
}

function shortClientOrderId(sigId, psym, idx){
  const base = `${String(sigId||'')}|${String(psym||'')}|TP|${idx}|${Date.now()}`;
  const h = crypto.createHash('sha1').update(base).digest('hex');
  const p = String(psym || '').toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,6);
  return `T${idx}${p}_${h.slice(0,22)}`.slice(0,32);
}

function protectiveClientOrderId(kind, sigId, psym){
  const base = `${PROTECTIVE_PREFIX}|${kind}|${String(sigId||'')}|${String(psym||'')}|${Date.now()}`;
  const h = crypto.createHash('sha1').update(base).digest('hex');
  const p = String(psym || '').toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,6);
  return `${PROTECTIVE_PREFIX}_${kind}_${p}_${h.slice(0,12)}`.slice(0,32);
}

function isProtectiveOrder(o){
  const cid = String(o?.client_order_id || '');
  return cid.startsWith(`${PROTECTIVE_PREFIX}_`);
}

// ✅ FIX 5b: isTpLikeOrder matches ONLY TP orders (T0-T5/TP prefix).
//   Protective orders (PRT_SL_, PRT_TRL_) are NOT included here.
//   This prevents BATCH_TPS from accidentally cancelling SL/trail orders
//   that were placed concurrently by the PROTECT queue.
function isTpLikeOrder(o){
  const cid = String(o?.client_order_id || '');
  return (
    cid.startsWith('T0') ||
    cid.startsWith('T1') ||
    cid.startsWith('T2') ||
    cid.startsWith('T3') ||
    cid.startsWith('T4') ||
    cid.startsWith('T5') ||
    cid.startsWith('TP')
  );
}

function clampBatchLotsToPosition(preOrders, positionLots){
  if (!(positionLots > 0)) return preOrders;

  if (positionLots < preOrders.length) {
    return preOrders.slice(0, positionLots).map(o => ({
      ...o,
      sizeLots: 1
    }));
  }

  const sum = preOrders.reduce((a,x)=>a + (x.sizeLots||0), 0);
  if (sum <= positionLots) return preOrders;

  const scale = positionLots / sum;

  let scaled = preOrders.map(o => ({
    ...o,
    sizeLots: Math.max(1, Math.floor(o.sizeLots * scale))
  }));

  let newSum = scaled.reduce((a,x)=>a + x.sizeLots, 0);

  let i = 0;
  while (newSum < positionLots && scaled.length) {
    scaled[i % scaled.length].sizeLots += 1;
    newSum += 1;
    i++;
    if (i > 10000) break;
  }

  i = 0;
  while (newSum > positionLots && scaled.length) {
    const idx = i % scaled.length;
    if (scaled[idx].sizeLots > 1) {
      scaled[idx].sizeLots -= 1;
      newSum -= 1;
    }
    i++;
    if (i > 10000) break;
  }

  return scaled;
}

async function cancelTpOrdersBySymbol(psym){
  const sym = toProductSymbol(psym);
  if (!sym) return { ok:true, skipped:true, reason:'missing_symbol' };

  let open = [];
  try {
    open = await listOpenOrdersAllPages();
  } catch(e) {
    throw new Error(`cancelTpOrdersBySymbol list failed: ${e?.message || e}`);
  }

  const mine = open.filter(o =>
    safeUpper(o?.product_symbol || o?.symbol) === safeUpper(sym) &&
    isTpLikeOrder(o)
  );

  if (!mine.length) {
    return { ok:true, skipped:true, reason:'no_tp_or_protective_orders', symbol:sym };
  }

  let cancelled = 0, failed = 0;
  for (const o of mine){
    const oid  = o?.id ?? o?.order_id;
    const pid  = o?.product_id;
    const coid = o?.client_order_id;
    try {
      await cancelOrder({ id: oid, client_order_id: coid, product_id: pid, product_symbol: sym });
      cancelled++;
    } catch(e){
      failed++;
      console.warn('cancelTpOrdersBySymbol failed', { sym, oid, coid, err: e?.message || e });
    }
  }

  return { ok:true, cancelled, failed, symbol:sym };
}

async function placeBatch(m){
  const psym =
    toProductSymbol(m.product_symbol || m.symbol) ||
    toProductSymbol(m?.orders?.[0]?.product_symbol);

  if (!psym) throw new Error('placeBatch: missing product_symbol/symbol');
  if (!Array.isArray(m.orders) || !m.orders.length) throw new Error('placeBatch: missing orders[]');

  const lotMult = await getLotMult(psym);
  const sigId = String(m.sig_id || m.signal_id || 'nosig');
  const lastEntry = LAST_ENTRY_SENT.get(psym) || null;

  const posInfo = await getPositionCloseSideAndLots(psym);
  if (!posInfo.hasPos) throw new Error(`placeBatch: no open position for ${psym}`);

  // ✅ FIX: Use known entry lots from LAST_ENTRY_SENT instead of re-inferring
  const knownEntry = LAST_ENTRY_SENT.get(psym);
  if (knownEntry?.lots > 0 && (Date.now() - knownEntry.ts) < 5 * 60 * 1000) {
    if (knownEntry.lots !== posInfo.lots) {
      console.log('⚠ placeBatch LOT FIX: overriding inferred lots with known entry lots', {
        psym,
        inferredLots: posInfo.lots,
        knownEntryLots: knownEntry.lots,
        entrySide: knownEntry.side,
        entryAge: `${Math.round((Date.now() - knownEntry.ts)/1000)}s ago`
      });
    }
    posInfo.lots = knownEntry.lots;
  }

  const tpSide = posInfo.closeSide;
  const positionLots = posInfo.lots;

  // ✅ FIX 3: Get entry price for TP validation
  const entryPrice = nnum(posInfo.row?.entry_price, 0) || nnum(m.entry, 0);
  const isLong = (tpSide === 'sell');  // long → close side is sell

  await cancelTpOrdersBySymbol(psym);

  let pre = [];
  let skippedTps = [];

  for (let idx = 0; idx < Math.min(m.orders.length, 50); idx++) {
    const o = m.orders[idx];
    const oo = { ...o };

    const sizeLots = normalizeTpSizeLots({ psym, lotMult, order: oo, lastEntry });
    if (!sizeLots) throw new Error(`placeBatch: bad size on order #${idx}`);

    if (!oo.limit_price && (oo.price || oo.lmt_price)) oo.limit_price = oo.price || oo.lmt_price;
    if (typeof oo.limit_price !== 'undefined') oo.limit_price = String(oo.limit_price);
    if (!oo.limit_price) throw new Error(`placeBatch: missing limit_price on order #${idx}`);

    // FIX 3 + FIX 6: Validate TP price — reject wrong-side AND too-close TPs
    const tpPrice = nnum(oo.limit_price, 0);
    if (entryPrice > 0 && tpPrice > 0) {
      if (isLong && tpPrice < entryPrice) {
        console.warn(`⚠ TP PRICE REJECTED [${psym}] order #${idx}: sell limit ${tpPrice} < entry ${entryPrice} (long). Would fill at a loss. SKIPPING.`);
        skippedTps.push({ idx, limit_price: oo.limit_price, sizeLots, reason: 'sell_limit_below_entry_for_long', entryPrice });
        continue;
      }
      if (!isLong && tpPrice > entryPrice) {
        console.warn(`⚠ TP PRICE REJECTED [${psym}] order #${idx}: buy limit ${tpPrice} > entry ${entryPrice} (short). Would fill at a loss. SKIPPING.`);
        skippedTps.push({ idx, limit_price: oo.limit_price, sizeLots, reason: 'buy_limit_above_entry_for_short', entryPrice });
        continue;
      }
      // FIX 6: TP too close to entry = useless (like COOKIE where entry = TP1)
      const distPct = Math.abs(tpPrice - entryPrice) / entryPrice * 100;
      if (distPct < MIN_TP_DISTANCE_PCT) {
        console.warn(`⚠ TP TOO CLOSE [${psym}] order #${idx}: ${tpSide} limit ${tpPrice} is only ${distPct.toFixed(3)}% from entry ${entryPrice} (min ${MIN_TP_DISTANCE_PCT}%). SKIPPING.`);
        skippedTps.push({ idx, limit_price: oo.limit_price, sizeLots, reason: 'tp_too_close_to_entry', distPct: +distPct.toFixed(4), minPct: MIN_TP_DISTANCE_PCT, entryPrice });
        continue;
      }
    }
    

    if (!oo.client_order_id) oo.client_order_id = shortClientOrderId(sigId, psym, idx);
    oo.client_order_id = String(oo.client_order_id).slice(0, 32);

    pre.push({
      idx,
      limit_price: oo.limit_price,
      sizeLots: Math.max(1, parseInt(sizeLots, 10)),
      client_order_id: oo.client_order_id,
      post_only: (typeof oo.post_only !== 'undefined') ? !!oo.post_only : undefined,
      mmp: (typeof oo.mmp !== 'undefined') ? !!oo.mmp : undefined
    });
  }

  if (skippedTps.length) {
    console.warn(`⚠ TP VALIDATION: ${skippedTps.length} TPs skipped for ${psym}`, { skippedTps, entryPrice, isLong });

    // FIX 6: Redistribute skipped TP lots to last remaining TP (furthest from entry)
    const skippedLots = skippedTps.reduce((a, s) => a + (s.sizeLots || 0), 0);
    if (skippedLots > 0 && pre.length > 0) {
      const lastIdx = pre.length - 1;
      pre[lastIdx].sizeLots += skippedLots;
      console.log(`⚠ TP REDISTRIBUTION [${psym}]: ${skippedLots} lots from ${skippedTps.length} skipped TPs → added to TP#${pre[lastIdx].idx} (${pre[lastIdx].limit_price})`, {
        newSize: pre[lastIdx].sizeLots,
        limit_price: pre[lastIdx].limit_price
      });
    }
  }

  if (!pre.length) {
    throw new Error(`placeBatch: all TPs rejected by price validation for ${psym} (entry=${entryPrice}, isLong=${isLong}). Skipped: ${JSON.stringify(skippedTps)}`);
  }

  pre = clampBatchLotsToPosition(pre, positionLots);

  const sumLots = pre.reduce((a,o)=>a + Number(o.sizeLots || 0), 0);

  console.log('TP ladder placing one-by-one', {
    psym,
    lotMult,
    positionLots,
    tpSide,
    sumLots,
    first3: pre.slice(0,3),
    skippedCount: skippedTps.length
  });

  if (sumLots > positionLots) {
    throw new Error(`TP safety: refusing to place sumLots=${sumLots} > positionLots=${positionLots} for ${psym}`);
  }

  const placed = [];
  const failed = [];

  for (const x of pre) {
    const body = {
      product_symbol: psym,
      order_type: 'limit_order',
      side: tpSide,
      size: x.sizeLots,
      limit_price: x.limit_price,
      reduce_only: true,
      time_in_force: 'gtc',
      client_order_id: x.client_order_id
    };

    if (typeof x.post_only !== 'undefined') body.post_only = x.post_only;
    if (typeof x.mmp !== 'undefined') body.mmp = x.mmp;

    try {
      const r = await dcall('POST', '/v2/orders', body);

      console.log('TP single placed raw', {
        psym,
        idx: x.idx,
        request: body,
        response: r?.result || r
      });

      placed.push({
        idx: x.idx,
        size: x.sizeLots,
        limit_price: x.limit_price,
        client_order_id: x.client_order_id,
        result: r?.result || r
      });
    } catch (e) {
      console.warn('TP single place failed', {
        psym,
        idx: x.idx,
        size: x.sizeLots,
        limit_price: x.limit_price,
        client_order_id: x.client_order_id,
        err: e?.message || e
      });

      failed.push({
        idx: x.idx,
        size: x.sizeLots,
        limit_price: x.limit_price,
        client_order_id: x.client_order_id,
        error: String(e?.message || e)
      });
    }

    await sleep(120);
  }

  console.log('TP ladder one-by-one result', {
    psym,
    placed: placed.length,
    failed: failed.length,
    skipped: skippedTps.length,
    placed_first3: placed.slice(0, 3),
    failed_first3: failed.slice(0, 3)
  });

  if (!placed.length) {
    throw new Error(`placeBatch: all TP placements failed for ${psym}`);
  }

  return { ok:true, mode:'single_orders', placed, failed, skippedTps };
}

// ---------- CANCEL/CLOSE ----------
const cancelAllOrders   = () => dcall('DELETE','/v2/orders/all');
const closeAllPositions = () => dcall('POST','/v2/positions/close_all', {});

async function cancelOrder({ id, client_order_id, product_id, product_symbol }){
  const payload = {};
  if (Number.isFinite(+id)) payload.id = +id;
  if (client_order_id) payload.client_order_id = String(client_order_id);

  let pid = Number.isFinite(+product_id) ? +product_id : null;
  if (!pid && product_symbol) pid = await getProductIdBySymbol(product_symbol);

  if (!pid) throw new Error(`cancelOrder: missing product_id (id=${id}, client_order_id=${client_order_id}, product_symbol=${product_symbol})`);
  payload.product_id = pid;

  if (!payload.id && !payload.client_order_id) return { ok:true, skipped:true, reason:'missing_id_and_client_order_id' };
  return dcall('DELETE', '/v2/orders', payload);
}

async function listOpenOrdersAllPages(){
  let all = [];
  let after = null;

  while (true) {
    const q =
      `?states=open,pending&page_size=200` +
      (after ? `&after=${encodeURIComponent(after)}` : '');

    const oo = await dcall('GET','/v2/orders', null, q);

    const arr = Array.isArray(oo?.result) ? oo.result
              : Array.isArray(oo?.orders) ? oo.orders
              : Array.isArray(oo) ? oo : [];

    all = all.concat(arr);

    const nextAfter = oo?.meta?.after || null;
    if (!nextAfter || arr.length === 0) break;
    after = nextAfter;
  }

  return all;
}

async function cancelOrdersBySymbol(psym, { fallbackAll=false, skipProtective=false } = {}){
  const sym = toProductSymbol(psym);
  if (!sym) return { ok:true, skipped:true, reason:'missing_symbol' };

  let open = [];
  try { open = await listOpenOrdersAllPages(); }
  catch(e) {
    if (fallbackAll) { await cancelAllOrders(); return { ok:true, fallback:'cancel_all_orders' }; }
    throw e;
  }

  const mine = open.filter(o =>
    safeUpper(o?.product_symbol||o?.symbol) === safeUpper(sym) &&
    (!skipProtective || !isProtectiveOrder(o))
  );
  if (!mine.length) return { ok:true, skipped:true, reason:'no_open_orders_for_symbol', symbol: sym };

  let cancelled = 0, failed = 0;
  for (const o of mine){
    const oid = o?.id ?? o?.order_id;
    const pid = o?.product_id;
    const coid = o?.client_order_id;

    try {
      await cancelOrder({ id: oid, client_order_id: coid, product_id: pid, product_symbol: sym });
      cancelled++;
    } catch(e){
      failed++;
      console.warn('cancelOrdersBySymbol: cancel failed', { symbol: sym, oid, pid, err: e?.message || e });
    }
  }

  if (failed && fallbackAll) {
    await cancelAllOrders();
    return { ok:true, cancelled, failed, fallback:'cancel_all_orders' };
  }

  return { ok:true, cancelled, failed, symbol: sym };
}

async function cancelProtectiveOrdersBySymbol(psym, skipClientOrderId = null){
  const sym = toProductSymbol(psym);
  if (!sym) return { ok:true, skipped:true, reason:'missing_symbol' };

  let open = [];
  try {
    open = await listOpenOrdersAllPages();
  } catch(e) {
    throw new Error(`cancelProtectiveOrdersBySymbol list failed: ${e?.message || e}`);
  }

  const mine = open.filter(o =>
    safeUpper(o?.product_symbol || o?.symbol) === safeUpper(sym) &&
    isProtectiveOrder(o) &&
    // ✅ FIX 4: Skip the order we JUST placed (don't cancel our own new protection)
    (!skipClientOrderId || String(o?.client_order_id || '') !== String(skipClientOrderId))
  );

  if (!mine.length) {
    return { ok:true, skipped:true, reason:'no_protective_orders', symbol:sym };
  }

  let cancelled = 0, failed = 0;
  for (const o of mine){
    const oid  = o?.id ?? o?.order_id;
    const pid  = o?.product_id;
    const coid = o?.client_order_id;
    try {
      await cancelOrder({ id: oid, client_order_id: coid, product_id: pid, product_symbol: sym });
      cancelled++;
    } catch(e){
      failed++;
      console.warn('cancelProtectiveOrdersBySymbol failed', { sym, oid, coid, err: e?.message || e });
    }
  }

  return { ok:true, cancelled, failed, symbol:sym };
}

async function closePositionBySymbol(symbolOrProductSymbol){
  const psym = toProductSymbol(symbolOrProductSymbol);
  if (!psym) throw new Error('closePositionBySymbol: missing symbol/product_symbol');

  const pos = await listPositionsArray();
  const row = pos.find(p => safeUpper(p?.product_symbol || p?.symbol) === safeUpper(psym));

  const rawSize = Number(row?.size || row?.position_size || 0);
  if (!rawSize || Math.abs(rawSize) < 1e-12) {
    console.log('closePositionBySymbol: no open position for', psym);
    return { ok:true, skipped:true, reason:'no_position' };
  }

  const lotMult = await getLotMult(psym);
  const inferred = await inferPositionUnits({ psym, rawSize, lotMult, posRow: row });

  let lots = clamp(inferred.lots, 1, MAX_LOTS_PER_ORDER);
  const side = rawSize > 0 ? 'sell' : 'buy';

  console.log('closePositionBySymbol:', { psym, rawSize, lotMult, inferred_units: inferred.units, lots, side });

  return dcall('POST','/v2/orders',{
    product_symbol: psym,
    order_type: 'market_order',
    side,
    size: lots,
    reduce_only: true
  });
}

// ---------- flat checks ----------
async function waitUntilFlat(timeoutMs = FLAT_TIMEOUT_MS, pollMs = FLAT_POLL_MS) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    try {
      const oo  = await listOpenOrdersAllPages();
      const hasOrders = oo.some(o => ['open','pending','triggered','untriggered']
        .includes(String(o?.state||o?.status||'').toLowerCase()));
      const pos = await listPositionsArray();
      const hasPos = pos.some(p => Math.abs(Number(p?.size||p?.position_size||0)) > 0);
      if (!hasOrders && !hasPos) return true;
    } catch(e) {}
    await sleep(pollMs);
  }
  return false;
}

async function waitUntilFlatSymbol(psym, timeoutMs = FLAT_TIMEOUT_MS, pollMs = FLAT_POLL_MS) {
  const sym = toProductSymbol(psym);
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    try {
      const oo  = await listOpenOrdersAllPages();
      const mineOrders = oo.filter(o => safeUpper(o?.product_symbol||o?.symbol) === safeUpper(sym));
      const hasOrders = mineOrders.some(o => ['open','pending','triggered','untriggered']
        .includes(String(o?.state||o?.status||'').toLowerCase()));

      const pos = await listPositionsArray();
      const minePos = pos.find(p => safeUpper(p?.product_symbol||p?.symbol) === safeUpper(sym));
      const hasPos = minePos ? (Math.abs(Number(minePos?.size||minePos?.position_size||0)) > 0) : false;

      if (!hasOrders && !hasPos) return true;
    } catch(e) {}
    await sleep(pollMs);
  }
  return false;
}

async function isFlatNowGlobal(){
  try {
    const oo  = await listOpenOrdersAllPages();
    const hasOrders = oo.some(o => ['open','pending','triggered','untriggered']
      .includes(String(o?.state||o?.status||'').toLowerCase()));
    const pos = await listPositionsArray();
    const hasPos = pos.some(p => Math.abs(Number(p?.size||p?.position_size||0)) > 0);
    return !hasOrders && !hasPos;
  } catch { return false; }
}
async function isFlatNowSymbol(psym){
  const sym = toProductSymbol(psym);
  try {
    const oo  = await listOpenOrdersAllPages();
    const mineOrders = oo.filter(o => safeUpper(o?.product_symbol||o?.symbol) === safeUpper(sym));
    const hasOrders = mineOrders.some(o => ['open','pending','triggered','untriggered']
      .includes(String(o?.state||o?.status||'').toLowerCase()));

    const pos = await listPositionsArray();
    const minePos = pos.find(p => safeUpper(p?.product_symbol||p?.symbol) === safeUpper(sym));
    const hasPos = minePos ? (Math.abs(Number(minePos?.size||minePos?.position_size||0)) > 0) : false;

    return !hasOrders && !hasPos;
  } catch { return false; }
}

async function waitUntilPositionSymbol(psym, timeoutMs = POSITION_WAIT_MS, pollMs = POSITION_POLL_MS){
  const sym = toProductSymbol(psym);
  const end = Date.now() + timeoutMs;

  while (Date.now() < end) {
    try {
      const info = await getPositionCloseSideAndLots(sym);
      if (info?.hasPos && info?.lots > 0) return info;
    } catch(e) {}
    await sleep(pollMs);
  }
  return null;
}

// ✅ FIX 2 helper: wait for the entry chain to set didEnter for a given sigId+psym
async function waitForEntryChainCompletion(sigId, psym, timeoutMs = PROTECTION_WAIT_FOR_ENTRY_MS, pollMs = PROTECTION_ENTRY_POLL_MS) {
  const start = Date.now();
  const end = start + timeoutMs;

  console.log(`waitForEntryChain: polling for didEnter on ${psym} (sigId=${sigId}), timeout=${timeoutMs}ms`);

  while (Date.now() < end) {
    if (peekChainDidEnter(sigId, psym)) {
      const elapsed = Date.now() - start;
      console.log(`waitForEntryChain: didEnter=true for ${psym} after ${elapsed}ms`);
      return true;
    }
    await sleep(pollMs);
  }

  const elapsed = Date.now() - start;
  console.warn(`waitForEntryChain: TIMEOUT waiting for didEnter on ${psym} after ${elapsed}ms`);
  return false;
}

// ----- Flatten helper for CANCAL (seq0) -----
async function flattenFromCancelMsg(cancelMsg, psym){
  const scopeAll = isScopeAll(cancelMsg);

  if (FORCE_CANCEL_ORDERS_ON_CANCEL && typeof cancelMsg.cancel_orders === 'undefined') cancelMsg.cancel_orders = true;
  if (FORCE_CLOSE_ON_CANCEL && typeof cancelMsg.close_position === 'undefined') cancelMsg.close_position = true;

  const doCancelOrders = (typeof cancelMsg.cancel_orders === 'undefined') ? true : !!cancelMsg.cancel_orders;
  const doClosePos     = (typeof cancelMsg.close_position === 'undefined') ? true : !!cancelMsg.close_position;

  const cancelScope = String(
    cancelMsg.cancel_orders_scope || (scopeAll ? 'ALL' : 'SYMBOL')
  ).toUpperCase();

  const cancelFallbackAll = !!cancelMsg.cancel_fallback_all;

  const steps = {
    cancel_orders: false,
    close_position: false,
    cancel_mode: null,
    close_mode: null,
    cancel_error: null,
    close_error: null
  };

  if (doCancelOrders) {
    try {
      if (scopeAll || cancelScope === 'ALL') {
        await cancelAllOrders();
        steps.cancel_mode = 'cancel_all_orders';
      } else {
        await cancelOrdersBySymbol(psym, { fallbackAll: cancelFallbackAll, skipProtective: true });
        steps.cancel_mode = 'cancel_symbol_orders_skip_protective';
      }
      steps.cancel_orders = true;
    } catch (e) {
      steps.cancel_error = String(e?.message || e);
      console.warn('cancel step: cancel failed:', e?.message || e);
    }
  }

  if (doClosePos) {
    try {
      if (scopeAll) {
        await closeAllPositions();
        steps.close_mode = 'close_all_positions';
      } else {
        const sym = cancelMsg.symbol || cancelMsg.product_symbol || psym;
        if (sym) {
          await closePositionBySymbol(sym);
          steps.close_mode = 'close_by_symbol';
        } else {
          await closeAllPositions();
          steps.close_mode = 'close_all_positions';
        }
      }
      steps.close_position = true;
    } catch (e) {
      steps.close_error = String(e?.message || e);
      console.warn('cancel step: close failed:', e?.message || e);
    }
  }

  return steps;
}

// ----- Preflight helper for ENTER (seq1) -----
async function preflightFromEnterMsg(enterMsg, psym){
  const scopeAll = isScopeAll(enterMsg);

  const doCancelOrders = !!enterMsg.cancel_orders;
  const doClosePos     = !!enterMsg.close_position;

  const cancelScope = String(
    enterMsg.cancel_orders_scope || (scopeAll ? 'ALL' : 'SYMBOL')
  ).toUpperCase();

  const cancelFallbackAll = !!enterMsg.cancel_fallback_all;

  const steps = {
    cancel_orders: false,
    close_position: false,
    cancel_mode: null,
    close_mode: null,
    cancel_error: null,
    close_error: null
  };

  if (doCancelOrders) {
    try {
      if (scopeAll || cancelScope === 'ALL') {
        await cancelAllOrders();
        steps.cancel_mode = 'cancel_all_orders';
      } else {
        await cancelOrdersBySymbol(psym, { fallbackAll: cancelFallbackAll });
        steps.cancel_mode = 'cancel_symbol_orders';
      }
      steps.cancel_orders = true;
    } catch (e) {
      steps.cancel_error = String(e?.message || e);
      console.warn('enter preflight: cancel failed:', e?.message || e);
    }
  }

  if (doClosePos) {
    try {
      if (scopeAll) {
        await closeAllPositions();
        steps.close_mode = 'close_all_positions';
      } else {
        const sym = enterMsg.symbol || enterMsg.product_symbol || psym;
        if (sym) {
          await closePositionBySymbol(sym);
          steps.close_mode = 'close_by_symbol';
        } else {
          await closeAllPositions();
          steps.close_mode = 'close_all_positions';
        }
      }
      steps.close_position = true;
    } catch (e) {
      steps.close_error = String(e?.message || e);
      console.warn('enter preflight: close failed:', e?.message || e);
    }
  }

  return steps;
}

// ---------- protection actions ----------
async function placeSLIntent(m){
  const psym = toProductSymbol(m.symbol || m.product_symbol);
  const sigId = m.sig_id || m.signal_id || '';

  console.log('PLACE_SL_INTENT received', {
    sig_id: sigId,
    symbol: psym,
    stop_price: m.stop_price
  });

  if (!psym) throw new Error('placeSLIntent: missing symbol/product_symbol');

  // ✅ FIX 2: Wait for the entry chain to complete (didEnter=true) instead of blind 10s sleep.
  //   This guarantees SL placement happens AFTER the entry order fills, regardless of how long
  //   the entry chain takes (could be 2s or 200s depending on API latency and flat-wait).
  if (sigId) {
    const entryDone = await waitForEntryChainCompletion(sigId, psym);
    if (!entryDone) {
      console.warn(`placeSLIntent: entry chain never completed for ${psym} (sigId=${sigId}), proceeding anyway to check position`);
    }
  } else {
    // No sigId → fallback to old behavior (short sleep)
    console.log(`placeSLIntent: no sigId, falling back to 10s sleep for ${psym}`);
    await sleep(10000);
  }

  // Now wait for the NEW position to appear (up to POSITION_WAIT_MS)
  const info = await waitUntilPositionSymbol(psym, POSITION_WAIT_MS);
  if (!info || !info.hasPos || !(info.lots > 0)) {
    throw new Error(`placeSLIntent: no live position found for ${psym}`);
  }

  // ✅ FIX: Use known entry lots from LAST_ENTRY_SENT instead of re-inferring
  const knownEntry = LAST_ENTRY_SENT.get(psym);
  if (knownEntry?.lots > 0 && (Date.now() - knownEntry.ts) < 5 * 60 * 1000) {
    if (knownEntry.lots !== info.lots) {
      console.log('⚠ placeSLIntent LOT FIX: overriding inferred lots with known entry lots', {
        psym,
        inferredLots: info.lots,
        knownEntryLots: knownEntry.lots,
        entryAge: `${Math.round((Date.now() - knownEntry.ts)/1000)}s ago`
      });
    }
    info.lots = knownEntry.lots;
  }

  const stopPrice = nnum(m.stop_price, 0);
  if (!(stopPrice > 0)) throw new Error(`placeSLIntent: invalid stop_price for ${psym}`);

  const client_order_id = protectiveClientOrderId('SL', sigId, psym);

  const body = {
    product_symbol: psym,
    order_type: m.order_type || 'market_order',
    side: info.closeSide,
    size: info.lots,
    reduce_only: true,
    stop_order_type: m.stop_order_type || 'stop_loss_order',
    stop_price: String(stopPrice),
    client_order_id
  };

  console.log('PLACE_SL_INTENT placing (place-first, cancel-after)', {
    psym,
    closeSide: info.closeSide,
    lots: info.lots,
    stopPrice,
    client_order_id
  });

  // ✅ FIX 4: PLACE NEW SL FIRST, then cancel old protective orders.
  //   If placement fails, old SL stays in place → position never left naked.
  const r = await dcall('POST', '/v2/orders', body);

  // New SL placed successfully → now safe to cancel old protective orders
  try {
    await cancelProtectiveOrdersBySymbol(psym, client_order_id);
  } catch (e) {
    console.warn('placeSLIntent: cancel old protective failed (non-fatal, new SL is already placed)', { psym, err: e?.message || e });
  }

  return { ok:true, action:'PLACE_SL_INTENT', symbol:psym, lots:info.lots, closeSide:info.closeSide, stopPrice, r };
}

async function placeTrailIntent(m){
  const psym = toProductSymbol(m.symbol || m.product_symbol);
  const sigId = m.sig_id || m.signal_id || '';

  console.log('TRAIL_SL_INTENT received', {
    sig_id: sigId,
    symbol: psym,
    trail_amount: m.trail_amount,
    mode: m.trail_mode
  });

  if (!psym) throw new Error('placeTrailIntent: missing symbol/product_symbol');

  // ✅ FIX 2: Wait for the entry chain to complete instead of blind 10s sleep
  if (sigId) {
    const entryDone = await waitForEntryChainCompletion(sigId, psym);
    if (!entryDone) {
      console.warn(`placeTrailIntent: entry chain never completed for ${psym} (sigId=${sigId}), proceeding anyway to check position`);
    }
  } else {
    console.log(`placeTrailIntent: no sigId, falling back to 10s sleep for ${psym}`);
    await sleep(10000);
  }

  // Now wait for the NEW position to appear
  const info = await waitUntilPositionSymbol(psym, POSITION_WAIT_MS);
  if (!info || !info.hasPos || !(info.lots > 0)) {
    throw new Error(`placeTrailIntent: no live position found for ${psym}`);
  }

  // ✅ FIX: Use known entry lots from LAST_ENTRY_SENT instead of re-inferring
  const knownEntry = LAST_ENTRY_SENT.get(psym);
  if (knownEntry?.lots > 0 && (Date.now() - knownEntry.ts) < 5 * 60 * 1000) {
    if (knownEntry.lots !== info.lots) {
      console.log('⚠ placeTrailIntent LOT FIX: overriding inferred lots with known entry lots', {
        psym,
        inferredLots: info.lots,
        knownEntryLots: knownEntry.lots,
        entryAge: `${Math.round((Date.now() - knownEntry.ts)/1000)}s ago`
      });
    }
    info.lots = knownEntry.lots;
  }

  const trailAmount = nnum(m.trail_amount, 0);
  if (!(trailAmount > 0)) throw new Error(`placeTrailIntent: invalid trail_amount for ${psym}`);

  // Get current price to calculate initial stop_price
  const markPrice = nnum(info.row?.mark_price || info.row?.last_price, 0);
  let currentPrice = markPrice;
  if (!(currentPrice > 0)) {
    currentPrice = nnum(await getTickerPriceUSD(psym), 0);
  }
  if (!(currentPrice > 0)) throw new Error(`placeTrailIntent: cannot get current price for ${psym}`);

  // Calculate initial stop_price:
  //   LONG (closeSide=sell): stop = currentPrice - trailAmount
  //   SHORT (closeSide=buy): stop = currentPrice + trailAmount
  const isLong = info.closeSide === 'sell';
  const stopPrice = isLong
    ? currentPrice - trailAmount
    : currentPrice + trailAmount;

  if (!(stopPrice > 0)) throw new Error(`placeTrailIntent: calculated stopPrice=${stopPrice} invalid for ${psym}`);

  const client_order_id = protectiveClientOrderId('TRL', sigId, psym);

  // ✅ FIX 4b: Delta India API requires NEGATIVE trail_amount for sell stop orders (closing a long),
  //   and POSITIVE trail_amount for buy stop orders (closing a short).
  //   Without this, sell-side trails get: "Trail amount should be negative for sell stop orders"
  const signedTrailAmount = isLong ? -trailAmount : trailAmount;

  // Delta India API: use stop_loss_order + trail_amount
  // trail_amount makes it behave as trailing stop
  // stop_trigger_method: last_traded_price for more responsive trailing
  const body = {
    product_symbol: psym,
    order_type: 'market_order',
    side: info.closeSide,
    size: info.lots,
    reduce_only: true,
    stop_order_type: 'stop_loss_order',
    trail_amount: String(signedTrailAmount),
    stop_trigger_method: 'last_traded_price',
    client_order_id
  };

  console.log('TRAIL_SL_INTENT placing (place-first, cancel-after)', {
    psym,
    closeSide: info.closeSide,
    lots: info.lots,
    isLong,
    currentPrice,
    trailAmount,
    signedTrailAmount,
    stopPrice,
    mode: m.trail_mode || 'ACTIVATE',
    client_order_id
  });

  // ✅ FIX 4: PLACE NEW TRAIL FIRST, then cancel old protective orders.
  //   If trail placement fails, old SL stays in place → position never left naked.
  const r = await dcall('POST', '/v2/orders', body);

  // New trail placed successfully → now safe to cancel old protective orders
  try {
    await cancelProtectiveOrdersBySymbol(psym, client_order_id);
  } catch (e) {
    console.warn('placeTrailIntent: cancel old protective failed (non-fatal, new trail is already placed)', { psym, err: e?.message || e });
  }

  return {
    ok:true,
    action:'TRAIL_SL_INTENT',
    symbol:psym,
    lots:info.lots,
    closeSide:info.closeSide,
    trailAmount,
    signedTrailAmount,
    stopPrice,
    currentPrice,
    mode:m.trail_mode || 'ACTIVATE',
    r
  };
}
async function cancelProtectiveIntent(m){
  const psym = toProductSymbol(m.symbol || m.product_symbol);
  console.log('CANCEL_PROTECTIVE received', {
    sig_id: m.sig_id || m.signal_id,
    symbol: m.symbol || m.product_symbol
  });

  if (!psym) throw new Error('cancelProtectiveIntent: missing symbol/product_symbol');

  const r = await cancelProtectiveOrdersBySymbol(psym);
  return { ok:true, action:'CANCEL_PROTECTIVE', symbol:psym, ...r };
}

// ===================== CLOSE_SL (candle-close confirmed SL) ===================== //
async function closeSLIntent(m){
  const psym = toProductSymbol(m.symbol || m.product_symbol);
  const sigId = m.sig_id || m.signal_id || '';
  const reason = m.reason || 'SOFTWARE_SL';
  const closePrice = nnum(m.close_price, 0);

  console.log('CLOSE_SL received', {
    sig_id: sigId,
    symbol: psym,
    reason,
    close_price: closePrice
  });

  if (!psym) throw new Error('closeSLIntent: missing symbol/product_symbol');

  // Step 1: Cancel ALL open orders for this symbol (TPs + protective SL/trail)
  let cancelResult = null;
  try {
    cancelResult = await cancelOrdersBySymbol(psym, { fallbackAll: false });
    console.log('CLOSE_SL cancel orders done', { psym, cancelResult });
  } catch (e) {
    console.warn('CLOSE_SL cancel orders failed, continuing to close position', { psym, err: e?.message || e });
  }

  // Step 2: Close the position with a market order
  // Step 2: Close the position with a market order
  const posInfo = await getPositionCloseSideAndLots(psym);

  // ✅ FIX: Use known entry lots from LAST_ENTRY_SENT instead of re-inferring
  const knownEntry = LAST_ENTRY_SENT.get(psym);
  if (knownEntry?.lots > 0 && posInfo.hasPos && (Date.now() - knownEntry.ts) < 30 * 60 * 1000) {
    if (knownEntry.lots !== posInfo.lots) {
      console.log('⚠ closeSLIntent LOT FIX: overriding inferred lots with known entry lots', {
        psym,
        inferredLots: posInfo.lots,
        knownEntryLots: knownEntry.lots,
        entryAge: `${Math.round((Date.now() - knownEntry.ts)/1000)}s ago`
      });
    }
    posInfo.lots = knownEntry.lots;
  }

  if (!posInfo.hasPos || !(posInfo.lots > 0)) {
    console.log('CLOSE_SL: no open position found, may already be closed', { psym });
    return {
      ok: true,
      action: 'CLOSE_SL',
      symbol: psym,
      reason,
      close_price: closePrice,
      position_found: false,
      cancel_result: cancelResult,
      note: 'No position found — may already be closed by emergency stop or TP fills'
    };
  }

  const closeBody = {
    product_symbol: psym,
    order_type: 'market_order',
    side: posInfo.closeSide,
    size: posInfo.lots,
    reduce_only: true
  };

  console.log('CLOSE_SL closing position', {
    psym,
    closeSide: posInfo.closeSide,
    lots: posInfo.lots,
    reason,
    close_price: closePrice
  });

  const closeResult = await dcall('POST', '/v2/orders', closeBody);

  return {
    ok: true,
    action: 'CLOSE_SL',
    symbol: psym,
    reason,
    close_price: closePrice,
    position_found: true,
    closeSide: posInfo.closeSide,
    lots: posInfo.lots,
    cancel_result: cancelResult,
    close_result: closeResult?.result || closeResult
  };
}

// ---------- health ----------
app.get('/health', (_req,res)=>res.json({ok:true, started_at:process.env.__STARTED_AT}));
app.get('/healthz', (_req,res)=>res.send('ok'));
app.get('/debug/seen', (_req,res)=>{ res.json({ size: SEEN.size }); });

app.get('/debug/chain', (_req,res)=>{
  cleanupChain();
  const out = {};
  for (const [k,v] of CHAIN) {
    out[k] = {
      createdAt: v.createdAt,
      lastTouch: v.lastTouch,
      didCancel: v.didCancel,
      didEnterPrep: v.didEnterPrep,
      didEnter:  v.didEnter,
      didBatch:  v.didBatch,
      haveCancel: !!v.cancelMsg,
      haveEnter:  !!v.enterMsg,
      haveBatch:  !!v.batchMsg,
    };
  }
  res.json({ size: CHAIN.size, items: out });
});

// =====================================================================
// ✅ PATCHED: CORE WEBHOOK PROCESSING LOGIC (extracted from /tv handler)
// This function contains ALL the original processing logic, unchanged.
// It runs ASYNCHRONOUSLY after TradingView gets its instant 200 response.
// =====================================================================
async function processWebhook(msg) {
  const action = String(msg.action || '').toUpperCase();
  const sigId  = String(msg.sig_id || msg.signal_id || '');
  const seq    = (typeof msg.seq !== 'undefined') ? Number(msg.seq) : NaN;
  const symTV  = msg.symbol || msg.product_symbol || '';
  const psym   = toProductSymbol(symTV);

  const qKey =
    action === 'CANCEL_PROTECTIVE' || action === 'CLOSE_SL' || action === 'PLACE_SL_INTENT' || action === 'TRAIL_SL_INTENT'
      ? protectiveKey(psym)
      : (isScopeAll(msg) ? 'GLOBAL' : `SYM:${safeUpper(psym)}`);

  const out = await enqueue(qKey, async () => {

    const key = seenKey(msg);
    if (SEEN.has(key)) return { ok:true, dedup:true };
    rememberSeen(key);

    if (action === 'EXIT') return { ok:true, ignored:'EXIT' };

    // ---------- protection actions (OUTSIDE strict seq0/1/2 chain) ----------
    if (action === 'PLACE_SL_INTENT') {
      return await placeSLIntent(msg);
    }
    if (action === 'TRAIL_SL_INTENT') {
      return await placeTrailIntent(msg);
    }
    if (action === 'CANCEL_PROTECTIVE') {
      return await cancelProtectiveIntent(msg);
    }
    if (action === 'CLOSE_SL') {
      return await closeSLIntent(msg);
    }

    // ---------- strict chain only for entry workflow ----------
    if (STRICT_SEQUENCE) {
      if (!sigId) return { ok:true, ignored:'missing_sig_id_in_strict_mode', action, symbol: psym };
      if (!Number.isFinite(seq)) return { ok:true, ignored:'missing_or_invalid_seq_in_strict_mode', sig_id: sigId, action, symbol: psym };
      if (![0,1,2].includes(seq)) return { ok:true, ignored:'bad_seq', sig_id: sigId, symbol: psym, seq };
    }

    console.log('instance:', { K_REVISION: process.env.K_REVISION, HOSTNAME: process.env.HOSTNAME });

    const chain = upsertChainMsg(sigId, psym, seq, msg);

    const ageMs = Date.now() - (chain?.createdAt || Date.now());
    if (ageMs > SIGNAL_CHAIN_WINDOW_MS) {
      return { ok:true, ignored:'chain_expired', sig_id: sigId, symbol: psym, age_ms: ageMs, window_ms: SIGNAL_CHAIN_WINDOW_MS };
    }

    const progressed = [];

    // STEP 1: CANCAL (seq0)
    if (!chain.didCancel) {
      if (chain.cancelMsg) {
        const cancelMsg = chain.cancelMsg;
        if (typeof cancelMsg.cancel_orders_scope === 'undefined') cancelMsg.cancel_orders_scope = 'SYMBOL';

        // ✅ FIX 1: Touch chain before expensive operation
        touchChain(sigId, psym);
        const steps = await flattenFromCancelMsg(cancelMsg, psym);
        touchChain(sigId, psym);

        const requireFlat = (typeof cancelMsg.require_flat === 'undefined') ? false : !!cancelMsg.require_flat;
        let flat = true;
        if (requireFlat) {
          touchChain(sigId, psym);
          flat = isScopeAll(cancelMsg) ? await waitUntilFlat() : await waitUntilFlatSymbol(psym);
          touchChain(sigId, psym);
        }

        chain.didCancel = true;
        if (STRICT_SEQUENCE) setSigState(sigId, psym, { lastSeq: 0 });

        progressed.push({ ok:true, did:'CANCAL', steps, flat, symbol: psym });
      } else {
        if (AUTO_CANCEL_ON_ENTER && chain.enterMsg) {
          const syntheticCancel = { ...chain.enterMsg, action:'CANCAL', seq:0, cancel_orders_scope:'SYMBOL' };

          touchChain(sigId, psym);
          const steps = await flattenFromCancelMsg(syntheticCancel, psym);
          touchChain(sigId, psym);

          const flat = isScopeAll(syntheticCancel)
            ? await waitUntilFlat()
            : await waitUntilFlatSymbol(psym);
          touchChain(sigId, psym);

          chain.didCancel = true;
          if (STRICT_SEQUENCE) setSigState(sigId, psym, { lastSeq: 0 });
          progressed.push({ ok:true, did:'CANCAL', synthetic:true, steps, flat, symbol: psym });
        } else {
          if (chain.enterMsg) {
            chain.didCancel = true;
            if (STRICT_SEQUENCE) setSigState(sigId, psym, { lastSeq: 0 });
            progressed.push({ ok:true, did:'CANCAL', skipped:true, note:'No seq0 received; proceeding because ENTER exists', symbol: psym });
          } else {
            return {
              ok:true,
              queued:'waiting_for_CANCAL',
              sig_id: sigId,
              symbol: psym,
              have:{ cancel:!!chain.cancelMsg, enter:!!chain.enterMsg, batch:!!chain.batchMsg },
              did:{ cancel:chain.didCancel, enter:chain.didEnter, batch:chain.didBatch }
            };
          }
        }
      }
    }

    // STEP 2: ENTER (seq1)
    if (chain.didCancel && !chain.didEnter) {
      if (!chain.enterMsg) {
        return {
          ok:true,
          queued:'waiting_for_ENTER',
          sig_id: sigId,
          symbol: psym,
          have:{ cancel:!!chain.cancelMsg, enter:!!chain.enterMsg, batch:!!chain.batchMsg },
          did:{ cancel:chain.didCancel, enter:chain.didEnter, batch:chain.didBatch }
        };
      }

      const enterMsg = chain.enterMsg;

      if (!chain.didEnterPrep) {
        touchChain(sigId, psym);
        const pre = await preflightFromEnterMsg(enterMsg, psym);
        touchChain(sigId, psym);

        chain.didEnterPrep = true;
        progressed.push({ ok:true, did:'ENTER_PRE', pre, symbol: psym });
      }

      const requireFlat = (typeof enterMsg.require_flat === 'undefined') ? true : !!enterMsg.require_flat;

      if (requireFlat) {
        touchChain(sigId, psym);
        const flatNow = isScopeAll(enterMsg) ? await isFlatNowGlobal() : await isFlatNowSymbol(psym);
        touchChain(sigId, psym);

        if (!flatNow) {
          if (FAST_ENTER) {
            touchChain(sigId, psym);
            const flatQuick = isScopeAll(enterMsg)
              ? await waitUntilFlat(FAST_ENTER_WAIT_MS, FLAT_POLL_MS)
              : await waitUntilFlatSymbol(psym, FAST_ENTER_WAIT_MS, FLAT_POLL_MS);
            touchChain(sigId, psym);

            if (!flatQuick) {
              touchChain(sigId, psym);
              const flatRetry = isScopeAll(enterMsg)
                ? await waitUntilFlat(FAST_ENTER_RETRY_MS, FLAT_POLL_MS)
                : await waitUntilFlatSymbol(psym, FAST_ENTER_RETRY_MS, FLAT_POLL_MS);
              touchChain(sigId, psym);

              if (!flatRetry) {
                return { ok:false, error:'require_flat_timeout', sig_id: sigId, symbol: psym, stage:'ENTER', note:'Not flat after ENTER preflight. Entry blocked.' };
              }
            }
          } else {
            touchChain(sigId, psym);
            const flat = isScopeAll(enterMsg) ? await waitUntilFlat() : await waitUntilFlatSymbol(psym);
            touchChain(sigId, psym);

            if (!flat) {
              return { ok:false, error:'require_flat_timeout', sig_id: sigId, symbol: psym, stage:'ENTER', note:'Not flat after ENTER preflight. Entry blocked.' };
            }
          }
        }
      }

      touchChain(sigId, psym);
      const r = await placeEntry(enterMsg);
      touchChain(sigId, psym);

      chain.didEnter = true;
      if (STRICT_SEQUENCE) setSigState(sigId, psym, { lastSeq: 1 });

      progressed.push({ ok:true, step:'entry', r, symbol: psym });
    }

    // STEP 3: BATCH_TPS (seq2)
    if (chain.didCancel && chain.didEnter && !chain.didBatch) {
      if (!chain.batchMsg) {
        return {
          ok:true,
          queued:'waiting_for_BATCH_TPS',
          sig_id: sigId,
          symbol: psym,
          have:{ cancel:!!chain.cancelMsg, enter:!!chain.enterMsg, batch:!!chain.batchMsg },
          did:{ cancel:chain.didCancel, enter:chain.didEnter, batch:chain.didBatch }
        };
      }

      touchChain(sigId, psym);
      const r = await placeBatch(chain.batchMsg);
      touchChain(sigId, psym);

      chain.didBatch = true;
      if (STRICT_SEQUENCE) setSigState(sigId, psym, { lastSeq: 2 });

      progressed.push({ ok:true, step:'batch', r, symbol: psym });
    }

    const have = { cancel: !!chain.cancelMsg, enter: !!chain.enterMsg, batch: !!chain.batchMsg };
    const did  = { cancel: !!chain.didCancel, enterPrep: !!chain.didEnterPrep, enter: !!chain.didEnter, batch: !!chain.didBatch };

    return {
      ok:true,
      status: (did.cancel && did.enter && did.batch) ? 'done' : 'progressed',
      sig_id: sigId,
      symbol: psym,
      have,
      did,
      progressed
    };
  });

  return out;
}

// =====================================================================
// ✅ PATCHED: TradingView webhook endpoint
// NOW: responds INSTANTLY with 200, processes ASYNC in background.
// TradingView will NEVER see a timeout — even if Delta API takes 10s+.
// Queue serialization is preserved (CANCAL → ENTER → TPs → SL in order).
// =====================================================================
app.post('/tv', async (req, res) => {
  try {
    // ---- Auth check (fast, synchronous) ----
    if (WEBHOOK_TOKEN) {
      const hdr = req.headers['x-webhook-token'];
      if (hdr !== WEBHOOK_TOKEN) return res.status(401).json({ ok:false, error:'unauthorized' });
    }

    // ---- Parse message (fast, synchronous) ----
    const msg    = (typeof req.body === 'string') ? JSON.parse(req.body) : (req.body || {});
    const action = String(msg.action || '').toUpperCase();
    const sigId  = String(msg.sig_id || msg.signal_id || '');
    const seq    = (typeof msg.seq !== 'undefined') ? Number(msg.seq) : NaN;
    const symTV  = msg.symbol || msg.product_symbol || '';
    const psym   = toProductSymbol(symTV);

    console.log('\n=== INCOMING /tv ===');
    console.log(JSON.stringify({ action, sigId, seq, symTV, psym, ts: new Date().toISOString() }));

    // =========================================================
    // ✅ RESPOND TO TRADINGVIEW IMMEDIATELY — prevents timeout
    // =========================================================
    res.status(200).json({
      ok: true,
      status: 'accepted',
      action,
      symbol: psym,
      sig_id: sigId,
      seq: Number.isFinite(seq) ? seq : undefined,
      ts: new Date().toISOString()
    });

    // =========================================================
    // ✅ PROCESS ASYNCHRONOUSLY — runs AFTER response is sent
    // The enqueue() inside processWebhook() preserves ordering:
    //   CANCAL → ENTER → BATCH_TPS all serialize on same queue key
    //   PLACE_SL_INTENT serializes on its own protective queue key
    // =========================================================
    setImmediate(async () => {
      try {
        const result = await processWebhook(msg);
        console.log(`ASYNC RESULT [${action}] ${psym}:`, JSON.stringify(result));
      } catch (e) {
        console.error(`✖ ASYNC ERROR [${action}] ${psym}:`, e?.message || e);
      }
    });

  } catch (e) {
    console.error('✖ PARSE/AUTH ERROR:', e?.message || e);
    // If we haven't sent a response yet (parse/auth failed before res.json)
    if (!res.headersSent) {
      return res.status(400).json({ ok:false, error:String(e.message || e) });
    }
  }
});

app.listen(PORT, ()=>console.log(
  `Relay listening http://localhost:${PORT} (BASE=${BASE_URL}, AUTH=${AUTH_MODE}, STRICT_SEQUENCE=${STRICT_SEQUENCE}, FAST_ENTER=${FAST_ENTER}, SIGNAL_CHAIN_WINDOW_MS=${SIGNAL_CHAIN_WINDOW_MS}, AUTO_CANCEL_ON_ENTER=${AUTO_CANCEL_ON_ENTER}, FORCE_CLOSE_ON_CANCEL=${FORCE_CLOSE_ON_CANCEL}, CHAIN_TTL_MS=${CHAIN_TTL_MS}, PROTECTION_WAIT_FOR_ENTRY_MS=${PROTECTION_WAIT_FOR_ENTRY_MS}, INSTANT_RESPONSE=true)`
));
