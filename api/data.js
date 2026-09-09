// 기기 간 실시간 데이터 동기화 API (Vercel Serverless Function)
//
// 저장소: Upstash Redis (Vercel 대시보드 → Storage → Upstash Redis 연결)
// 필요한 환경변수 (연결하면 Vercel이 자동으로 주입합니다):
//   KV_REST_API_URL / KV_REST_API_TOKEN  또는
//   UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN
//
// 실시간 알림: Pusher Channels (https://pusher.com 무료 플랜)
// 필요한 환경변수 (Pusher 앱 대시보드 → App Keys 에서 확인):
//   PUSHER_APP_ID / PUSHER_KEY / PUSHER_SECRET / PUSHER_CLUSTER
// 설정되어 있으면 저장 직후 다른 모든 기기에 즉시 push 알림을 보낸다.
// 설정 안 돼 있어도 기존처럼 동작(다른 기기는 폴링으로 뒤늦게 반영)한다.
//
// GET  /api/data → { ok:true, rev, state, updatedAt }
// POST /api/data  body: { state } → { ok:true, rev, updatedAt }
// 저장소가 아직 연결되지 않았으면 { ok:false, reason:'not-configured' } 를 반환하고,
// 앱은 기존처럼 로컬 저장소만으로 동작합니다.

const crypto = require('crypto');

const STATE_KEY = 'mg:state';
const REV_KEY   = 'mg:rev';
const PUSHER_CHANNEL = 'meeting-manager';
const PUSHER_EVENT   = 'state-updated';

function redisConfig() {
  const url =
    process.env.KV_REST_API_URL ||
    process.env.UPSTASH_REDIS_REST_URL ||
    process.env.REDIS_REST_URL;
  const token =
    process.env.KV_REST_API_TOKEN ||
    process.env.UPSTASH_REDIS_REST_TOKEN ||
    process.env.REDIS_REST_TOKEN;
  if (!url || !token) return null;
  return { url: url.replace(/\/+$/, ''), token };
}

async function redis(cfg, command) {
  const res = await fetch(cfg.url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${cfg.token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(command),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`redis ${res.status}: ${text.slice(0, 200)}`);
  }
  const json = await res.json();
  if (json.error) throw new Error(`redis: ${json.error}`);
  return json.result;
}

function pusherConfig() {
  const appId   = process.env.PUSHER_APP_ID;
  const key     = process.env.PUSHER_KEY;
  const secret  = process.env.PUSHER_SECRET;
  const cluster = process.env.PUSHER_CLUSTER;
  if (!appId || !key || !secret || !cluster) return null;
  return { appId, key, secret, cluster };
}

// Pusher REST API 트리거를 npm 의존성 없이 직접 서명해서 호출한다.
// (문서: https://pusher.com/docs/channels/library_auth_reference/rest-api/)
async function pusherTrigger(cfg, payload) {
  const body = JSON.stringify({
    name: PUSHER_EVENT,
    channels: [PUSHER_CHANNEL],
    data: JSON.stringify(payload),
  });
  const bodyMd5 = crypto.createHash('md5').update(body).digest('hex');
  const params = {
    auth_key: cfg.key,
    auth_timestamp: String(Math.floor(Date.now() / 1000)),
    auth_version: '1.0',
    body_md5: bodyMd5,
  };
  const sortedQuery = Object.keys(params).sort()
    .map((k) => `${k}=${params[k]}`)
    .join('&');
  const path = `/apps/${cfg.appId}/events`;
  const stringToSign = `POST\n${path}\n${sortedQuery}`;
  const signature = crypto.createHmac('sha256', cfg.secret).update(stringToSign).digest('hex');
  const url = `https://api-${cfg.cluster}.pusher.com${path}?${sortedQuery}&auth_signature=${signature}`;

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`pusher ${res.status}: ${text.slice(0, 200)}`);
  }
}

function readBody(req) {
  if (req.body && typeof req.body === 'object') return Promise.resolve(req.body);
  if (typeof req.body === 'string') {
    try { return Promise.resolve(JSON.parse(req.body)); } catch { return Promise.resolve(null); }
  }
  return new Promise((resolve) => {
    let raw = '';
    req.on('data', (c) => { raw += c; });
    req.on('end', () => {
      try { resolve(raw ? JSON.parse(raw) : null); } catch { resolve(null); }
    });
    req.on('error', () => resolve(null));
  });
}

module.exports = async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store, max-age=0');

  const cfg = redisConfig();
  if (!cfg) {
    res.status(200).json({ ok: false, reason: 'not-configured' });
    return;
  }

  try {
    if (req.method === 'GET') {
      const raw = await redis(cfg, ['GET', STATE_KEY]);
      if (!raw) {
        res.status(200).json({ ok: true, rev: 0, state: null, updatedAt: 0 });
        return;
      }
      const doc = typeof raw === 'string' ? JSON.parse(raw) : raw;
      res.status(200).json({
        ok: true,
        rev: doc.rev || 0,
        state: doc.state || null,
        updatedAt: doc.updatedAt || 0,
      });
      return;
    }

    if (req.method === 'POST' || req.method === 'PUT') {
      const body = await readBody(req);
      if (!body || typeof body.state !== 'object' || body.state === null) {
        res.status(400).json({ ok: false, reason: 'bad-request' });
        return;
      }
      const rev = await redis(cfg, ['INCR', REV_KEY]);
      const doc = { rev, state: body.state, updatedAt: Date.now() };
      await redis(cfg, ['SET', STATE_KEY, JSON.stringify(doc)]);

      const pCfg = pusherConfig();
      if (pCfg) {
        // 알림 실패는 저장 자체를 실패시키지 않는다 — 다른 기기는 폴링으로 뒤늦게라도 반영된다.
        try { await pusherTrigger(pCfg, { rev: doc.rev, updatedAt: doc.updatedAt }); } catch {}
      }

      res.status(200).json({ ok: true, rev: doc.rev, updatedAt: doc.updatedAt });
      return;
    }

    res.setHeader('Allow', 'GET, POST');
    res.status(405).json({ ok: false, reason: 'method-not-allowed' });
  } catch (err) {
    res.status(500).json({ ok: false, reason: 'storage-error', message: String(err && err.message || err) });
  }
};
