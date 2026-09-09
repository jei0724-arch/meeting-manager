// 기기 간 데이터 동기화용 API (Vercel Serverless Function)
//
// 저장소: Upstash Redis (Vercel 대시보드 → Storage → Upstash Redis 연결)
// 필요한 환경변수 (연결하면 Vercel이 자동으로 주입합니다):
//   KV_REST_API_URL / KV_REST_API_TOKEN  또는
//   UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN
//
// GET  /api/data → { ok:true, rev, state, updatedAt }
// POST /api/data  body: { state } → { ok:true, rev, updatedAt }
// 저장소가 아직 연결되지 않았으면 { ok:false, reason:'not-configured' } 를 반환하고,
// 앱은 기존처럼 로컬 저장소만으로 동작합니다.

const STATE_KEY = 'mg:state';
const REV_KEY   = 'mg:rev';

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
      res.status(200).json({ ok: true, rev: doc.rev, updatedAt: doc.updatedAt });
      return;
    }

    res.setHeader('Allow', 'GET, POST');
    res.status(405).json({ ok: false, reason: 'method-not-allowed' });
  } catch (err) {
    res.status(500).json({ ok: false, reason: 'storage-error', message: String(err && err.message || err) });
  }
};
