// 브라우저가 실시간(Pusher) 연결에 필요한 공개 값만 내려주는 엔드포인트.
// PUSHER_KEY / PUSHER_CLUSTER 는 비밀값이 아니라 클라이언트에 공개되어도 되는 식별자다.
// (실제 쓰기 권한은 서버에만 있는 PUSHER_SECRET 으로 서명하는 api/data.js 가 갖는다)
module.exports = function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store, max-age=0');
  const key = process.env.PUSHER_KEY || null;
  const cluster = process.env.PUSHER_CLUSTER || null;
  res.status(200).json({ ok: !!(key && cluster), key, cluster });
};
