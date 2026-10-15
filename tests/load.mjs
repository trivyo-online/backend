// Load test: N simulated players on mixed network quality play a full
// autopilot quiz. Reports join/fan-out/answer latency, reconnect recovery
// and checks every player's score against the server leaderboard.
// Usage: node tests/load.mjs [base=http://localhost:8080] [players=100] [questions=10] [seconds=8] [out.json]
const [BASE = 'http://localhost:8080', NP = '100', NQ = '10', SECS = '8', OUT] = process.argv.slice(2)
const PLAYERS = Number(NP), QUESTIONS = Number(NQ), SECONDS = Number(SECS)
const B = BASE + '/api', W = BASE.replace(/^http/, 'ws') + '/api'

const rand = (a, b) => a + Math.random() * (b - a)
const wait = (ms) => new Promise((r) => setTimeout(r, ms))
const pct = (arr, p) => {
  if (!arr.length) return null
  const s = [...arr].sort((a, b) => a - b)
  return Math.round(s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))])
}
const stats = (arr) => ({ n: arr.length, p50: pct(arr, 50), p95: pct(arr, 95), p99: pct(arr, 99), max: arr.length ? Math.round(Math.max(...arr)) : null })

// Network profiles: one-way latency range (ms); flaky clients also drop once.
const PROFILES = [
  { name: 'good', share: 0.6, lat: [5, 40], drop: false },
  { name: '3g', share: 0.3, lat: [150, 400], drop: false },
  { name: 'flaky', share: 0.1, lat: [300, 800], drop: true },
]

async function api(method, path, body, token) {
  const r = await fetch(B + path, { method, headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: 'Bearer ' + token } : {}) }, body: body ? JSON.stringify(body) : undefined })
  return r.json()
}

const metrics = { join: [], fanout: [], fanoutRaw: [], answerRtt: [], recovery: [], reconnects: 0, answersSent: 0, answersAccepted: 0, resumedResults: 0, errors: {}, framesIn: 0, bytesIn: 0, wrongScores: [] }

class Player {
  constructor(i, room, profile, correctBy) {
    Object.assign(this, { i, room, profile, correctBy, name: 'load' + String(i).padStart(3, '0'), key: 'k' + i + Math.random(), score: 0, expected: 0, answered: new Set(), pending: null, ended: false, dropped: false })
  }
  lat() {
    return rand(...this.profile.lat)
  }
  connect() {
    const t0 = performance.now()
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(`${W}/connect?${new URLSearchParams({ roomid: this.room, userid: this.name, key: this.key })}`)
      this.ws = ws
      let first = true
      ws.onmessage = (e) => {
        metrics.framesIn++
        metrics.bytesIn += e.data.length
        const recv = Date.now()
        // Simulated downlink latency, applied after we measured raw arrival.
        setTimeout(() => this.onFrame(JSON.parse(e.data), recv, first && (first = false, t0)), this.lat())
      }
      ws.onopen = () => resolve()
      ws.onerror = () => reject(new Error('connect failed ' + this.name))
      ws.onclose = () => {
        // Only the current socket matters; a replaced one closing is expected.
        if (ws === this.ws && !ws.intentional && !this.ended) this.reconnect('unexpected close')
      }
    })
  }
  async reconnect(why) {
    metrics.reconnects++
    const t = performance.now()
    for (let attempt = 0; attempt < 8; attempt++) {
      try {
        await wait(Math.min(4000, 500 * 2 ** attempt) * rand(0.5, 1))
        this.recoverFrom = t
        await this.connect()
        return
      } catch {
        /* retry with backoff */
      }
    }
    metrics.errors['reconnect-gave-up'] = (metrics.errors['reconnect-gave-up'] ?? 0) + 1
  }
  send(o) {
    // Simulated uplink latency.
    setTimeout(() => this.ws.readyState === 1 && this.ws.send(JSON.stringify(o)), this.lat())
  }
  onFrame(f, recv, joinStart) {
    if (f.type === 'welcome') {
      if (this.recoverFrom) {
        metrics.recovery.push(performance.now() - this.recoverFrom)
        this.recoverFrom = null
      } else if (joinStart !== false) metrics.join.push(performance.now() - joinStart)
      // Resend an answer whose result never arrived (lost in the drop).
      if (this.pending) this.send(this.pending.msg)
    }
    switch (f.type) {
      case 'question': {
        if (this.answered.has(f.questionno)) return
        metrics.fanoutRaw.push(recv - f.now)
        metrics.fanout.push(Date.now() - f.now)
        // 5% skip; others think 0.5-5 s then answer, 70% correctly.
        if (Math.random() < 0.05) return
        const think = rand(500, Math.min(5000, SECONDS * 1000 - 1500))
        setTimeout(() => {
          if (this.answered.has(f.questionno)) return
          const right = Math.random() < 0.7
          const correct = this.correctBy[f.questionno]
          const choice = right ? correct : (correct + 1 + Math.floor(Math.random() * 3)) % 4
          const msg = { type: 'answer', questionno: f.questionno, choice }
          this.pending = { msg, at: performance.now(), q: f.questionno }
          metrics.answersSent++
          this.send(msg)
          // Flaky clients drop right after answering, sometimes before the ack.
          if (this.profile.drop && !this.dropped && f.questionno >= 2) {
            this.dropped = true
            setTimeout(() => {
              this.ws.intentional = true
              this.ws.close()
              this.reconnect('simulated drop')
            }, rand(0, 600))
          }
        }, think)
        break
      }
      case 'result':
        if (this.pending && this.pending.q === f.questionno) {
          metrics.answerRtt.push(performance.now() - this.pending.at)
          this.pending = null
        }
        if (f.resumed) metrics.resumedResults++
        if (!this.answered.has(f.questionno)) {
          this.answered.add(f.questionno)
          metrics.answersAccepted++
          this.expected += f.points
        }
        this.score = f.score
        break
      case 'error':
        metrics.errors[f.code ?? f.message] = (metrics.errors[f.code ?? f.message] ?? 0) + 1
        if (f.code === 'late' && this.pending) this.pending = null
        break
      case 'end':
        this.ended = true
        this.final = f.leaderboard
        break
    }
  }
}

const started = Date.now()
const u = 'load' + Date.now().toString(36)
const { token } = await api('POST', '/admin/signup', { username: u, password: 'password123' })
const correctBy = Array.from({ length: QUESTIONS }, () => Math.floor(Math.random() * 4))
const questions = correctBy.map((c, i) => ({ Question: `Load question ${i + 1}?`, Options: ['A', 'B', 'C', 'D'].map((x) => `Option ${x}`), Correctoption: String(c), Eachquestiontime: SECONDS }))
const { roomid } = await api('POST', '/create-event', { Nameofroom: `Load test ${PLAYERS}×${QUESTIONS}`, Questions: questions }, token)

const hostWs = new WebSocket(`${W}/host?roomid=${roomid}&token=${encodeURIComponent(token)}`)
let hostFrames = 0, lastCount = 0
hostWs.onmessage = (e) => {
  hostFrames++
  const f = JSON.parse(e.data)
  if (f.type === 'players') lastCount = f.count
}
await new Promise((r) => (hostWs.onopen = r))
console.log(`room ${roomid}: joining ${PLAYERS} players…`)

const profiles = []
for (const p of PROFILES) for (let k = 0; k < Math.round(p.share * PLAYERS); k++) profiles.push(p)
while (profiles.length < PLAYERS) profiles.push(PROFILES[0])
const players = profiles.slice(0, PLAYERS).map((p, i) => new Player(i, roomid, p, correctBy))
// Ramp joins over ~3 s, like a room filling up.
await Promise.all(players.map((p, i) => wait((i / PLAYERS) * 3000).then(() => p.connect())))
await wait(2500)
console.log(`host sees ${lastCount} players; starting autopilot (${QUESTIONS} questions × ${SECONDS}s)`)
const quizStart = Date.now()
hostWs.send(JSON.stringify({ type: 'auto', delay: 1 }))

const deadline = Date.now() + QUESTIONS * (SECONDS + 7) * 1000 + 30000
while (!players.every((p) => p.ended) && Date.now() < deadline) await wait(500)
const quizMs = Date.now() - quizStart
await wait(1000)

const board = (await api('GET', `/rooms/${roomid}/leaderboard?limit=500`)).entries
const byName = Object.fromEntries(board.map((e) => [e.userid, e]))
for (const p of players) {
  const server = byName[p.name]?.score ?? 0
  if (server !== p.expected) metrics.wrongScores.push({ name: p.name, client: p.expected, server })
}
const statsRes = await api('GET', `/rooms/${roomid}/stats`)

const report = {
  room: roomid,
  players: PLAYERS,
  questions: QUESTIONS,
  secondsPerQuestion: SECONDS,
  profiles: PROFILES.map((p) => ({ name: p.name, players: profiles.filter((x) => x === p).length, latencyMs: p.lat, drops: p.drop })),
  hostSawPlayers: lastCount,
  finished: players.filter((p) => p.ended).length,
  quizSeconds: Math.round(quizMs / 1000),
  totalSeconds: Math.round((Date.now() - started) / 1000),
  joinMs: stats(metrics.join),
  questionFanoutServerMs: stats(metrics.fanoutRaw),
  questionFanoutWithNetworkMs: stats(metrics.fanout),
  answerRoundTripMs: stats(metrics.answerRtt),
  reconnects: metrics.reconnects,
  reconnectRecoveryMs: stats(metrics.recovery),
  answersSent: metrics.answersSent,
  answersAccepted: metrics.answersAccepted,
  resumedResults: metrics.resumedResults,
  errors: metrics.errors,
  scoreMismatches: metrics.wrongScores.length,
  scoreMismatchSample: metrics.wrongScores.slice(0, 5),
  leaderboardEntries: board.length,
  top3: board.slice(0, 3).map((e) => ({ userid: e.userid, score: e.score, avgS: +(e.totalms / Math.max(1, e.answered) / 1000).toFixed(2) })),
  accuracyByQuestion: statsRes.questions?.map((q) => Math.round((q.correct / Math.max(1, q.total)) * 100)),
  answersByQuestion: statsRes.questions?.map((q) => q.total),
  framesPerPlayer: Math.round(metrics.framesIn / PLAYERS),
  kbPerPlayer: +(metrics.bytesIn / PLAYERS / 1024).toFixed(1),
  hostFrames,
}
console.log(JSON.stringify(report, null, 2))
if (OUT) (await import('node:fs')).writeFileSync(OUT, JSON.stringify(report, null, 2))
hostWs.close()
process.exit(report.scoreMismatches || report.finished < PLAYERS ? 1 : 0)
