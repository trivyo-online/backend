// End-to-end API + socket test. Usage: node tests/e2e.mjs [http://localhost:8080]
const BASE = process.argv[2] ?? 'http://localhost:8080'
const B = BASE + '/api', W = BASE.replace(/^http/, 'ws') + '/api'
const ok = (c, m) => { if (!c) { console.error('FAIL:', m); process.exitCode = 1 } else console.log('ok  ', m) }
async function j(method, path, body, token) {
  const r = await fetch(B + path, { method, headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: 'Bearer ' + token } : {}) }, body: body ? JSON.stringify(body) : undefined })
  return { status: r.status, data: await r.json() }
}
function sock(url) {
  const ws = new WebSocket(url), msgs = [], waiters = []
  ws.onmessage = e => { const m = JSON.parse(e.data); msgs.push(m); [...waiters].forEach(w => w()) }
  const next = (type, t = 3000) => new Promise((res, rej) => {
    const check = () => { const i = msgs.findIndex(m => m.type === type); if (i >= 0) { res(msgs.splice(i, 1)[0]); return true } }
    if (check()) return
    const w = () => { if (check()) { clearTimeout(to); waiters.splice(waiters.indexOf(w), 1) } }; waiters.push(w)
    const to = setTimeout(() => rej(new Error('timeout waiting for ' + type + ' got ' + JSON.stringify(msgs.map(m => m.type)))), t)
  })
  return { ws, next, send: o => ws.send(JSON.stringify(o)), open: new Promise((r, x) => { ws.onopen = r; ws.onerror = x }) }
}
const u = 'admin' + Date.now() % 100000
let r = await j('POST', '/admin/signup', { username: u, password: 'short' }); ok(r.status === 400, 'weak password rejected')
r = await j('POST', '/admin/signup', { username: u, password: 'password123' }); ok(r.status === 200 && r.data.token, 'signup returns token')
r = await j('POST', '/admin/login', { username: u, password: 'nope-nope' }); ok(r.status === 401, 'bad login rejected')
r = await j('POST', '/admin/login', { username: u, password: 'password123' }); const T = r.data.token; ok(r.status === 200 && T, 'login')
r = await j('POST', '/create-event', { Nameofroom: 'x' }); ok(r.status === 401, 'create-event needs auth')
r = await j('POST', '/create-event', { Nameofroom: 'Friday Quiz', DateofQuiz: '2026-10-16', StartTime: '19:00' }, T)
const R = r.data.roomid; ok(r.status === 200 && /^[A-Z0-9]{8}$/.test(R), 'create-event returns roomid ' + R)
r = await j('POST', '/add-question', { roomid: R, question: { Question: 'Q1?', Options: ['a', 'b', 'c', 'd'], Correctoption: '1' } }, T); ok(r.status === 200, 'add-question')
r = await j('POST', '/add-question', { roomid: R, question: { Question: 'bad', Options: ['a'], Correctoption: '0' } }, T); ok(r.status === 400, 'invalid question rejected')
r = await j('GET', '/admin/events/' + R, null, T); const ev = r.data.event; ok(ev.Questions.length === 1, 'get event')
ev.Questions.push({ Question: 'Q2?', Options: ['x', 'y'], Correctoption: '0', Eachquestiontime: 10 }); ev.Nameofroom = 'Friday Quiz II'
r = await j('PUT', '/admin/events/' + R, ev, T); ok(r.status === 200 && r.data.event.Questions.length === 2, 'editor save (PUT)')
r = await j('GET', '/admin/events', null, T); ok(r.data.events[0].RoomId === R && r.data.events[0].QuestionCount === 2, 'list events')
const other = (await j('POST', '/admin/signup', { username: u + 'b', password: 'password123' })).data.token
r = await j('GET', '/admin/events/' + R, null, other); ok(r.status === 404, 'other admin cannot see event')
r = await j('GET', '/rooms/' + R); ok(r.data.live === false && r.data.name === 'Friday Quiz II', 'public room info')

const bad = await fetch(B.replace('http', 'http') + '/connect?roomid=' + R + '&userid=ada'); ok(bad.status === 404, 'join before live → 404')
const host = sock(`${W}/host?roomid=${R}&token=${encodeURIComponent(T)}`); await host.open
const hw = await host.next('welcome'); ok(hw.room.questioncount === 2, 'host welcome')
r = await j('GET', '/rooms/' + R); ok(r.data.live === true, 'room live')
const ada = sock(`${W}/connect?roomid=${R.toLowerCase()}&userid=ada&key=k1`); await ada.open
const bob = sock(`${W}/connect?roomid=${R}&userid=bob`); await bob.open
ok((await ada.next('welcome')).room.name === 'Friday Quiz II', 'player welcome')
await bob.next('welcome')
const dup = await fetch(`${B}/connect?roomid=${R}&userid=ADA&key=other`); ok(dup.status === 409, 'same name, different key rejected')
const ada2 = sock(`${W}/connect?roomid=${R}&userid=Ada&key=k1`); await ada2.open
ok((await ada2.next('welcome')).userid === 'Ada', 'same key rejoins (takeover)')
await new Promise(r => setTimeout(r, 300)); ok(ada.ws.readyState === 3, 'stale socket closed on takeover')
ada2.ws.close(); await new Promise(r => setTimeout(r, 300))
const ada3 = sock(`${W}/connect?roomid=${R}&userid=ada&key=k1`); await ada3.open; await ada3.next('welcome')
Object.assign(ada, ada3)
ada.send({ type: 'answer', questionno: 0, choice: 1 }); ok((await ada.next('error')).message.includes('no question'), 'answer before question rejected')
host.send({ type: 'question', questionno: 0 })
const q = await ada.next('question'); ok(q.question.Question === 'Q1?' && q.question.Correctoption === undefined, 'question pushed without answer key')
await bob.next('question')
ada.send({ type: 'answer', questionno: 0, choice: 1 })
let res = await ada.next('result'); ok(res.correct && res.score === 50, 'correct answer scores 50')
ada.send({ type: 'answer', questionno: 0, choice: 1 }); { const again = await ada.next('result'); ok(again.resumed && again.score === 50, 'repeat answer is idempotent (same result, no extra points)') }
bob.send({ ans: 'c' }) // legacy text format
res = await bob.next('result'); ok(!res.correct && res.score === 0, 'legacy text answer graded wrong')
const ha = await host.next('answer'); ok(ha.userid === 'ada' && ha.correct, 'host sees answers')
const lb = await ada.next('leaderboard'); ok(lb.entries.length >= 1 && lb.entries[0].userid === 'ada', 'live leaderboard broadcast')
host.send({ type: 'reveal' }); const rv = await bob.next('reveal'); ok(rv.correctoption === 1 && rv.stats.counts[1] === 1 && rv.stats.counts[2] === 1, 'reveal with distribution')
host.send({ type: 'next' }); ok((await bob.next('question')).questionno === 1, 'next question')
bob.send({ type: 'answer', questionno: 1, choice: 0 }); ok((await bob.next('result')).score === 50, 'bob scores on Q2')
r = await j('GET', '/rooms/' + R + '/stats'); ok(r.status === 409, 'stats hidden while live')
host.send({ type: 'end' })
const end = await ada.next('end'); ok(end.leaderboard.length === 2, 'end message with final board')
await new Promise(r => setTimeout(r, 800))
r = await j('GET', '/rooms/' + R); ok(r.data.live === false, 'room no longer live')
r = await j('GET', '/rooms/' + R + '/leaderboard'); ok(r.data.entries.map(e => e.userid).join() === 'ada,bob' || r.data.entries.map(e => e.userid).join() === 'bob,ada', 'room leaderboard ' + JSON.stringify(r.data.entries.map(e => [e.userid, e.score])))
r = await j('GET', '/rooms/' + R + '/stats'); ok(r.data.questions.length === 2 && r.data.questions[0].total === 2 && r.data.questions[0].correct === 1, 'room stats')
r = await j('GET', '/leaderboard'); ok(r.data.entries.length >= 2 && r.data.totals.answers >= 3, 'global leaderboard ' + JSON.stringify(r.data.totals))
r = await j('GET', '/rooms'); ok(r.data.rooms.some(x => x.roomid === R && x.players === 2), 'recent rooms')
r = await j('DELETE', '/admin/events/' + R, null, T); ok(r.status === 200, 'delete event')
for (const s of [host, ada, bob]) s.ws.close()
