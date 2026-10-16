// Edge-case suite for live rooms: reconnects, duplicates, timing, abuse,
// host drop, autopilot and scheduling. Needs Node 22+ (global WebSocket).
// Usage: node tests/edge.mjs [http://localhost:8080]
const BASE = process.argv[2] ?? 'http://localhost:8080'
const B = BASE + '/api'
const W = BASE.replace(/^http/, 'ws') + '/api'

let failed = 0
const ok = (cond, msg) => {
  if (cond) console.log('ok  ', msg)
  else {
    failed++
    console.error('FAIL', msg)
  }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms))

async function api(method, path, body, token) {
  const r = await fetch(B + path, {
    method,
    headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: 'Bearer ' + token } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  })
  return { status: r.status, data: await r.json().catch(() => ({})) }
}

/** WebSocket with a typed inbox: next(type) resolves with the next frame of that type. */
function sock(url) {
  const ws = new WebSocket(url)
  const inbox = []
  const waiters = new Set()
  let closed = false
  const s = {
    ws,
    inbox,
    closeCode: null,
    get closed() {
      return closed
    },
    open: new Promise((res, rej) => {
      ws.onopen = res
      ws.onerror = () => rej(new Error('ws error ' + url))
    }),
    send: (o) => ws.send(typeof o === 'string' ? o : JSON.stringify(o)),
    next(type, t = 4000, pred = () => true) {
      return new Promise((res, rej) => {
        const take = () => {
          const i = inbox.findIndex((m) => m.type === type && pred(m))
          if (i < 0) return false
          res(inbox.splice(i, 1)[0])
          return true
        }
        if (take()) return
        const w = () => {
          if (take()) {
            clearTimeout(to)
            waiters.delete(w)
          }
        }
        waiters.add(w)
        const to = setTimeout(() => {
          waiters.delete(w)
          rej(new Error(`timeout waiting for ${type}; inbox: ${inbox.map((m) => m.type).join(',')}`))
        }, t)
      })
    },
    count: (type) => inbox.filter((m) => m.type === type).length,
    close: () => ws.close(),
  }
  ws.onmessage = (e) => {
    inbox.push(JSON.parse(e.data))
    for (const w of [...waiters]) w()
  }
  ws.onclose = (e) => {
    closed = true
    s.closeCode = e.code
  }
  return s
}

const player = async (room, name, key = name + '-key') => {
  const s = sock(`${W}/connect?${new URLSearchParams({ roomid: room, userid: name, key })}`)
  await s.open
  return s
}
const join = (room, name, key = name + '-key') => fetch(`${B}/connect?${new URLSearchParams({ roomid: room, userid: name, key })}`)

const Q = (text, correct = 1, secs = 30) => ({ Question: text, Options: ['a', 'b', 'c', 'd'], Correctoption: String(correct), Eachquestiontime: secs })

async function admin() {
  const u = 'edge' + Math.random().toString(36).slice(2, 8)
  const r = await api('POST', '/admin/signup', { username: u, password: 'password123' })
  return r.data.token
}
async function room(T, questions, extra = {}) {
  const r = await api('POST', '/create-event', { Nameofroom: 'Edge ' + Date.now(), Questions: questions, ...extra }, T)
  if (r.status !== 200) throw new Error('create-event: ' + JSON.stringify(r.data))
  return r.data.roomid
}
const host = async (R, T) => {
  const h = sock(`${W}/host?roomid=${R}&token=${encodeURIComponent(T)}`)
  await h.open
  await h.next('welcome')
  return h
}

async function section(name, fn) {
  console.log(`\n── ${name}`)
  try {
    await fn()
  } catch (e) {
    failed++
    console.error('FAIL', name, '→', e.message)
  }
}

const T = await admin()

await section('Joining: names, validation, late join', async () => {
  const R = await room(T, [Q('q1'), Q('q2')])
  ok((await join(R, 'early')).status === 404, 'join before the room is live → 404')
  const h = await host(R, T)
  ok((await join(R, 'x'.repeat(25))).status === 400, 'name longer than 24 chars → 400')
  ok((await join(R, '   ')).status === 400, 'blank name → 400')
  ok((await join(R, 'bad<script>')).status === 400, 'name with markup characters → 400')
  const jose = await player(R, 'José Ñ')
  ok((await jose.next('welcome')).userid === 'José Ñ', 'unicode name accepted')
  const ada = await player(R, 'ada')
  await ada.next('welcome')
  ok((await join(R, 'ADA', 'someone-else')).status === 409, 'same name in other case + other key → 409 (no impersonation)')
  h.send({ type: 'question', questionno: 0 })
  await ada.next('question')
  await wait(1200)
  const late = await player(R, 'late')
  await late.next('welcome')
  const lq = await late.next('question')
  ok(lq.questionno === 0 && lq.deadline - lq.now < 30000 && lq.deadline - lq.now > 25000, 'late joiner gets the open question with the remaining time')
  late.send({ type: 'answer', questionno: 0, choice: 1 })
  ok((await late.next('result')).correct, 'late joiner can still answer')
  for (const s of [h, jose, ada, late]) s.close()
})

await section('Reconnects mid-question', async () => {
  const R = await room(T, [Q('q1', 2), Q('q2', 0)])
  const h = await host(R, T)
  let bob = await player(R, 'bob')
  await bob.next('welcome')
  h.send({ type: 'question', questionno: 0 })
  const q = await bob.next('question')

  // drop before answering → rejoin restores the question, same deadline
  bob.close()
  await wait(100)
  bob = await player(R, 'bob')
  await bob.next('welcome')
  const q2 = await bob.next('question')
  ok(q2.questionno === 0 && q2.deadline === q.deadline, 'rejoin before answering: same question, same deadline')
  ok(bob.count('result') === 0, 'rejoin before answering: no phantom result')

  // answer, drop, rejoin → answer state restored, no double points
  bob.send({ type: 'answer', questionno: 0, choice: 2 })
  ok((await bob.next('result')).score === 50, 'answer scores 50')
  bob.close()
  await wait(100)
  bob = await player(R, 'bob')
  const w = await bob.next('welcome')
  await bob.next('question')
  const resumed = await bob.next('result')
  ok(w.score === 50 && resumed.resumed && resumed.choice === 2, 'rejoin after answering: score kept, own answer restored')

  // the answer was sent but the result was lost: the client retries → same result
  bob.send({ type: 'answer', questionno: 0, choice: 3 })
  const retry = await bob.next('result')
  ok(retry.resumed && retry.choice === 2 && retry.score === 50, 'retried answer after a lost ack is idempotent (first answer stands)')

  // rejoin while still connected (second tab / flaky network takes over)
  const bob2 = await player(R, 'bob')
  await bob2.next('welcome')
  await wait(300)
  ok(bob.closed, 'same key on a new socket closes the stale one')
  const players = await h.next('players', 3000, (m) => m.count >= 1)
  ok(players.players.filter((p) => p.toLowerCase() === 'bob').length === 1, 'host sees bob once, not twice')

  // reveal while bob is offline, rejoin after reveal → no question re-sent
  bob2.close()
  h.send({ type: 'reveal' })
  await wait(300)
  const bob3 = await player(R, 'bob')
  await bob3.next('welcome')
  await wait(400)
  ok(bob3.count('question') === 0, 'rejoin after reveal: closed question not re-opened')
  bob3.send({ type: 'answer', questionno: 0, choice: 2 })
  const e = await bob3.next('result').catch(() => null)
  ok(e?.resumed, 'answering a revealed question you answered → original result, not a new score')
  h.close()
  bob3.close()
})

await section('Answer validation and timing', async () => {
  const R = await room(T, [Q('fast', 1, 5), Q('next', 1, 30)])
  const h = await host(R, T)
  const p = await player(R, 'carol')
  await p.next('welcome')
  p.send({ type: 'answer', questionno: 0, choice: 1 })
  ok((await p.next('error')).code === 'no_question', 'answer before any question → no_question')
  h.send({ type: 'question', questionno: 0 })
  await p.next('question')
  for (const [choice, why] of [[9, 'out of range'], [-1, 'negative']]) {
    p.send({ type: 'answer', questionno: 0, choice })
    ok((await p.next('error')).code === 'invalid', `choice ${why} → invalid`)
  }
  p.send({ type: 'answer', questionno: 1, choice: 1 })
  ok((await p.next('error')).code === 'closed', 'answer for a different question → closed')
  p.send('{not json')
  ok((await p.next('error')).message === 'invalid JSON', 'malformed JSON → error, socket stays open')
  p.send({ type: 'teleport' })
  ok((await p.next('error')).message.includes('unknown'), 'unknown message type → error')
  await wait(5000 + 1700) // 5 s timer + 1.5 s grace
  p.send({ type: 'answer', questionno: 0, choice: 1 })
  ok((await p.next('error')).code === 'late', 'answer after timer + grace → late')
  ok(!p.closed, 'player still connected after bad input')
  h.close()
  p.close()
})

await section('Flooding', async () => {
  const R = await room(T, [Q('q')])
  const h = await host(R, T)
  const p = await player(R, 'spam')
  await p.next('welcome')
  for (let i = 0; i < 6; i++) p.send({ type: 'ping' })
  await wait(500)
  ok(p.count('pong') === 6 && p.count('error') === 0, 'a short burst (6 msgs) is allowed')
  await wait(2000)
  for (let i = 0; i < 15; i++) p.send({ type: 'ping' })
  await wait(800)
  ok(p.count('error') > 0 && !p.closed, 'a bigger burst gets "slow down" errors but stays connected')
  await wait(2000)
  for (let i = 0; i < 60; i++) p.send({ type: 'ping' })
  await wait(1500)
  ok(p.closed, 'persistent flooder is disconnected')
  const fine = await player(R, 'calm')
  ok((await fine.next('welcome')).type === 'welcome', 'other players unaffected')
  h.close()
  fine.close()
})

await section('Host drops and returns mid-quiz', async () => {
  const R = await room(T, [Q('q1', 3), Q('q2', 3)])
  let h = await host(R, T)
  const ps = await Promise.all(['p1', 'p2', 'p3'].map((n) => player(R, n)))
  await Promise.all(ps.map((p) => p.next('welcome')))
  h.send({ type: 'question', questionno: 0 })
  await Promise.all(ps.map((p) => p.next('question')))
  h.close() // host loses connection
  await wait(300)
  ps[0].send({ type: 'answer', questionno: 0, choice: 3 })
  ok((await ps[0].next('result')).correct, 'players keep playing while the host is away')
  ok((await api('GET', '/rooms/' + R)).data.live, 'room stays live without a host')
  h = sock(`${W}/host?roomid=${R}&token=${encodeURIComponent(T)}`)
  await h.open
  const w = await h.next('welcome')
  ok(w.state?.questionno === 0 && !w.state.revealed, 'reconnected host resumes the open question')
  ok(w.state.stats.counts[3] === 1, 'reconnected host sees answers given while away')
  const pl = await h.next('players')
  ok(pl.count === 3, 'reconnected host sees all 3 players')
  ok((await join(R, 'p4')).status !== 404, 'new players can still join')
  const bad = sock(`${W}/host?roomid=${R}&token=forged.token`)
  ok(await bad.open.then(() => false, () => true), 'forged host token cannot take over the room')
  h.send({ type: 'end' })
  const ends = await Promise.all(ps.map((p) => p.next('end')))
  ok(ends.every((e) => e.leaderboard[0].userid === 'p1'), 'everyone receives the final board')
  await wait(1800)
  ok(ps.every((p) => p.closed), 'sockets close after the end')
  ok((await join(R, 'p1')).status === 404, 'rejoining an ended room → 404')
  const info = (await api('GET', '/rooms/' + R)).data
  ok(info.finished && !info.live, 'room info says finished')
})

await section('Editing a live room', async () => {
  const R = await room(T, [Q('q1'), Q('q2'), Q('q3')])
  const h = await host(R, T)
  const ev = (await api('GET', '/admin/events/' + R, null, T)).data.event
  ev.Questions = ev.Questions.slice(0, 1)
  ok((await api('PUT', '/admin/events/' + R, ev, T)).status === 200, 'editor can save while live')
  await h.next('room', 3000, (m) => m.room.questioncount === 1)
  h.send({ type: 'question', questionno: 2 })
  ok((await h.next('error')).message === 'no such question', 'host cannot open a deleted question')
  ok((await api('DELETE', '/admin/events/' + R, null, T)).status === 409, 'cannot delete a live room')
  h.send({ type: 'end' })
  await wait(300)
})

await section('Autopilot runs the whole quiz', async () => {
  const R = await room(T, [Q('a1', 0, 5), Q('a2', 1, 5)])
  const h = await host(R, T)
  const p = await player(R, 'auto')
  await p.next('welcome')
  h.send({ type: 'auto', delay: 1 })
  ok((await h.next('room')).room.auto === true, 'host sees auto mode on')
  const q0 = await p.next('question', 4000)
  p.send({ type: 'answer', questionno: q0.questionno, choice: 0 })
  await p.next('result')
  const r0 = await p.next('reveal', 9000)
  ok(r0.questionno === 0 && r0.correctoption === 0, 'auto reveals after the timer')
  const q1 = await p.next('question', 7000)
  ok(q1.questionno === 1, 'auto opens the next question')
  h.send({ type: 'end' }) // host ends while autopilot is mid-question
  await p.next('end')
  await wait(9000) // past when autopilot would have revealed and ended
  ok(p.count('end') === 0 && p.count('reveal') === 0, 'autopilot stops after a manual end (no second end)')
})

await section('Scheduled test opens a lobby and starts itself', async () => {
  const start = new Date(Date.now() + 25_000)
  const R = await room(T, [Q('s1', 2, 5)], { ScheduledAt: start.toISOString(), AutoStart: true })
  ok((await api('POST', '/create-event', { Nameofroom: 'x', AutoStart: true }, T)).status === 400, 'auto-start without a time → 400')
  let info
  for (let i = 0; i < 20 && !(info = (await api('GET', '/rooms/' + R)).data).live; i++) await wait(1000)
  ok(info.live && info.autostart, 'scheduler opened the lobby before the start time')
  const p = await player(R, 'early-bird')
  const w = await p.next('welcome')
  ok(Math.abs(w.room.startsat - start.getTime()) < 1000, 'lobby tells players when it starts')
  const q = await p.next('question', 30000)
  ok(Math.abs(Date.now() - start.getTime()) < 2500, 'first question opens on schedule')
  p.send({ type: 'answer', questionno: q.questionno, choice: 2 })
  ok((await p.next('result')).correct, 'scheduled quiz scores answers')
  const end = await p.next('end', 15000)
  ok(end.leaderboard[0].userid === 'early-bird', 'scheduled quiz ends by itself')
  await wait(16000) // one more scheduler tick
  ok(!(await api('GET', '/rooms/' + R)).data.live, 'finished scheduled test is not reopened')
})

await section('Reset players between runs', async () => {
  const R = await room(T, [Q('r1', 2)])
  const other = await admin()
  ok((await api('POST', `/admin/events/${R}/reset`, null, other)).status === 404, "another admin cannot reset someone else's test")
  let h = await host(R, T)
  let ps = await Promise.all(['ra', 'rb'].map((n) => player(R, n)))
  await Promise.all(ps.map((p) => p.next('welcome')))
  h.send({ type: 'question', questionno: 0 })
  await Promise.all(ps.map((p) => p.next('question')))
  ps.forEach((p) => p.send({ type: 'answer', questionno: 0, choice: 2 }))
  await Promise.all(ps.map((p) => p.next('result')))
  h.send({ type: 'end' })
  await wait(1800)
  ok((await api('GET', `/rooms/${R}/leaderboard`)).data.entries.length === 2, 'first run left 2 players on the board')

  // Reset while not live clears the stored answers.
  const res = await api('POST', `/admin/events/${R}/reset`, null, T)
  ok(res.status === 200 && res.data.answers === 2 && !res.data.live, 'reset of an ended test removes its 2 answers')
  ok((await api('GET', `/rooms/${R}/leaderboard`)).data.entries.length === 0, 'board is empty after reset')

  // Going live again starts clean: the same player can score again.
  h = await host(R, T)
  ps = [await player(R, 'ra')]
  const w = await ps[0].next('welcome')
  ok(w.score === 0 && w.leaderboard.length === 0, 'returning player starts at 0 on an empty board')
  h.send({ type: 'question', questionno: 0 })
  await ps[0].next('question')
  ps[0].send({ type: 'answer', questionno: 0, choice: 2 })
  const r = await ps[0].next('result')
  ok(r.correct && !r.resumed && r.score === 50, 'returning player can answer again (not a resumed old answer)')

  // Reset while live kicks players and returns the room to its lobby.
  const res2 = await api('POST', `/admin/events/${R}/reset`, null, T)
  ok(res2.status === 200 && res2.data.players === 1 && res2.data.answers === 1 && res2.data.live, 'live reset removes 1 player and 1 answer, room stays live')
  ok((await ps[0].next('reset')).type === 'reset', 'player is told the room was reset')
  await wait(300)
  ok(ps[0].closed, 'player socket is closed after reset')
  const hr = await h.next('reset')
  ok(hr.room.current === -1 && hr.answers === 1, 'host is told the room is back in its lobby')
  ok((await h.next('players', 4000, (m) => m.count === 0)).count === 0, 'host player list is empty')
  const back = await player(R, 'ra', 'another-key')
  const w2 = await back.next('welcome')
  ok(w2.score === 0, 'player can rejoin after reset, even from a new browser')

  // The host can also reset from its control socket.
  h.send({ type: 'reset' })
  ok((await back.next('reset')).type === 'reset', 'host socket reset kicks players too')
  h.send({ type: 'end' })
  await wait(300)
})

await section('Reconnect storm', async () => {
  const R = await room(T, [Q('storm', 1)])
  const h = await host(R, T)
  const names = Array.from({ length: 25 }, (_, i) => 'storm' + i)
  let ps = await Promise.all(names.map((n) => player(R, n)))
  await Promise.all(ps.map((p) => p.next('welcome')))
  h.send({ type: 'question', questionno: 0 })
  await Promise.all(ps.map((p) => p.next('question')))
  ps.slice(0, 10).forEach((p) => p.send({ type: 'answer', questionno: 0, choice: 1 }))
  await Promise.all(ps.slice(0, 10).map((p) => p.next('result')))
  ps.forEach((p) => p.close()) // everyone drops at once
  await wait(200)
  ps = await Promise.all(names.map((n) => player(R, n)))
  const back = await Promise.all(ps.map((p) => p.next('question')))
  ok(back.every((q) => q.questionno === 0), 'all 25 players get the open question after a mass reconnect')
  const restored = await Promise.all(ps.slice(0, 10).map((p) => p.next('result')))
  ok(restored.every((r) => r.resumed && r.score === 50), 'the 10 who had answered get their answers back')
  ps.slice(10).forEach((p) => p.send({ type: 'answer', questionno: 0, choice: 0 }))
  await Promise.all(ps.slice(10).map((p) => p.next('result')))
  const pl = await h.next('players', 4000, (m) => m.count === 25)
  ok(pl.count === 25, 'host count settles at 25 (no ghosts)')
  const board = (await api('GET', `/rooms/${R}/leaderboard`)).data.entries
  ok(board.length === 25 && board.filter((e) => e.score === 50).length === 10, 'board: exactly 10 scored, nobody double-counted')
  h.send({ type: 'end' })
  await wait(300)
})

console.log(failed ? `\n${failed} FAILED` : '\nall edge cases passed')
process.exit(failed ? 1 : 0)
