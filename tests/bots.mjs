// Fill a live room with simulated players that answer every question.
// Usage: node tests/bots.mjs <room> [count=50] [base=http://localhost:8080] [answers=0,2,1,...] [accuracy=0.65]
// With an answers list (correct option index per question) bots hit the
// given accuracy; without it they pick at random.
const [ROOM, N = '50', BASE = 'http://localhost:8080', ANSWERS = '', ACC = '0.65'] = process.argv.slice(2)
if (!ROOM) {
  console.error('usage: node tests/bots.mjs <room> [count] [base] [answers] [accuracy]')
  process.exit(2)
}
const W = BASE.replace(/^http/, 'ws') + '/api'
const answers = ANSWERS ? ANSWERS.split(',').map(Number) : null
const NAMES = ['ada', 'alan', 'grace', 'linus', 'ken', 'barbara', 'edsger', 'margaret', 'tim', 'radia', 'donald', 'frances', 'john', 'hedy', 'claude', 'katherine', 'dennis', 'anita', 'guido', 'sophie']
const rand = (a, b) => a + Math.random() * (b - a)
let done = 0

function bot(i) {
  const name = `${NAMES[i % NAMES.length]}_${Math.floor(i / NAMES.length) + 1}`
  const skill = Math.min(0.97, Math.max(0.2, Number(ACC) + rand(-0.25, 0.25)))
  const speed = rand(0.8, 1.6)
  const ws = new WebSocket(`${W}/connect?${new URLSearchParams({ roomid: ROOM, userid: name, key: 'bot-' + i })}`)
  ws.onmessage = (e) => {
    const f = JSON.parse(e.data)
    if (f.type === 'question' && Math.random() > 0.04) {
      const n = f.question.Options.length
      const right = answers?.[f.questionno]
      const choice = right !== undefined && Math.random() < skill ? right : Math.floor(Math.random() * n)
      const left = f.deadline - f.now
      setTimeout(() => ws.readyState === 1 && ws.send(JSON.stringify({ type: 'answer', questionno: f.questionno, choice })), Math.min(left - 300, rand(900, 4500) * speed))
    }
    if (f.type === 'end' && ++done === Number(N)) {
      console.log('all bots finished')
      process.exit(0)
    }
  }
  ws.onerror = () => console.error(name, 'could not connect')
}

for (let i = 0; i < Number(N); i++) setTimeout(() => bot(i), (i / Number(N)) * 2500)
console.log(`joining ${N} bots to ${ROOM}`)
