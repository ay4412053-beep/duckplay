// DuckPlay sohbet sunucusu: tek bir herkese acik oda ("Genel").
// Istemci -> sunucu:  {type:"join", name}   {type:"say", text}
// Sunucu -> istemci:  {type:"joined", name, history, online}   {type:"msg", id, name, text, ts}
//                     {type:"presence", online, joined?|left?}  {type:"error", code}
const http = require('http')
const { WebSocketServer } = require('ws')

const MAX_NAME = 20
const MIN_NAME = 2
const MAX_TEXT = 200
const HISTORY = 50
const RATE_COUNT = 5 // en fazla 5 mesaj
const RATE_WINDOW_MS = 10_000 // 10 saniyede

/** Kontrol karakterlerini atar, bosluklari teke indirir, sinira keser. */
function clean(value, max) {
  return String(value ?? '')
    .replace(/[\u0000-\u001f\u007f]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, max)
}

function createServer() {
  const history = []
  const users = new Map() // ws -> { name, times: [] }
  let nextId = 1

  const send = (ws, obj) => {
    if (ws.readyState === 1) ws.send(JSON.stringify(obj))
  }
  const broadcast = (obj) => {
    const data = JSON.stringify(obj)
    for (const ws of users.keys()) if (ws.readyState === 1) ws.send(data)
  }
  const nameTaken = (name) => {
    const lower = name.toLowerCase()
    for (const u of users.values()) if (u.name.toLowerCase() === lower) return true
    return false
  }

  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' })
    res.end('DuckPlay chat OK\n')
  })
  const wss = new WebSocketServer({ server, maxPayload: 2048 })

  function join(ws, m) {
    if (users.has(ws)) return
    const name = clean(m.name, MAX_NAME)
    if (name.length < MIN_NAME) return send(ws, { type: 'error', code: 'bad_name' })
    if (nameTaken(name)) return send(ws, { type: 'error', code: 'name_taken' })
    users.set(ws, { name, times: [] })
    send(ws, { type: 'joined', name, history, online: users.size })
    broadcast({ type: 'presence', online: users.size, joined: name })
  }

  function say(ws, user, m) {
    const text = clean(m.text, MAX_TEXT)
    if (!text) return
    const now = Date.now()
    user.times = user.times.filter((t) => now - t < RATE_WINDOW_MS)
    if (user.times.length >= RATE_COUNT) return send(ws, { type: 'error', code: 'slow_down' })
    user.times.push(now)
    const msg = { type: 'msg', id: nextId++, name: user.name, text, ts: now }
    history.push(msg)
    if (history.length > HISTORY) history.shift()
    broadcast(msg)
  }

  wss.on('connection', (ws) => {
    ws.isAlive = true
    ws.on('pong', () => {
      ws.isAlive = true
    })
    ws.on('message', (raw) => {
      let m
      try {
        m = JSON.parse(raw.toString())
      } catch {
        return
      }
      if (!m || typeof m !== 'object') return
      if (m.type === 'join') return join(ws, m)
      const user = users.get(ws)
      if (!user) return send(ws, { type: 'error', code: 'not_joined' })
      if (m.type === 'say') say(ws, user, m)
    })
    ws.on('close', () => {
      const user = users.get(ws)
      if (!user) return
      users.delete(ws)
      broadcast({ type: 'presence', online: users.size, left: user.name })
    })
    ws.on('error', () => {})
  })

  // Olu baglantilari temizle: 30 saniyede bir ping, cevap vermeyeni kapat.
  const beat = setInterval(() => {
    for (const ws of wss.clients) {
      if (!ws.isAlive) {
        ws.terminate()
        continue
      }
      ws.isAlive = false
      ws.ping()
    }
  }, 30_000)
  server.on('close', () => clearInterval(beat))

  return server
}

module.exports = { createServer, clean }

if (require.main === module) {
  const port = Number(process.env.PORT) || 8080
  createServer().listen(port, () => console.log(`DuckPlay chat server: ws://localhost:${port}`))
}
