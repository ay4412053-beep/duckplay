// Sunucuyu kendi icinde dener: node test.js
const assert = require('assert')
const WebSocket = require('ws')
const { createServer } = require('./index.js')

const server = createServer()

function client(port) {
  const ws = new WebSocket(`ws://localhost:${port}`)
  const inbox = []
  const waiters = []
  ws.on('message', (raw) => {
    const m = JSON.parse(raw.toString())
    const i = waiters.findIndex((w) => w.test(m))
    if (i >= 0) waiters.splice(i, 1)[0].resolve(m)
    else inbox.push(m)
  })
  const next = (test = () => true) =>
    new Promise((resolve, reject) => {
      const i = inbox.findIndex(test)
      if (i >= 0) return resolve(inbox.splice(i, 1)[0])
      const t = setTimeout(() => reject(new Error('zaman asimi')), 2000)
      waiters.push({ test, resolve: (m) => (clearTimeout(t), resolve(m)) })
    })
  const send = (o) => ws.send(JSON.stringify(o))
  const open = new Promise((r) => ws.on('open', r))
  return { ws, send, next, open, inbox }
}

server.listen(0, async () => {
  const port = server.address().port
  try {
    const a = client(port), b = client(port)
    await Promise.all([a.open, b.open])

    a.send({ type: 'join', name: '  Ali ' })
    const ja = await a.next((m) => m.type === 'joined')
    assert.strictEqual(ja.name, 'Ali')
    assert.deepStrictEqual(ja.history, [])

    b.send({ type: 'join', name: 'ALI' })
    assert.strictEqual((await b.next((m) => m.type === 'error')).code, 'name_taken')
    b.send({ type: 'join', name: 'x' })
    assert.strictEqual((await b.next((m) => m.type === 'error')).code, 'bad_name')
    b.send({ type: 'join', name: 'Veli' })
    await b.next((m) => m.type === 'joined')

    a.send({ type: 'say', text: 'selam\u0000\n  kamyon  ' })
    const ma = await a.next((m) => m.type === 'msg')
    const mb = await b.next((m) => m.type === 'msg')
    assert.strictEqual(ma.text, 'selam kamyon')
    assert.strictEqual(mb.name, 'Ali')

    a.send({ type: 'say', text: 'x'.repeat(500) })
    assert.strictEqual((await b.next((m) => m.type === 'msg' && m.text.startsWith('xx'))).text.length, 200)

    // 5 mesaj siniri (2 tanesi gitti): 3 daha gecer, sonrasi reddedilir
    for (let i = 0; i < 4; i++) a.send({ type: 'say', text: 'm' + i })
    assert.strictEqual((await a.next((m) => m.type === 'error')).code, 'slow_down')

    // yeni gelen son mesajlari gorur
    const c = client(port)
    await c.open
    c.send({ type: 'join', name: 'Can' })
    const jc = await c.next((m) => m.type === 'joined')
    assert.ok(jc.history.length >= 5)
    assert.strictEqual(jc.online, 3)

    // katilmadan yazilamaz
    const d = client(port)
    await d.open
    d.send({ type: 'say', text: 'merhaba' })
    assert.strictEqual((await d.next((m) => m.type === 'error')).code, 'not_joined')

    // ayrilan gorulur
    b.ws.close()
    const left = await a.next((m) => m.type === 'presence' && m.left === 'Veli')
    assert.strictEqual(left.online, 2)

    console.log('Hepsi gecti: katilma, ad cakismasi, mesaj, temizlik, sinirlar, gecmis, ayrilma')
    process.exit(0)
  } catch (e) {
    console.error('TEST BASARISIZ:', e.message)
    process.exit(1)
  }
})
