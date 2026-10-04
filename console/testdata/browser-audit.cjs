// Browser checks for the console, one per item in section U of
// docs/reports/AUDIT_2026-10.md. Each drives a real page and prints PASS or FAIL.
//
//   node console/testdata/browser-audit.cjs http://127.0.0.1:9093/_console [path/to/playwright]
//
// It needs a running broker with: a topic "orders" (3 partitions, 20+ records on
// partition 0, one with a very long key at the highest offset), a topic
// "payments.events.v1", and an empty group with committed offsets on it named
// by AUDIT_GROUP (default "a-group/with?odd#chars"). It produces a few records.
// These become Playwright specs under e2e/ in work package 7.4.
const base = (process.argv[2] || 'http://127.0.0.1:9093/_console').replace(/\/$/, '');
const { chromium } = require(process.argv[3] || 'playwright');
const GROUP = process.env.AUDIT_GROUP || 'a-group/with?odd#chars';
const only = process.env.ONLY ? process.env.ONLY.split(',') : null;

let failed = 0;
const results = [];
function report(id, ok, detail) {
  if (!ok) failed++;
  results.push(`${ok ? 'PASS' : 'FAIL'}  ${id}${detail ? '  ' + detail : ''}`);
}

(async () => {
  const browser = await chromium.launch();
  const errors = [];
  async function open(path, opts = {}) {
    const ctx = await browser.newContext({ viewport: { width: opts.w || 1440, height: opts.h || 900 }, colorScheme: opts.scheme || 'light' });
    const page = await ctx.newPage();
    page.on('pageerror', e => errors.push(`${path}: ${e.message}`));
    if (opts.init) await page.addInitScript(opts.init);
    await page.goto(base + path, { waitUntil: 'networkidle' });
    if (opts.dock) await page.click('#compose-head'); // open the composer.
    return page;
  }
  const produce = (page, topic, partition, value) =>
    page.request.post(`${base}/topics/${topic}/produce`, { form: { partition: String(partition), value, partition_ctx: String(partition) } });
  // The offset of the nth row in the table (0 = newest shown). The checks use rows
  // that are on screen rather than fixed offsets, so they do not depend on how
  // many records the topic happens to hold.
  const rowOffset = (page, n) => page.evaluate(n => Number(document.querySelectorAll('tr[id^="msg-"]')[n].id.slice(4)), n);
  async function check(id, fn) {
    if (only && !only.includes(id)) return;
    try { await fn(); } catch (e) { report(id, false, 'threw: ' + String(e.message).split('\n')[0]); }
  }

  await check('U1', async () => {
    const page = await open('/topics');
    for (const combo of ['Meta+c', 'Control+c', 'Alt+c', 'Meta+j', 'Control+m']) await page.keyboard.press(combo);
    await page.waitForTimeout(300);
    report('U1', page.url().endsWith('/topics'), 'after modifier+key the page is ' + page.url().replace(base, ''));
    await page.context().close();
  });

  await check('U2', async () => {
    let worst = null;
    for (const h of [900, 700, 560]) {
      const page = await open('/topics/orders', { h, dock: true });
      const r = await page.evaluate(() => {
        const wb = document.querySelector('.detail.workbench');
        wb.scrollTop = wb.scrollHeight;
        const t = document.querySelector('.tbl-wrap').getBoundingClientRect(), d = document.querySelector('.dock').getBoundingClientRect();
        return { tableVisible: Math.round(Math.min(t.bottom, d.top) - t.top), overlap: Math.round(t.bottom - d.top) };
      });
      if (r.tableVisible < 100 || r.overlap > 1) worst = { h, ...r };
      await page.context().close();
    }
    report('U2', !worst, worst ? JSON.stringify(worst) : 'the table stays clear of the open composer at 900, 700 and 560px');
  });

  await check('U3', async () => {
    const page = await open('/topics/orders');
    await page.fill('#jump-to', '12');
    await page.evaluate(() => document.getElementById('tape-and-table').setAttribute('data-mark', '1'));
    await produce(page, 'orders', 0, 'u3');
    await page.waitForFunction(() => !document.getElementById('tape-and-table').hasAttribute('data-mark'), null, { timeout: 8000 });
    await page.waitForTimeout(300); // let the swap settle.
    const st = await page.evaluate(() => ({ v: document.getElementById('jump-to').value, focused: document.activeElement && document.activeElement.id }));
    report('U3', st.v === '12' && st.focused === 'jump-to', JSON.stringify(st));
    await page.context().close();
  });

  await check('U4', async () => {
    const page = await open('/topics/orders');
    await page.route('**/message?*', async r => { await new Promise(x => setTimeout(x, 900)); r.continue(); });
    const at = await rowOffset(page, 5);
    await page.click('#msg-' + at);
    await page.waitForSelector(`#drawer-title[data-offset="${at}"]`);
    await page.click('#drawer button[aria-label="Older record"]');
    await page.waitForSelector(`#drawer-title[data-offset="${at - 1}"]`);
    await page.waitForTimeout(200);
    const barHidden = await page.evaluate(() => document.getElementById('reqbar').hidden);
    await page.unroute('**/message?*');
    await page.route('**/message?*', r => r.abort());
    await page.click('#drawer button[aria-label="Older record"]');
    await page.waitForTimeout(500);
    const st = await page.evaluate(() => ({ text: document.getElementById('drawer').innerText.replace(/\s+/g, ' ').slice(0, 80), alert: !!document.querySelector('#drawer [role=alert]'), bar: document.getElementById('reqbar').hidden }));
    report('U4', barHidden && st.alert && st.bar && !/Loading/.test(st.text), JSON.stringify({ barHidden, ...st }));
    await page.context().close();
  });

  await check('U6', async () => {
    const page = await open('/topics/orders');
    const at = await rowOffset(page, 5);
    await page.click('#msg-' + at);
    await page.waitForSelector(`#drawer-title[data-offset="${at}"]`);
    await page.keyboard.press('c'); await page.keyboard.press('m'); await page.keyboard.press('j');
    await page.waitForTimeout(300);
    const a = await page.evaluate(() => ({ url: location.pathname, dock: document.documentElement.getAttribute('data-dock'), cursor: !!document.querySelector('.li.cursor') }));
    await page.keyboard.press('Escape');
    await page.click('.idband-acts .btn.danger');
    await page.keyboard.press('Meta+k'); await page.keyboard.press('c');
    await page.waitForTimeout(200);
    const b = await page.evaluate(() => ({ palette: !document.getElementById('palette').hidden, confirm: !document.getElementById('confirm').hidden, url: location.pathname }));
    const ok = a.url.endsWith('/topics/orders') && a.dock !== 'open' && !a.cursor && !b.palette && b.confirm && b.url.endsWith('/topics/orders');
    report('U6', ok, JSON.stringify({ a, b }));
    await page.context().close();
  });

  await check('U7', async () => {
    const page = await open('/groups/' + encodeURIComponent(GROUP));
    let posted = 0;
    page.on('request', r => { if (r.method() === 'POST' && r.url().includes('/reset')) posted++; });
    await page.click('text=Reset to latest');
    await page.waitForSelector('#confirm:not([hidden])');
    await produce(page, 'payments.events.v1', 0, 'u7'); await produce(page, 'payments.events.v1', 1, 'u7');
    await page.waitForTimeout(4000); // a poll or two, with the lag now different.
    await page.click('#confirm-yes');
    await page.waitForTimeout(800);
    report('U7', posted === 1, `reset requests sent after confirming: ${posted}`);
    await page.context().close();
  });

  await check('U8', async () => {
    const page = await open('/topics/orders');
    await page.click('[data-live-toggle]');
    await page.click('.rail a[href$="/groups"]');
    await page.waitForSelector('.listpane .li');
    await page.click('.listpane .li >> nth=1');
    await page.waitForSelector('#group-live');
    let polls = 0;
    page.on('request', r => { if (r.url().includes('/live')) polls++; });
    await page.waitForTimeout(4000);
    report('U8', polls >= 1, `group polls in 4s after pausing a topic and navigating: ${polls}`);
    await page.context().close();
  });

  await check('U9', async () => {
    const page = await open('/topics/orders');
    await page.keyboard.press('j'); await page.keyboard.press('j');
    await page.focus('.idband-acts .btn.btn-outline');
    await page.keyboard.press('Enter');
    await page.waitForTimeout(400);
    const st = await page.evaluate(() => ({ url: location.pathname, confirm: !document.getElementById('confirm').hidden }));
    report('U9', st.confirm && st.url.endsWith('/topics/orders'), JSON.stringify(st));
    await page.context().close();
  });

  await check('U10', async () => {
    const page = await open('/topics/orders', { dock: true });
    let posts = 0;
    page.on('request', r => { if (r.method() === 'POST' && r.url().includes('/produce')) posts++; });
    await page.click('.dock .CodeMirror');
    await page.keyboard.type('held');
    await page.keyboard.down('Meta');
    for (let i = 0; i < 8; i++) await page.keyboard.down('Enter'); // a held key: the first press, then repeats.
    await page.keyboard.up('Enter'); await page.keyboard.up('Meta');
    await page.waitForTimeout(1200);
    report('U10', posts === 1, `records sent by one held ⌘⏎: ${posts}`);
    await page.context().close();
  });

  // The composer belongs to the page it was opened on: it stays through a change of
  // partition, and closes when you move to another topic. (The audit's U11 asked for
  // it to be remembered across topics; in use that covered the next topic's messages.)
  await check('U11', async () => {
    const page = await open('/topics/orders');
    await page.click('#compose-head');
    await page.click('.ptab >> nth=1');
    await page.waitForFunction(() => /partition=1/.test(location.search));
    await page.waitForTimeout(300);
    const same = await page.evaluate(() => ({ shown: document.querySelector('.dock-form').offsetParent !== null, aria: document.getElementById('compose-head').getAttribute('aria-expanded') }));
    await page.click('.listpane .li[data-name="payments.events.v1"]');
    await page.waitForFunction(() => location.pathname.endsWith('payments.events.v1'));
    await page.waitForTimeout(300);
    const other = await page.evaluate(() => ({ shown: document.querySelector('.dock-form').offsetParent !== null, aria: document.getElementById('compose-head').getAttribute('aria-expanded') }));
    report('U11', same.shown && same.aria === 'true' && !other.shown && other.aria === 'false', JSON.stringify({ samePartitionChange: same, otherTopic: other }));
    await page.context().close();
  });

  await check('U12', async () => {
    const page = await open('/topics/orders', { h: 600 });
    const set = y => page.evaluate(y => { document.querySelector('.tbl-wrap').scrollTop = y; }, y);
    await set(60);
    await page.waitForTimeout(3200); // an unchanged poll goes by.
    await set(160);
    const at = await rowOffset(page, 12);
    await page.evaluate(at => document.getElementById('msg-' + at).click(), at);
    await page.waitForSelector(`#drawer-title[data-offset="${at}"]`);
    await page.waitForTimeout(200);
    const y = await page.evaluate(() => document.querySelector('.tbl-wrap').scrollTop);
    report('U12', y === 160, `scrolled to 160, opened a record, now at ${y}`);
    await page.context().close();
  });

  await check('U13', async () => {
    const page = await open('/topics/orders');
    await page.route('**/message?*', async r => { await new Promise(x => setTimeout(x, 1200)); r.continue().catch(() => {}); });
    await page.click('#msg-' + await rowOffset(page, 5));
    await page.waitForTimeout(150);
    await page.keyboard.press('Escape');
    await page.waitForTimeout(2000);
    const st = await page.evaluate(() => ({ search: location.search, hidden: document.getElementById('drawer-overlay').hidden, html: document.getElementById('drawer').innerHTML.length }));
    report('U13', st.hidden && !/open=/.test(st.search) && st.html === 0, JSON.stringify(st));
    await page.context().close();
  });

  await check('U14', async () => {
    const page = await open('/topics/orders');
    const top = await page.evaluate(() => document.querySelector('tr[data-drawer-open] .cell-key[title^="unbroken"]').parentElement.id);
    await page.click('#' + top);
    await page.waitForSelector('#drawer-title[data-offset]');
    const st = await page.evaluate(() => { const d = document.getElementById('drawer'); return { sw: d.scrollWidth, cw: d.clientWidth, right: Math.round(Math.max(...Array.from(d.querySelectorAll('*')).map(e => e.getBoundingClientRect().right))), edge: Math.round(d.getBoundingClientRect().right) }; });
    report('U14', st.sw <= st.cw + 1, JSON.stringify(st));
    await page.context().close();
  });

  await check('U15', async () => {
    const bad = [];
    const long = await (await open('/topics')).evaluate(() => Array.from(document.querySelectorAll('.li')).map(l => l.dataset.name).sort((a, b) => b.length - a.length)[0]);
    for (const w of [420, 360]) {
      for (const [path, dock] of [['/topics', 0], ['/topics/orders', 0], ['/topics/orders', 1], ['/topics/' + long, 0], ['/groups/analytics', 0], ['/groups/' + encodeURIComponent(GROUP), 0], ['/cluster', 0], ['/new/topic', 0], ['/topics/orders?open=5', 0]]) {
        const page = await open(path, { w, h: 760, dock: !!dock });
        await page.waitForTimeout(250);
        const over = await page.evaluate(() => {
          const vw = window.innerWidth;
          const doc = document.documentElement.scrollWidth - vw;
          // Anything sticking out past the right edge that is not inside a scroller of its own.
          const out = Array.from(document.querySelectorAll('body *')).filter(e => {
            if (e.offsetParent === null) return false;
            const r = e.getBoundingClientRect();
            if (r.width === 0 || r.right <= vw + 1) return false;
            for (let p = e.parentElement; p; p = p.parentElement) { const o = getComputedStyle(p).overflowX; if (o === 'auto' || o === 'scroll' || o === 'hidden') return false; }
            return true;
          }).slice(0, 3).map(e => e.className || e.tagName);
          return { doc, out };
        });
        if (over.doc > 0 || over.out.length) bad.push(`${w}px ${path}${dock ? ' (dock)' : ''}: ${JSON.stringify(over)}`);
        await page.context().close();
      }
    }
    report('U15', bad.length === 0, bad.length ? bad.join(' | ') : 'no page overflows at 420 or 360px');
  });

  await check('U16', async () => {
    const page = await open('/topics/orders', { h: 600 });
    await page.fill('#jump-to', '3');
    await page.click('.jump .jump-go');
    await page.waitForSelector('tr[data-hl]');
    await page.waitForTimeout(400);
    const st = await page.evaluate(() => { const r = document.querySelector('tr[data-hl]').getBoundingClientRect(), w = document.querySelector('.tbl-wrap').getBoundingClientRect(); return { id: document.querySelector('tr[data-hl]').id, inView: r.top >= w.top && r.bottom <= w.bottom }; });
    report('U16', st.inView, JSON.stringify(st));
    await page.context().close();
  });

  await check('U17', async () => {
    const page = await open('/topics/orders');
    await page.route('**/message?*', async r => { await new Promise(x => setTimeout(x, 1000)); r.fulfill({ status: 502, body: 'The console could not get an answer from the broker.' }); });
    const at = await rowOffset(page, 5);
    await page.click('#msg-' + at);
    await page.waitForTimeout(300);
    const loading = await page.evaluate(() => ({ title: !!document.querySelector('#drawer #drawer-title'), close: !!document.querySelector('#drawer [data-drawer-close]'), focusIn: document.getElementById('drawer').contains(document.activeElement) }));
    await page.waitForTimeout(1200);
    const err = await page.evaluate(() => ({ title: !!document.querySelector('#drawer #drawer-title'), alert: (document.querySelector('#drawer [role=alert]') || {}).textContent, focusIn: document.getElementById('drawer').contains(document.activeElement) }));
    await page.keyboard.press('Escape');
    const closed = await page.evaluate(at => document.getElementById('drawer-overlay').hidden && document.activeElement.id === 'msg-' + at, at);
    const ok = loading.title && loading.close && loading.focusIn && err.title && /could not get an answer/.test(err.alert || '') && err.focusIn && closed;
    report('U17', ok, JSON.stringify({ loading, err, closed }));
    await page.context().close();
  });

  await check('U18', async () => {
    const page = await open('/topics/orders');
    await page.focus('.ptab >> nth=1');
    await page.evaluate(() => document.getElementById('tape-and-table').setAttribute('data-mark', '1'));
    await produce(page, 'orders', 0, 'u18');
    await page.waitForFunction(() => !document.getElementById('tape-and-table').hasAttribute('data-mark'), null, { timeout: 8000 });
    await page.waitForTimeout(300);
    const st = await page.evaluate(() => ({ cls: document.activeElement.className, text: document.activeElement.textContent.trim().slice(0, 2) }));
    report('U18', /ptab/.test(st.cls) && st.text === 'P1', JSON.stringify(st));
    await page.context().close();
  });

  await check('U19', async () => {
    const before = errors.length;
    const page = await open('/topics/orders', { init: () => { Object.defineProperty(Navigator.prototype, 'clipboard', { get: () => undefined }); Object.defineProperty(window, 'isSecureContext', { get: () => false }); } });
    await page.click('.copychip .copy-btn');
    await page.waitForSelector('.toast');
    const text = await page.innerText('.toast');
    report('U19', errors.length === before, `no exception; the toast says "${text.replace(/\s+/g, ' ').trim()}"`);
    await page.context().close();
  });

  await check('U21', async () => {
    const bad = [];
    for (const scheme of ['light', 'dark']) {
      for (const [path, dock] of [['/topics/orders', 1], ['/topics/orders?open=5', 0], ['/topics/orders?q=ord&all=1', 0], ['/groups/analytics', 0], ['/groups/' + encodeURIComponent(GROUP), 0], ['/cluster', 0], ['/new/topic', 0]]) {
        const page = await open(path, { scheme, dock: !!dock });
        await page.waitForTimeout(300);
        const r = await page.evaluate(() => {
          const named = el => {
            if (el.getAttribute('aria-label') || el.getAttribute('aria-labelledby') || el.title) return true;
            if (el.id && document.querySelector('label[for="' + el.id + '"]')) return true;
            if (el.closest('label')) return true;
            return (el.textContent || '').trim().length > 0;
          };
          const unnamed = Array.from(document.querySelectorAll('button, a[href], input:not([type=hidden]), select, textarea'))
            .filter(el => el.offsetParent !== null && !el.closest('.CodeMirror') && !named(el)).map(el => el.outerHTML.slice(0, 70));
          const parse = c => { const m = c.match(/-?[\d.]+(e-?\d+)?/g).map(Number); const v = /^color\(/.test(c) ? [m[0] * 255, m[1] * 255, m[2] * 255, m[3]] : m; return v[3] === undefined ? [v[0], v[1], v[2], 1] : v; };
          const lum = ([r, g, b]) => { const f = v => { v /= 255; return v <= .03928 ? v / 12.92 : Math.pow((v + .055) / 1.055, 2.4); }; return .2126 * f(r) + .7152 * f(g) + .0722 * f(b); };
          const over = (top, under) => { const a = top[3]; return [0, 1, 2].map(i => top[i] * a + under[i] * (1 - a)).concat(1); };
          const bgOf = el => { const stack = []; for (let p = el; p; p = p.parentElement) { const c = parse(getComputedStyle(p).backgroundColor); if (c[3] > 0) stack.push(c); if (c[3] === 1) break; } let bg = [255, 255, 255, 1]; for (const c of stack.reverse()) bg = over(c, bg); return bg; };
          const low = [];
          for (const el of document.querySelectorAll('.badge, .chip, th, .dim, .sz, .note, .lagnum, .cell-key, .jt-k, .jt-n, .jt-p, .cell-time, .ri, .li .meta, .btn, .kbd, .honest, .tape-foot, .tbl-foot, .ptab, .counter .lbl, .hint, .meta-row .k, .sec-title')) {
            if (el.offsetParent === null || !(el.textContent || '').trim()) continue;
            if (parseFloat(getComputedStyle(el).opacity) < 1 || el.closest('[disabled]')) continue;
            const bg = bgOf(el), fg = over(parse(getComputedStyle(el).color), bg);
            const a = lum(fg), b = lum(bg), ratio = (Math.max(a, b) + .05) / (Math.min(a, b) + .05);
            if (ratio < 4.5) low.push((el.className || el.tagName) + ' ' + ratio.toFixed(2));
          }
          return { unnamed, low: Array.from(new Set(low)), main: document.querySelectorAll('main').length, nav: !!document.querySelector('nav[aria-label]'), header: !!document.querySelector('header'),
            acks: Array.from(document.querySelectorAll('.w-acks .seg button')).map(b => b.getAttribute('aria-pressed')).join(',') };
        });
        if (r.unnamed.length || r.low.length || r.main !== 1 || !r.nav || !r.header) bad.push(`${scheme} ${path}: ${JSON.stringify(r)}`);
        if (dock && r.acks !== 'true,false,false') bad.push(`${scheme} ${path}: acks aria-pressed = ${r.acks}`);
        await page.context().close();
      }
    }
    report('U21', bad.length === 0, bad.length ? bad.join(' | ') : 'every control is named, text holds 4.5:1 in both themes, one <main>, a labelled <nav>');
  });

  await check('U23', async () => {
    const page = await open('/topics/orders', { dock: true });
    await page.selectOption('#f-partition', '2');
    await page.click('.dock .CodeMirror');
    await page.keyboard.type('to another partition');
    await page.click('.dock button[type=submit]');
    await page.waitForSelector('.toast');
    const st = await page.evaluate(() => ({ tab: document.querySelector('.ptab.on').textContent.trim().slice(0, 2), title: document.querySelector('.secrow .sec-title').textContent.trim(), url: location.search, toast: document.querySelector('.toast').innerText.replace(/\s+/g, ' ') }));
    report('U23', st.tab === 'P0' && /partition 0/.test(st.title) && /partition 2/.test(st.toast), JSON.stringify(st));
    await page.context().close();
  });

  await check('theme', async () => {
    const page = await open('/topics/orders', { scheme: 'dark' });
    const sys = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    await page.click('#theme-seg [data-mode="light"]');
    const light = await page.evaluate(() => [document.documentElement.getAttribute('data-theme'), getComputedStyle(document.body).backgroundColor].join(' '));
    await page.reload({ waitUntil: 'networkidle' });
    const kept = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    await page.click('#theme-seg [data-mode="system"]');
    await page.emulateMedia({ colorScheme: 'light' });
    await page.waitForTimeout(200);
    const follows = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
    await page.evaluate(() => document.fonts.ready);
    const fonts = await page.evaluate(() => [document.fonts.check('14px "IBM Plex Sans Variable"'), document.fonts.check('500 13px "IBM Plex Mono"')].join(','));
    report('theme', sys === 'dark' && light === 'light rgb(255, 255, 255)' && kept === 'light' && follows === 'light' && fonts === 'true,true', JSON.stringify({ sys, light, kept, follows, fonts }));
    await page.context().close();
  });

  // Moving between topics: nothing on the page moves after it is shown, and the
  // list is not redrawn (it keeps its scroll position and its elements).
  await check('switch', async () => {
    const page = await open('/topics/orders', { h: 420 });
    await page.evaluate(() => {
      window.__shifts = [];
      new PerformanceObserver(l => { for (const e of l.getEntries()) if (!e.hadRecentInput) window.__shifts.push(+e.value.toFixed(4)); }).observe({ type: 'layout-shift' });
      document.querySelector('.listpane').setAttribute('data-same', '1');
      document.querySelector('.lp-scroll').scrollTop = 40;
    });
    const seen = [];
    for (const name of ['payments.events.v1', 'orders', 'binary-blobs', 'empty-topic', 'orders']) {
      await page.evaluate(n => document.querySelector(`.listpane .li[data-name="${n}"]`).click(), name);
      await page.waitForFunction(n => location.pathname.endsWith('/' + n) && document.querySelector('.idband h1').textContent === n, name);
      await page.waitForTimeout(500);
      seen.push(await page.evaluate(() => document.querySelector('.listpane .li.on').dataset.name));
    }
    const st = await page.evaluate(() => ({ shifts: window.__shifts, sameList: document.querySelector('.listpane').hasAttribute('data-same'), listScroll: document.querySelector('.lp-scroll').scrollTop, late: document.querySelectorAll('[hx-trigger="load"]').length }));
    report('switch', st.shifts.length === 0 && st.sameList && st.listScroll === 40 && st.late === 0 && seen.join() === 'payments.events.v1,orders,binary-blobs,empty-topic,orders', JSON.stringify({ ...st, highlighted: seen.join() }));
    await page.context().close();
  });

  await check('offsets', async () => {
    const page = await open('/groups/' + encodeURIComponent(GROUP));
    const row = '#group-live tbody tr >> nth=0';
    await page.click(`${row} >> text=Set…`);
    await page.fill(`${row} >> input[name=offset]`, '1');
    await produce(page, 'payments.events.v1', 0, 'offsets'); // the lag changes: a refresh is due.
    await page.waitForTimeout(3500);
    const kept = await page.inputValue(`${row} >> input[name=offset]`).catch(() => 'GONE');
    await page.click(`${row} >> button[type=submit]`);
    await page.waitForSelector('.toast');
    const toast = (await page.innerText('.toast')).replace(/\s+/g, ' ');
    const committed = await page.innerText(`${row} >> td >> nth=3`);
    await page.click(`${row} >> text=Set…`);
    await page.fill(`${row} >> input[name=offset]`, '99999');
    await page.click(`${row} >> button[type=submit]`);
    await page.waitForSelector(`${row} >> [data-err-slot]:not([hidden])`);
    const err = await page.innerText(`${row} >> [data-err-slot]`);
    report('offsets', kept === '1' && /now at offset 1/.test(toast) && committed.trim() === '1' && /outside partition/.test(err), JSON.stringify({ keptWhileRefreshing: kept, toast, committed: committed.trim(), err: err.slice(0, 60) }));
    await page.context().close();
  });

  await check('search', async () => {
    const page = await open('/topics/orders');
    await page.fill('#find-q', 'ord_89');
    await page.evaluate(() => document.getElementById('tape-and-table').setAttribute('data-mark', '1'));
    await produce(page, 'orders', 0, 'search');
    await page.waitForFunction(() => !document.getElementById('tape-and-table').hasAttribute('data-mark'), null, { timeout: 8000 });
    await page.waitForTimeout(300);
    const typed = await page.inputValue('#find-q');
    await page.check('#find-all');
    await page.click('.find button[type=submit]');
    await page.waitForSelector('tr[id^="hit-"]');
    const st = await page.evaluate(() => ({ rows: document.querySelectorAll('tr[id^="hit-"]').length, parts: new Set(Array.from(document.querySelectorAll('tr[id^="hit-"]')).map(r => r.id.split('-')[1])).size, note: document.querySelector('.jumpnote').innerText.slice(0, 50), live: document.getElementById('tape-and-table').hasAttribute('data-live'), url: location.search }));
    await page.click('tr[id^="hit-"] >> nth=0');
    await page.waitForSelector('#drawer-title[data-offset]');
    const opened = await page.evaluate(() => ({ title: document.getElementById('drawer-title').textContent, url: location.search }));
    report('search', typed === 'ord_89' && st.rows > 3 && st.parts === 3 && !st.live && /q=ord_89/.test(st.url) && !/open=/.test(opened.url), JSON.stringify({ typedSurvivedRefresh: typed, ...st, opened }));
    await page.context().close();
  });

  report('no-exceptions', errors.length === 0, errors.slice(0, 5).join(' | '));
  console.log(results.join('\n'));
  await browser.close();
  process.exit(failed ? 1 : 0);
})();
