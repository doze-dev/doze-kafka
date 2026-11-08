// site-media: the console's still and video on doze-dev.com, made from a real
// broker playing Larder, the example project (doze-examples/larder).
//
//   task site-media                      writes into ../doze-website/public/media
//   bun run site-media.ts [out-dir]      the same, from e2e/
//
// It builds doze-kafka and e2e/larder, boots the broker in a fresh directory
// named larder (so the console's top bar says larder, as it does under
// `doze up` in that project) with auto-create off, creates Larder's four
// topics, starts its four consumer groups (larder services), and produces
// sign-ups and orders in Larder's JSON from franz-go clients named identity
// and orders, as Larder's own Go services publish them. Then:
//
//   kafka-orders.webp   orders.placed opened: its tape, the messages, the newest
//                       order decoded, and the requests underneath. 1440x900
//                       at 2x, dark, WebP 85.
//   kafka-live.webm     orders placed live: Traffic fills with the services'
//                       requests, then orders.placed takes them on its tape.
//                       1440x900 at 2x, VP9 at 30 fps (the screencast technique of
//                       doze/test/desk-e2e/record.ts).
//
// Nothing is made up but the shoppers: every record on screen went through the
// broker, and every payment was answered by a consumer reading orders.placed.
import { chromium, type Page } from "@playwright/test";
import sharp from "sharp";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import net from "node:net";

const repo = path.resolve(import.meta.dir, "..");
const out = path.resolve(process.argv[2] ?? path.join(repo, "..", "doze-website", "public", "media"));
const W = 1440, H = 900, SCALE = 2, FPS = 30;
fs.mkdirSync(out, { recursive: true });

// ── the build ────────────────────────────────────────────────────────────────
const bin = path.join(repo, "e2e", ".tmp", "bin");
fs.mkdirSync(bin, { recursive: true });
run(["go", "build", "-o", path.join(bin, "doze-kafka-site"), "./cmd/doze-kafka"]);
run(["go", "build", "-o", path.join(bin, "larder"), "./e2e/larder"]);

// ── the broker, in a directory called larder ─────────────────────────────────
const home = fs.mkdtempSync(path.join(os.tmpdir(), "doze-site-media-"));
const dir = path.join(home, "larder");
fs.mkdirSync(dir);
const [kport, cport, rport] = [await freePort(), await freePort(), await freePort()];
const addr = `127.0.0.1:${kport}`;
const base = `http://127.0.0.1:${cport}/_console/`;
const procs: Bun.Subprocess[] = [];
const broker = spawn([path.join(bin, "doze-kafka-site"), "--listen", addr, "--console-addr", `127.0.0.1:${cport}`,
  "--schema-registry-addr", `127.0.0.1:${rport}`, "--data-dir", "data", "--auto-create-topics=false"], { cwd: dir, env: { ...process.env, DOZE_ZONE: "off", TZ: "UTC" } });
procs.push(broker);

// main runs at the end of the file, once the tables below are defined.
async function main() {
let failed = false;
try {
  await until(async () => (await fetch(`http://127.0.0.1:${cport}/healthz`).catch(() => null))?.ok === true, 20000, "the broker did not start");
  run([path.join(bin, "larder"), "topics", addr]);
  const services = spawn([path.join(bin, "larder"), "services", addr], {});
  procs.push(services);

  // The morning so far: people sign up and shop, some come back.
  const rnd = mulberry32(20261108);
  const users = SHOPPERS.map(([name, email]) => ({ user_id: crypto.randomUUID(), email, name }));
  const identity = producer("users.signed-up", "identity");
  const orders = producer("orders.placed", "orders");
  let shoppers = 0;
  for (let i = 0; i < 50; i++) {
    if (shoppers < users.length && (i < 4 || rnd() < 0.45)) {
      const u = users[shoppers++];
      identity.send(u.user_id, { ...u, at: new Date().toISOString() });
    } else {
      orders.send(...order(users[Math.floor(rnd() * shoppers)], rnd, i % 11 === 7));
    }
    await Bun.sleep(250 + rnd() * 400);
  }
  await identity.close();
  await orders.close();
  await Bun.sleep(4000); // payments answers, everyone commits.
  if (process.env.SITE_MEDIA_HOLD) {
    // To look around before shooting: the broker stays up until interrupted.
    console.log(`holding: ${base} (kafka ${addr})`);
    await new Promise((ok) => process.on("SIGINT", ok));
    return;
  }

  const browser = await chromium.launch();
  try {
    await still(browser);
    await video(browser, users.slice(0, shoppers), rnd);
  } finally {
    await browser.close();
  }
} catch (e) {
  console.error("site-media:", (e as Error).message);
  failed = true;
} finally {
  // The consumers first, while the broker can still hear them leave; then the broker.
  for (const p of procs.reverse()) {
    p.kill();
    if (await Promise.race([p.exited.then(() => true), Bun.sleep(5000).then(() => false)]) === false) p.kill(9);
    await p.exited;
  }
  fs.rmSync(home, { recursive: true, force: true });
}
process.exit(failed ? 1 : 0);
}

// ── the still ────────────────────────────────────────────────────────────────
async function still(browser: import("@playwright/test").Browser) {
  const page = await browser.newPage({ viewport: { width: W, height: H }, deviceScaleFactor: SCALE, colorScheme: "dark", timezoneId: "UTC" });
  await page.goto(base + "topics/orders.placed");
  await settle(page);
  const header = await page.locator(".brand").innerText();
  if (!/larder/.test(header)) throw new Error(`the top bar says ${JSON.stringify(header)}, not larder`);
  // The newest order, opened: its JSON decoded in the inspector beside the tape.
  await page.locator("tr[data-inspector-open]").first().click();
  await page.waitForTimeout(1200);
  const png = await page.screenshot();
  const file = path.join(out, "kafka-orders.webp");
  await sharp(png).webp({ quality: 85 }).toFile(file);
  console.log(file, fs.statSync(file).size);
  await page.close();
}

// ── the video ────────────────────────────────────────────────────────────────
async function video(browser: import("@playwright/test").Browser, users: { user_id: string; email: string; name: string }[], rnd: () => number) {
  // The screencast is at CSS pixels whatever the device scale, so the page is
  // drawn at twice the size and zoomed by two: a 1440x900 layout, sharp.
  const page = await browser.newPage({ viewport: { width: W * SCALE, height: H * SCALE }, colorScheme: "dark", timezoneId: "UTC" });
  // The shell is 100vh tall, which a zoomed page gets wrong once htmx swaps a
  // view in (the workspace doubles and the page scrolls): pin it to the window.
  await page.addInitScript(([z, h]) => addEventListener("DOMContentLoaded", () => {
    document.documentElement.style.zoom = String(z);
    const s = document.createElement("style");
    s.textContent = `.app { height: ${h}px !important }`;
    document.head.appendChild(s);
  }), [SCALE, H]);
  await page.addInitScript(pointer);
  await page.goto(base + "traffic");
  await settle(page);
  await page.mouse.move(W * SCALE * 0.62, H * SCALE * 0.55);
  at = { x: W * SCALE * 0.62, y: H * SCALE * 0.55 };
  await page.waitForTimeout(800);

  const file = path.join(out, "kafka-live.webm");
  await record(page, file, async () => {
    const orders = producer("orders.placed", "orders");
    const shop = async (n: number) => {
      for (let i = 0; i < n; i++) {
        orders.send(...order(users[Math.floor(rnd() * users.length)], rnd, false));
        await page.waitForTimeout(650 + rnd() * 350);
      }
    };
    await page.waitForTimeout(900);
    await shop(4);
    await clickAt(page, `#rail a[href$="/topics"]`);
    await page.waitForTimeout(600);
    await clickAt(page, `a[href*="topics/orders.placed"]`);
    await page.waitForTimeout(600);
    await shop(8);
    await page.waitForTimeout(2000);
    await orders.close();
  });
  console.log(file, fs.statSync(file).size);
  await page.close();
}

// ── Larder's events ──────────────────────────────────────────────────────────
// order is events.OrderPlaced as services/orders publishes it, keyed by the
// order ID. A card ending 0002 is the one payments declines.
function order(u: { user_id: string; email: string }, rnd: () => number, declined: boolean): [string, unknown] {
  const n = 2 + Math.floor(rnd() * 4);
  const picked = new Set<number>();
  while (picked.size < n) picked.add(Math.floor(rnd() * PRODUCTS.length));
  const items = [...picked].map((i) => {
    const [sku, name, price_pence] = PRODUCTS[i];
    return { sku, name, price_pence, qty: 1 + Math.floor(rnd() * (rnd() < 0.7 ? 1 : 3)) };
  });
  const total_pence = items.reduce((s, it) => s + it.price_pence * it.qty, 0);
  const card_last4 = declined ? "0002" : CARDS[Math.floor(rnd() * CARDS.length)];
  const order_id = crypto.randomUUID();
  return [order_id, { order_id, user_id: u.user_id, email: u.email, total_pence, card_last4, items, at: new Date().toISOString() }];
}

// From larder/services/catalog/seed/001_products.sql: sku, name, price in pence.
const PRODUCTS: [string, string, number][] = [
  ["FV-BAN-6", "Fairtrade bananas", 95], ["FV-APL-4", "Braeburn apples", 160], ["FV-AVO-2", "Ripe & ready avocados", 199],
  ["FV-TOM-V", "Vine cherry tomatoes", 185], ["FV-SPN-B", "Baby spinach", 150], ["FV-POT-2K", "Maris Piper potatoes", 175],
  ["FV-LEM-4", "Unwaxed lemons", 130], ["BK-SOU-1", "Sourdough loaf", 325], ["BK-CRO-4", "All-butter croissants", 250],
  ["BK-CIN-2", "Cinnamon buns", 275], ["DY-MLK-2", "Semi-skimmed milk", 125], ["DY-EGG-6", "Free-range eggs", 210],
  ["DY-BUT-1", "Salted butter", 225], ["DY-CHD-1", "Mature cheddar", 350], ["DY-YOG-1", "Greek-style yoghurt", 165],
  ["DY-OAT-1", "Oat drink barista", 200], ["PN-PAS-1", "Spaghetti", 110], ["PN-TOM-4", "Chopped tomatoes", 240],
  ["PN-RIC-1", "Basmati rice", 265], ["PN-OIL-1", "Extra virgin olive oil", 575], ["PN-OAT-1", "Porridge oats", 150],
  ["PN-HNY-1", "Wildflower honey", 399], ["DR-COF-1", "Ground coffee", 450], ["DR-TEA-80", "English breakfast tea", 325],
  ["DR-OJ-1", "Orange juice with bits", 230], ["DR-KOM-1", "Ginger kombucha", 199],
];
// Test cards' last four digits (the shop takes any Luhn-valid number).
const CARDS = ["4242", "4242", "4444", "1881", "0005"];
const SHOPPERS: [string, string][] = [
  ["Priya Shah", "priya.shah@example.com"], ["Tom Okafor", "tom.okafor@example.com"], ["Eilidh Fraser", "eilidh@example.com"],
  ["Marek Nowak", "marek.nowak@example.com"], ["Sofia Rossi", "sofia.r@example.com"], ["Daniel Mensah", "dan.mensah@example.com"],
  ["Hannah Lewis", "hannah.lewis@example.com"], ["Yusuf Demir", "yusuf.demir@example.com"], ["Grace Liu", "grace.liu@example.com"],
  ["Oliver Byrne", "ollie.byrne@example.com"], ["Amara Nwosu", "amara.nwosu@example.com"], ["Jonas Berg", "jonas.berg@example.com"],
];

// producer is `larder produce`: a franz-go client with the service's name,
// publishing each key and JSON as it is written, as platform.Publish does. (kcat
// was the first choice: it reads a pipe in 4 KiB gulps, so orders reached the
// broker seven at a time.)
function producer(topic: string, client: string) {
  const p = Bun.spawn([path.join(bin, "larder"), "produce", addr, topic, client],
    { stdin: "pipe", stdout: "inherit", stderr: "inherit" });
  return {
    send(key: string, v: unknown) {
      p.stdin.write(`${key}\t${JSON.stringify(v)}\n`);
      p.stdin.flush();
    },
    async close() {
      p.stdin.end();
      if ((await p.exited) !== 0) throw new Error(`larder produce for ${topic} failed`);
    },
  };
}

// ── helpers ──────────────────────────────────────────────────────────────────
function run(cmd: string[]) {
  const r = Bun.spawnSync(cmd, { cwd: repo, env: { ...process.env, GOWORK: "off" }, stdout: "inherit", stderr: "inherit" });
  if (r.exitCode !== 0) throw new Error(`${cmd.join(" ")} failed`);
}

function spawn(cmd: string[], o: { cwd?: string; env?: Record<string, string | undefined> }) {
  return Bun.spawn(cmd, { cwd: o.cwd, env: o.env, stdout: "ignore", stderr: process.env.SITE_MEDIA_LOG ? "inherit" : "ignore" });
}

async function freePort(): Promise<number> {
  return new Promise((ok) => {
    const s = net.createServer().listen(0, "127.0.0.1", () => {
      const p = (s.address() as net.AddressInfo).port;
      s.close(() => ok(p));
    });
  });
}

async function until(f: () => Promise<boolean>, ms: number, why: string) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await f()) return;
    await Bun.sleep(200);
  }
  throw new Error(why);
}

async function settle(page: Page) {
  await page.waitForLoadState("networkidle").catch(() => {});
  await page.evaluate(() => document.fonts.ready);
  await page.waitForTimeout(1200);
}

function mulberry32(a: number) {
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ── the recorder (after doze/test/desk-e2e/record.ts) ────────────────────────
let at = { x: 0, y: 0 };

async function clickAt(page: Page, sel: string) {
  const box = await page.locator(sel).first().boundingBox();
  if (!box) throw new Error(`nothing at ${sel}`);
  const x = box.x + box.width / 2, y = box.y + box.height / 2;
  const from = { ...at };
  for (let i = 1; i <= 24; i++) {
    const t = i / 24, e = t < 0.5 ? 2 * t * t : 1 - (-2 * t + 2) ** 2 / 2;
    await page.mouse.move(from.x + (x - from.x) * e, from.y + (y - from.y) * e);
    await page.waitForTimeout(16);
  }
  at = { x, y };
  await page.waitForTimeout(250);
  await page.mouse.click(x, y);
}

// record screencasts the page while act runs, then encodes at a steady rate: a
// frame comes only when the page changes, so each is held until the next.
async function record(page: Page, file: string, act: () => Promise<void>) {
  const cdp = await page.context().newCDPSession(page);
  const frames: { t: number; jpg: Buffer }[] = [];
  cdp.on("Page.screencastFrame", async (f: { data: string; sessionId: number; metadata: { timestamp: number } }) => {
    frames.push({ t: f.metadata.timestamp, jpg: Buffer.from(f.data, "base64") });
    await cdp.send("Page.screencastFrameAck", { sessionId: f.sessionId }).catch(() => {});
  });
  const vp = page.viewportSize()!;
  await cdp.send("Page.startScreencast", { format: "jpeg", quality: 92, maxWidth: vp.width, maxHeight: vp.height, everyNthFrame: 1 });
  const start = Date.now() / 1000;
  await page.waitForTimeout(300);
  await act();
  const end = Date.now() / 1000;
  await cdp.send("Page.stopScreencast");
  if (!frames.length) throw new Error("no frames");
  // Playwright's ffmpeg reads no pipes: the frames go through a file.
  const tmp = `${file}.mjpeg`;
  const fd = fs.openSync(tmp, "w");
  let i = 0;
  for (let t = start; t < end; t += 1 / FPS) {
    while (i + 1 < frames.length && frames[i + 1].t <= t) i++;
    fs.writeSync(fd, frames[i].jpg);
  }
  fs.closeSync(fd);
  const enc = Bun.spawn([findFFmpeg(), "-loglevel", "error", "-y", "-f", "image2pipe", "-framerate", String(FPS), "-c:v", "mjpeg", "-i", tmp,
    "-c:v", "libvpx-vp9", "-pix_fmt", "yuv420p", "-crf", "34", "-b:v", "0", "-deadline", "good", "-cpu-used", "2", "-row-mt", "1", "-an", file],
  { stderr: "inherit" });
  const code = await enc.exited;
  fs.rmSync(tmp, { force: true });
  if (code !== 0) throw new Error("ffmpeg failed");
}

// pointer draws a macOS-like arrow where the mouse is.
function pointer() {
  addEventListener("DOMContentLoaded", () => {
    const p = document.createElement("div");
    p.innerHTML = '<svg width="22" height="28" viewBox="0 0 22 28"><path d="M2 2 L2 22 L7.5 17 L11.5 26 L15 24.5 L11 15.5 L18.5 15.5 Z" fill="#111" stroke="#fff" stroke-width="1.6" stroke-linejoin="round"/></svg>';
    Object.assign(p.style, { position: "fixed", left: "0", top: "0", zIndex: "2147483647", pointerEvents: "none", transform: "translate(-2px,-2px)" });
    document.body.appendChild(p);
    addEventListener("mousemove", (e) => { const z = Number(document.documentElement.style.zoom || 1); p.style.left = e.clientX / z + "px"; p.style.top = e.clientY / z + "px"; }, true);
  });
}

// findFFmpeg is Playwright's own ffmpeg (it has VP9), or one on the PATH.
function findFFmpeg() {
  const cache = path.join(os.homedir(), os.platform() === "darwin" ? "Library/Caches/ms-playwright" : ".cache/ms-playwright");
  const dirs = fs.existsSync(cache) ? fs.readdirSync(cache).filter((d) => d.startsWith("ffmpeg-")).sort().reverse() : [];
  for (const d of dirs) for (const f of ["ffmpeg-mac", "ffmpeg-linux", "ffmpeg"]) {
    const p = path.join(cache, d, f);
    if (fs.existsSync(p)) return p;
  }
  return "ffmpeg";
}

await main();
