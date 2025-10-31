# If you see this, previous cell reset the state. Rewriting the file now.
import os, sys, asyncio, logging, sqlite3, csv, json, re, time, random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import aiohttp, feedparser
from aiohttp import web  # optional tiny HTTP server for Render Web Service
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError, TelegramBadRequest

print("MAIN_VERSION=2025-10-31-v3.1", flush=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# === ENV ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в Environment.")
if not CHANNEL_ID:
    raise RuntimeError("CHANNEL_ID не задан в Environment.")
try:
    CHANNEL_ID = int(CHANNEL_ID)
except Exception:
    raise RuntimeError("CHANNEL_ID должен быть числом вида -100XXXXXXXXXX")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
ENABLE_TRANSLATE = os.getenv("ENABLE_TRANSLATE", "1") == "1"
ENABLE_COMMENT = os.getenv("ENABLE_COMMENT", "1") == "1"   # legacy switch for short comment
ENABLE_SUMMARY = os.getenv("ENABLE_SUMMARY", "1") == "1"   # NEW: concise summary of the core news
SUMMARY_MAX_CHARS = int(os.getenv("SUMMARY_MAX_CHARS", "220"))

# Anti-flood controls
MIN_SECONDS_BETWEEN_POSTS = float(os.getenv("MIN_SECONDS_BETWEEN_POSTS", "1.8"))
MAX_POSTS_PER_CYCLE = int(os.getenv("MAX_POSTS_PER_CYCLE", "6"))
RETRY_AFTER_GRACE = int(os.getenv("RETRY_AFTER_GRACE", "2"))
SLOWDOWN_AFTER_BURST = int(os.getenv("SLOWDOWN_AFTER_BURST", "10"))

# Timings & feeds
DB_PATH = "data.db"
FEEDS_FILE = "feeds/sources.csv"
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "600"))  # 10 минут

# Digest settings
ENABLE_DIGEST = os.getenv("ENABLE_DIGEST", "1") == "1"
DIGEST_TZ = os.getenv("DIGEST_TZ", "Europe/Moscow")
DIGEST_TIMES = os.getenv("DIGEST_TIMES", "08:00,20:00")
DIGEST_LOOKBACK_HOURS = int(os.getenv("DIGEST_LOOKBACK_HOURS", "12"))
DIGEST_TOP_N = int(os.getenv("DIGEST_TOP_N", "5"))

# Optional tiny web server (for Render Web Service)
BIND_WEB = os.getenv("BIND_WEB", "0") == "1"
PORT = int(os.getenv("PORT", "10000"))

dp = Dispatcher()

@dp.message(CommandStart())
async def start(m: Message):
    await m.answer("🟢 Axed News v3.1: умная ссылкой в заголовке + лаконичный пересказ сути.")

# === DB ===
def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS posted (
        guid TEXT PRIMARY KEY,
        ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS items (
        guid TEXT PRIMARY KEY,
        ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        title TEXT,
        summary TEXT,
        link TEXT,
        priority REAL DEFAULT 0.0,
        urgent INTEGER DEFAULT 0
    )
    """)
    con.commit(); con.close()

def was_posted(guid: str) -> bool:
    if not guid: return False
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("SELECT 1 FROM posted WHERE guid=?", (guid,))
    row = cur.fetchone(); con.close()
    return row is not None

def mark_posted_and_store(item: dict, priority: float, urgent: bool):
    guid = item.get("guid") or item.get("link")
    if not guid: 
        return
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO posted (guid) VALUES (?)", (guid,))
    cur.execute("""INSERT OR REPLACE INTO items (guid, ts, title, summary, link, priority, urgent)
                   VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)""",
                (guid, item.get("title") or "", item.get("summary") or "", item.get("link") or "",
                 float(priority), 1 if urgent else 0))
    con.commit(); con.close()

def read_recent_items(hours: int, top_n: int):
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("""SELECT title, summary, link, priority, urgent, ts
                   FROM items
                   WHERE ts >= datetime('now', ?)
                   ORDER BY urgent DESC, priority DESC, ts DESC
                   LIMIT ?""", (f'-{hours} hours', top_n))
    rows = cur.fetchall(); con.close()
    return rows

# === FEEDS ===
def load_feeds() -> list[str]:
    defaults = [
        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        "https://www.reuters.com/finance/rss",
        "https://www.economist.com/finance-and-economics/rss.xml",
        "https://www.rbc.ru/economics/?rss",
        "https://www.moex.com/export/news.aspx?cat=stocks",
        "https://cbr.ru/press/pr/?rss=1",
    ]
    if not os.path.exists(FEEDS_FILE):
        logging.warning("feeds/sources.csv не найден — использую дефолтный пул источников.")
        return defaults
    feeds = []
    try:
        with open(FEEDS_FILE, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                url = (row.get("url") or "").strip()
                if url: feeds.append(url)
    except Exception as e:
        logging.warning(f"Ошибка чтения {FEEDS_FILE}: {e} — использую дефолтный пул.")
        return defaults
    return feeds or defaults

# === Helpers ===
def detect_lang(text: str) -> str:
    cyr = len(re.findall(r"[А-Яа-яЁё]", text or ""))
    lat = len(re.findall(r"[A-Za-z]", text or ""))
    return "ru" if cyr >= lat else "en"

def clean_text(s: str) -> str:
    if not s: return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"https?://\S+", "", s)  # убрать голые URLs
    return s

PRIORITY_RULES = [
    (["санкц", "sanction", "embargo"], 10, True),
    (["цб", "ставк", "key rate", "фрс", "ecb", "cbr", "rate hike", "rate cut"], 9, True),
    (["default", "дефолт", "обвал", "crash", "обруш"], 9, True),
    (["офз", "minfin", "moex", "мосбирж", "русал", "сбер", "газпром"], 7, False),
    (["inflation", "инфляц", "gdp", "ввп", "cpi", "pce", "pmi"], 7, False),
    (["brent", "нефть", "oil", "газ", "gas", "energy"], 6, False),
    (["ipo", "m&a", "acquisition", "сделк", "buyback", "выкуп"], 6, False),
]

def compute_priority_and_urgent(title: str, summary: str) -> tuple[float, bool]:
    text = f"{title} {summary}".lower()
    score = 0.0
    urgent = False
    for kws, base, is_urgent in PRIORITY_RULES:
        if any(k in text for k in kws):
            score += base
            urgent = urgent or is_urgent
    score += min(len(summary) / 500.0, 2.0)
    return score, urgent

def pick_emoji(title: str, summary: str, urgent: bool) -> str:
    if urgent:
        return "🚨"
    txt = f"{title} {summary}".lower()
    if any(k in txt for k in ["цб", "ставк", "фрс", "ecb", "cbr", "регулятор", "rate"]):
        return "🏦"
    if any(k in txt for k in ["инфляц", "ввп", "gdp", "cpi", "pce", "pmi"]):
        return "📊"
    if any(k in txt for k in ["нефть", "brent", "gas", "газ", "energy"]):
        return "🛢️"
    if any(k in txt for k in ["акци", "индекс", "s&p", "nasdaq", "moex", "риск"]):
        return "📈"
    return "📰"

def openai_chat(prompt: str, model: str = "gpt-4o-mini") -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is empty")
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
        "max_tokens": 360,
    }
    resp = requests.post(url, headers=headers, data=json.dumps(data), timeout=20)
    resp.raise_for_status()
    j = resp.json()
    return j["choices"][0]["message"]["content"].strip()

def translate_ru(text: str) -> str:
    if not text:
        return ""
    if not ENABLE_TRANSLATE or not OPENAI_API_KEY:
        return text
    prompt = (
        "Переведи на русский литературно и сжато, максимум 160 символов. "
        "Без кавычек и ссылок:\n\n" + text
    )
    try:
        return openai_chat(prompt)
    except Exception:
        return text

def concise_summary(title: str, summary: str, link: str) -> str:
    base = clean_text(summary) or clean_text(title)
    if not ENABLE_SUMMARY or not OPENAI_API_KEY:
        return (base[:SUMMARY_MAX_CHARS] + "…") if len(base) > SUMMARY_MAX_CHARS else base
    prompt = (
        f"Сжато перескажи суть новости (1–2 предложения, до {SUMMARY_MAX_CHARS} символов), "
        "живым деловым тоном, без клише, без слова 'AI-анализ', без ссылок, без HTML. "
        "Если есть цифры/сроки — включи их. Фокус: влияние на рынки/экономику/рубль/акции.\n\n"
        f"Заголовок: {title}\nАннотация: {summary}\nURL: {link}"
    )
    try:
        txt = openai_chat(prompt)
        txt = clean_text(txt)
        if len(txt) > SUMMARY_MAX_CHARS:
            txt = txt[:SUMMARY_MAX_CHARS - 1] + "…"
        return txt
    except Exception:
        return (base[:SUMMARY_MAX_CHARS] + "…") if len(base) > SUMMARY_MAX_CHARS else base

KEYWORDS_FOR_LINK = [
    "ФРС","ECB","ЕЦБ","ЦБ","ЦБР","Минфин","РФ","Рубль","Рынки","Рынок","Нефть","Brent",
    "Санкции","Газпром","Сбер","Мосбиржа","MOEX","ОФЗ","Китай","США","Евросоюз","ОПЕК",
    "Nasdaq","S&P","Dow","EU","UK","Япония","Канада","Индия","Казахстан"
]

def linkify_in_title(title_ru: str, link: str) -> str:
    if not link or not title_ru:
        return title_ru
    for kw in KEYWORDS_FOR_LINK:
        pattern = r'(?<![A-Яа-яA-Za-z0-9])(' + re.escape(kw) + r')(?![A-Яа-яA-Za-z0-9])'
        if re.search(pattern, title_ru):
            return re.sub(pattern, rf'<a href="{link}">\1</a>', title_ru, count=1)
    tokens = title_ru.split()
    for i, t in enumerate(tokens):
        if len(re.sub(r"[^A-Za-zА-Яа-яЁё0-9]", "", t)) >= 4:
            tokens[i] = f'<a href="{link}">{t}</a>'
            return " ".join(tokens)
    return f'<a href="{link}">{title_ru}</a>'

def format_post(item: dict, priority: float, urgent: bool) -> str:
    title = clean_text(item.get("title") or "")
    link = (item.get("link") or "").strip()
    summary = clean_text(item.get("summary") or "")

    lang = detect_lang(f"{title} {summary}")
    if lang != "ru":
        title_ru = translate_ru(title)
    else:
        title_ru = title

    title_linked = linkify_in_title(title_ru, link)
    core = concise_summary(title, summary, link)

    emoji = "🚨" if urgent else "📰"
    if not urgent:
        txt = (title + " " + summary).lower()
        if any(k in txt for k in ["цб","ставк","фрс","ecb","cbr","регулятор","rate"]):
            emoji = "🏦"
        elif any(k in txt for k in ["инфляц","ввп","gdp","cpi","pce","pmi"]):
            emoji = "📊"
        elif any(k in txt for k in ["нефть","brent","gas","газ","energy"]):
            emoji = "🛢️"
        elif any(k in txt for k in ["акци","индекс","s&p","nasdaq","moex","риск"]):
            emoji = "📈"

    lines = []
    if title_linked:
        lines.append(f"{emoji} <b>{title_linked}</b>")
    if core:
        lines.append(f"• {core}")
    return "\n".join(lines).strip()

_last_send_ts = 0.0

async def safe_send_message(bot: Bot, chat_id: int, text: str) -> bool:
    global _last_send_ts
    elapsed = time.time() - _last_send_ts
    if elapsed < MIN_SECONDS_BETWEEN_POSTS:
        await asyncio.sleep(MIN_SECONDS_BETWEEN_POSTS - elapsed)

    try:
        await bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=False)
        _last_send_ts = time.time()
        return True

    except TelegramRetryAfter as e:
        wait_s = int(getattr(e, "retry_after", 5)) + RETRY_AFTER_GRACE
        logging.warning(f"Flood limit: retry after {wait_s}s")
        await asyncio.sleep(wait_s)
        try:
            await bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=False)
            _last_send_ts = time.time()
            return True
        except Exception as e2:
            logging.exception(f"Retry after failed: {e2}")
            return False

    except (TelegramForbiddenError, TelegramBadRequest) as e:
        logging.exception(f"Send failed (forbidden/bad request): {e}")
        return False

    except Exception as e:
        logging.exception(f"Send failed (generic): {e}")
        return False

async def post_to_channel(bot: Bot, channel_id: int, item: dict, priority: float, urgent: bool) -> bool:
    return await safe_send_message(bot, channel_id, format_post(item, priority, urgent))

async def fetch_feed(session: aiohttp.ClientSession, url: str):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            content = await resp.read()
            feed = feedparser.parse(content)
            items = []
            for e in feed.entries[:12]:
                items.append({
                    "guid": e.get("id") or e.get("guid") or e.get("link"),
                    "title": (e.get("title") or "").strip(),
                    "link": (e.get("link") or "").strip(),
                    "summary": (e.get("summary") or e.get("description") or "").strip(),
                })
            return items
    except Exception as e:
        logging.warning(f"Ошибка ленты {url}: {e}")
        return []

async def fetch_all(urls: list[str]):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_feed(session, u) for u in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out = []
        for r in results:
            if isinstance(r, list):
                out.extend(r)
        return out

async def fetch_all_and_score(feeds: list[str]):
    items = await fetch_all(feeds)
    scored = []
    for it in items:
        title = clean_text(it.get("title") or "")
        summary = clean_text(it.get("summary") or "")
        pr, urg = compute_priority_and_urgent(title, summary)
        scored.append((pr, urg, it))
    scored.sort(key=lambda x: (x[1], x[0]), reverse=True)
    return scored

async def worker_loop(bot: Bot):
    feeds = load_feeds()
    if not feeds:
        logging.warning("Нет источников — добавь feeds/sources.csv или используй дефолтный список.")
    while True:
        try:
            logging.info("Fetching feeds...")
            scored = await fetch_all_and_score(feeds)
            logging.info(f"Fetched {len(scored)} items (sorted by priority)")
            posted = 0
            for pr, urg, it in scored:
                if posted >= MAX_POSTS_PER_CYCLE:
                    logging.info(f"Reached MAX_POSTS_PER_CYCLE={MAX_POSTS_PER_CYCLE}, breaking this cycle")
                    break

                guid = it.get("guid") or it.get("link")
                if was_posted(guid):
                    continue

                ok = await post_to_channel(bot, CHANNEL_ID, it, pr, urg)
                if ok:
                    posted += 1
                    mark_posted_and_store(it, pr, urg)
                    await asyncio.sleep(random.uniform(0.2, 0.9))
            if posted > 0:
                await asyncio.sleep(SLOWDOWN_AFTER_BURST)
        except Exception as e:
            logging.exception(f"Loop error: {e}")
        await asyncio.sleep(POLL_INTERVAL_SEC)

def now_in_tz(tzname: str) -> datetime:
    return datetime.now(ZoneInfo(tzname))

def next_run_after(times_str: str, tzname: str) -> datetime:
    tz = ZoneInfo(tzname)
    now = datetime.now(tz)
    times = []
    for part in times_str.split(","):
        part = part.strip()
        if not re.match(r"^\d{2}:\d{2}$", part):
            continue
        hh, mm = map(int, part.split(":"))
        candidate = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if candidate <= now:
            candidate = candidate + timedelta(days=1)
        times.append(candidate)
    if not times:
        return now + timedelta(hours=12)
    return min(times)

def build_digest_text(rows) -> str:
    if not rows:
        return "🗓 <b>Дайджест</b>\n• Новостей за период нет."
    lines = []
    title = f"🗓 <b>Итоги периода (последние {DIGEST_LOOKBACK_HOURS} ч)</b>"
    lines.append(title)
    for (ti, su, ln, pr, urg, ts) in rows:
        emoji = "🚨" if urg else "•"
        ti_c = clean_text(ti)
        su_c = clean_text(su)
        headline = ti_c if len(ti_c) <= 120 else ti_c[:117] + "…"
        comment = su_c if len(su_c) <= 160 else su_c[:157] + "…"
        lines.append(f"{emoji} <b>{translate_ru(headline) if detect_lang(headline)!='ru' else headline}</b>")
        if comment:
            lines.append(f"  — {translate_ru(comment) if detect_lang(comment)!='ru' else comment}")
        if ln:
            lines.append(f'  <a href="{ln}">Подробнее</a>')
    return "\n".join(lines)

async def digest_loop(bot: Bot):
    if not ENABLE_DIGEST:
        logging.info("Digest disabled")
        return
    while True:
        try:
            nxt = next_run_after(DIGEST_TIMES, DIGEST_TZ)
            sleep_s = (nxt - now_in_tz(DIGEST_TZ)).total_seconds()
            logging.info(f"Next digest at {nxt.isoformat()} ({DIGEST_TZ}) in {int(sleep_s)}s")
            await asyncio.sleep(max(5, sleep_s))
            rows = read_recent_items(DIGEST_LOOKBACK_HOURS, DIGEST_TOP_N)
            text = build_digest_text(rows)
            await safe_send_message(bot, CHANNEL_ID, text)
            await asyncio.sleep(5)
        except Exception as e:
            logging.exception(f"Digest error: {e}")
            await asyncio.sleep(30)

async def start_health_server():
    async def handle_health(request):
        return web.Response(text="OK")
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"Health server listening on 0.0.0.0:{PORT}")

async def on_startup(bot: Bot):
    logging.info("🚀 Bot started successfully")
    try:
        await bot.send_message(CHANNEL_ID, "✅ Axed News v3.1: умная ссылка в заголовке + лаконичный пересказ — активированы.")
    except Exception as e:
        logging.exception(f"Не удалось отправить тест в канал: {e}")

async def main():
    db_init()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await on_startup(bot)
    asyncio.create_task(worker_loop(bot))
    asyncio.create_task(digest_loop(bot))
    if BIND_WEB:
        asyncio.create_task(start_health_server())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
