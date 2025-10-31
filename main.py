import os, sys, asyncio, logging, sqlite3, csv, json, re, time, random
import requests
import aiohttp, feedparser
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError, TelegramBadRequest

print("MAIN_VERSION=2025-10-31-AF-translate-format", flush=True)

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
ENABLE_COMMENT = os.getenv("ENABLE_COMMENT", "1") == "1"

# Anti-flood controls (tune in Render ENV)
MIN_SECONDS_BETWEEN_POSTS = float(os.getenv("MIN_SECONDS_BETWEEN_POSTS", "1.4"))
MAX_POSTS_PER_CYCLE = int(os.getenv("MAX_POSTS_PER_CYCLE", "6"))
RETRY_AFTER_GRACE = int(os.getenv("RETRY_AFTER_GRACE", "2"))
SLOWDOWN_AFTER_BURST = int(os.getenv("SLOWDOWN_AFTER_BURST", "10"))

# Timings
DB_PATH = "data.db"
FEEDS_FILE = "feeds/sources.csv"
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "600"))  # 10 минут

dp = Dispatcher()

@dp.message(CommandStart())
async def start(m: Message):
    await m.answer("🟢 Axed News запущен. Улучшенная подача включена.")

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
    con.commit(); con.close()

def was_posted(guid: str) -> bool:
    if not guid: return False
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("SELECT 1 FROM posted WHERE guid=?", (guid,))
    row = cur.fetchone(); con.close()
    return row is not None

def mark_posted(guid: str):
    if not guid: return
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO posted (guid) VALUES (?)", (guid,))
    con.commit(); con.close()

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

def pick_emoji(title: str, summary: str) -> str:
    txt = f"{title} {summary}".lower()
    if any(k in txt for k in ["breaking", "emergency", "санкц", "обвал", "обруш", "urgent", "downgrade", "default"]):
        return "🚨"
    if any(k in txt for k in ["цб", "ставк", "фрс", "ecb", "cbr", "регулятор"]):
        return "🏦"
    if any(k in txt for k in ["инфляц", "ввп", "gdp", "cpi", "pce", "pmi"]):
        return "📊"
    if any(k in txt for k in ["нефть", "brent", "gas", "газ", "energy"]):
        return "🛢️"
    if any(k in txt for k in ["акци", "индекс", "s&p", "nasdaq", "moex", "риск"]):
        return "📈"
    return "📰"

def clean_text(s: str) -> str:
    if not s: return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"https?://\S+", "", s)  # убрать голые URLs
    return s

def openai_chat(prompt: str, model: str = "gpt-4o-mini") -> str:
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
        "max_tokens": 300,
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

def make_commentary(title: str, summary: str) -> str:
    base = clean_text(summary) or clean_text(title)
    if not ENABLE_COMMENT or not OPENAI_API_KEY:
        return (base[:200] + "…") if len(base) > 200 else base
    prompt = (
        "Дай краткий комментарий к новости (1–2 предложения, до 220 символов). "
        "Без клише и слов 'AI-анализ', без ссылок. "
        "Фокус: почему важно для рынков/экономики/рубля/акций.\n\n"
        f"Заголовок: {title}\nСодержание: {summary}"
    )
    try:
        return openai_chat(prompt)
    except Exception:
        return (base[:200] + "…") if len(base) > 200 else base

# === Parser ===
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

# === Formatting & sending ===
def format_post(item: dict) -> str:
    title = clean_text(item.get("title") or "")
    link = (item.get("link") or "").strip()
    summary = clean_text(item.get("summary") or "")

    lang = detect_lang(f"{title} {summary}")
    if lang != "ru":
        title_ru = translate_ru(title)
        comm_ru = make_commentary(title, summary)
    else:
        title_ru = title
        comm_ru = make_commentary(title, summary)

    emoji = pick_emoji(title, summary)

    lines = []
    if title_ru:
        lines.append(f"{emoji} <b>{title_ru}</b>")
    if comm_ru:
        lines.append(f"• {comm_ru}")
    if link:
        lines.append(f'\n<a href="{link}">Источник</a>')
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

async def post_to_channel(bot: Bot, channel_id: int, item: dict) -> bool:
    return await safe_send_message(bot, channel_id, format_post(item))

# === Worker ===
async def worker_loop(bot: Bot):
    feeds = load_feeds()
    if not feeds:
        logging.warning("Нет источников — добавь feeds/sources.csv или используй дефолтный список.")
    while True:
        try:
            logging.info("Fetching feeds...")
            items = await fetch_all(feeds)
            logging.info(f"Fetched {len(items)} items")
            posted = 0
            for it in items:
                if posted >= MAX_POSTS_PER_CYCLE:
                    logging.info(f"Reached MAX_POSTS_PER_CYCLE={MAX_POSTS_PER_CYCLE}, breaking this cycle")
                    break

                guid = it.get("guid") or it.get("link")
                if was_posted(guid):
                    continue

                ok = await post_to_channel(bot, CHANNEL_ID, it)
                if ok:
                    posted += 1
                    mark_posted(guid)
                    await asyncio.sleep(random.uniform(0.0, 0.6))  # small jitter
            if posted > 0:
                await asyncio.sleep(SLOWDOWN_AFTER_BURST)
        except Exception as e:
            logging.exception(f"Loop error: {e}")
        await asyncio.sleep(POLL_INTERVAL_SEC)

async def on_startup(bot: Bot):
    logging.info("🚀 Bot started successfully")
    try:
        await bot.send_message(CHANNEL_ID, "✅ Axed News: улучшенный формат и анти-флуд активированы.")
    except Exception as e:
        logging.exception(f"Не удалось отправить тест в канал: {e}")

async def main():
    db_init()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await on_startup(bot)
    asyncio.create_task(worker_loop(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
