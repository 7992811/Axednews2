import os, sys, asyncio, logging, sqlite3
import aiohttp, feedparser
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

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

DB_PATH = "data.db"
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "600"))

dp = Dispatcher()

@dp.message(CommandStart())
async def start(m: Message):
    await m.answer("🟢 Axed News запущен. Готов постить новости из RSS.")

def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS posted (guid TEXT PRIMARY KEY, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
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

def default_feeds():
    return [
        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        "https://www.reuters.com/finance/rss",
        "https://www.economist.com/finance-and-economics/rss.xml",
        "https://www.rbc.ru/economics/?rss",
        "https://www.moex.com/export/news.aspx?cat=stocks",
        "https://cbr.ru/press/pr/?rss=1",
    ]

async def fetch_feed(session: aiohttp.ClientSession, url: str):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            content = await resp.read()
            feed = feedparser.parse(content)
            items = []
            for e in feed.entries[:10]:
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

def format_post(item: dict) -> str:
    title = item.get("title") or "Без заголовка"
    link = item.get("link") or ""
    summary = item.get("summary") or ""
    return f"<b>{title}</b>\n{summary[:300]}{'…' if len(summary)>300 else ''}\n\n<a href=\"{link}\">Читать</a>"

async def post_to_channel(bot: Bot, channel_id: int, item: dict) -> bool:
    try:
        await bot.send_message(chat_id=channel_id, text=format_post(item),
                               parse_mode=ParseMode.HTML, disable_web_page_preview=False)
        return True
    except Exception as e:
        logging.exception(f"Failed to post: {e}")
        return False

async def worker_loop(bot: Bot):
    feeds = default_feeds()
    while True:
        try:
            logging.info("Fetching feeds...")
            items = await fetch_all(feeds)
            logging.info(f"Fetched {len(items)} items")
            posted = 0
            for it in items:
                guid = it.get("guid") or it.get("link")
                if was_posted(guid):
                    continue
                ok = await post_to_channel(bot, CHANNEL_ID, it)
                if ok:
                    posted += 1
                    mark_posted(guid)
            logging.info(f"Posted {posted} new items")
        except Exception as e:
            logging.exception(f"Loop error: {e}")
        await asyncio.sleep(POLL_INTERVAL_SEC)

async def on_startup(bot: Bot):
    logging.info("🚀 Bot started successfully")
    try:
        await bot.send_message(CHANNEL_ID, "✅ Axed News: деплой успешен, начинаем сбор ленты.")
    except Exception as e:
        logging.exception(f"Не удалось отправить тест в канал: {e}")

async def main():
    db_init()
    bot = Bot(BOT_TOKEN, parse_mode=ParseMode.HTML)
    await on_startup(bot)
    asyncio.create_task(worker_loop(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
