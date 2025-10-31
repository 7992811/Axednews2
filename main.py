import os
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message

from modules.parser import fetch_all
from modules.poster import post_to_channel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

if CHANNEL_ID:
    try:
        CHANNEL_ID = int(CHANNEL_ID)
    except Exception:
        raise RuntimeError("CHANNEL_ID должен быть числом, формат -100XXXXXXXXXX")

DB_PATH = "data.db"
FEEDS_FILE = os.path.join("feeds", "sources.csv")
CONFIG_FILE = "config.json"
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "600"))  # каждые 10 минут

dp = Dispatcher()

@dp.message(CommandStart())
async def start(m: Message):
    await m.answer("🟢 Axed News 2.0 работает. Буду приносить новости из RSS.")

def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS posted (
        guid TEXT PRIMARY KEY,
        ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    con.commit()
    con.close()

def was_posted(guid: str) -> bool:
    if not guid:
        return False
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM posted WHERE guid = ?", (guid,))
    row = cur.fetchone()
    con.close()
    return row is not None

def mark_posted(guid: str):
    if not guid:
        return
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO posted (guid) VALUES (?)", (guid,))
        con.commit()
    finally:
        con.close()

def load_config() -> dict:
    if not os.path.exists(CONFIG_FILE):
        logging.warning("config.json not found — используются значения по умолчанию в коде.")
        return {}
    import json
    with open(CONFIG_FILE, "r", encoding="utf-8") as cf:
        return json.load(cf)

def load_feeds() -> list:
    import csv
    feeds = []
    if not os.path.exists(FEEDS_FILE):
        logging.warning("feeds/sources.csv not found — добавь источники.")
        return feeds
    with open(FEEDS_FILE, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            url = (row.get("url") or "").strip()
            if not url: 
                continue
            try:
                pr = int(row.get("priority") or 1)
            except Exception:
                pr = 1
            tags = (row.get("tags") or "").strip()
            lang = (row.get("lang") or "en").strip().lower()
            feeds.append({"url": url, "priority": pr, "tags": tags, "lang": lang})
    return feeds


def within_hours(cfg: dict) -> bool:
    from datetime import datetime
    hstart = int((cfg.get("hours_window") or {}).get("start", 0))
    hend = int((cfg.get("hours_window") or {}).get("end", 23))
    now = datetime.now()
    return hstart <= now.hour <= hend

def kw_score(text: str, cfg: dict) -> float:
    text_l = (text or "").lower()
    inc = cfg.get("include_keywords") or []
    exc = cfg.get("exclude_keywords") or []
    s = 0.0
    for w in inc:
        if w.lower() in text_l:
            s += cfg.get("score", {}).get("keyword_hit", 2.0)
    for w in exc:
        if w.lower() in text_l:
            return -999.0
    return s

def quota_ok(con: sqlite3.Connection, cfg: dict) -> bool:
    from datetime import date
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, val TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS stats (d TEXT PRIMARY KEY, cnt INTEGER)")
    # daily cap
    d = str(date.today())
    cur.execute("SELECT cnt FROM stats WHERE d=?", (d,))
    row = cur.fetchone()
    cnt = 0 if not row else int(row[0])
    max_per_day = int(cfg.get("max_posts_per_day", 100))
    if cnt >= max_per_day:
        return False
    # min interval
    cur.execute("SELECT val FROM meta WHERE key='last_post_ts'")
    row = cur.fetchone()
    import time as _t
    now = int(_t.time())
    min_interval = int(cfg.get("min_seconds_between_posts", 60))
    if row:
        last_ts = int(row[0])
        if now - last_ts < min_interval:
            return False
    return True

def mark_quota(con: sqlite3.Connection, cfg: dict):
    from datetime import date
    cur = con.cursor()
    d = str(date.today())
    cur.execute("INSERT INTO stats(d,cnt) VALUES(?,1) ON CONFLICT(d) DO UPDATE SET cnt=cnt+1", (d,))
    import time as _t
    cur.execute("INSERT INTO meta(key,val) VALUES('last_post_ts',?) ON CONFLICT(key) DO UPDATE SET val=excluded.val", (str(int(_t.time())),))
    con.commit()

async def worker_loop(bot: Bot):
    feeds = load_feeds()
    cfg = load_config()
    if not feeds:
        logging.warning("Нет источников. Добавь строки в feeds/sources.csv")
    con = sqlite3.connect(DB_PATH)
    while True:
        try:
            if not within_hours(cfg):
                logging.info("Вне временного окна — ждём...")
                await asyncio.sleep(POLL_INTERVAL_SEC)
                continue

            logging.info("Fetching feeds...")
            feed_urls = [f["url"] for f in feeds]
            items = await fetch_all(feed_urls)
            logging.info(f"Fetched {len(items)} items")

            posted = 0
            for it in items:
                guid = it.get("guid") or it.get("link")
                if was_posted(guid):
                    continue
                text_blob = " ".join([it.get("title") or "", it.get("summary") or ""])
                s_kw = kw_score(text_blob, cfg)
                # estimate priority by source (best effort: base on first matching feed by link hostname)
                pr = 1
                for f in feeds:
                    if f["url"].split("/")[2] in (it.get("link") or ""):
                        pr = int(f["priority"])
                        break
                score = pr * float((cfg.get("score") or {}).get("priority_weight", 1.0)) + s_kw
                if score < float((cfg.get("score") or {}).get("min_score_to_post", 0.0)):
                    continue

                if not quota_ok(con, cfg):
                    logging.info("Квота на постинг исчерпана/интервал — пропускаем до следующего цикла")
                    break

                ok = await post_to_channel(bot, CHANNEL_ID, it)
                if ok:
                    posted += 1
                    mark_posted(guid)
                    mark_quota(con, cfg)

            logging.info(f"Posted {posted} new items")
        except Exception as e:
            logging.exception(f"Loop error: {e}")
        await asyncio.sleep(POLL_INTERVAL_SEC)


async def on_startup(bot: Bot):
    logging.info("🚀 Bot started successfully")
    # тестовое сообщение в канал
    if CHANNEL_ID:
        try:
            await bot.send_message(CHANNEL_ID, "✅ Axed News 2.0: деплой успешен, начинаем сбор ленты.")
        except Exception as e:
            logging.exception(f"Не удалось отправить тест в канал: {e}")

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан (Environment/.env)")
    if not CHANNEL_ID:
        raise RuntimeError("CHANNEL_ID не задан (Environment/.env)")

    db_init()
    bot = Bot(BOT_TOKEN, parse_mode=ParseMode.HTML)
    await on_startup(bot)
    # Запускаем фоновую задачу
    asyncio.create_task(worker_loop(bot))
    # Запускаем polling, чтобы процесс не завершался
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
