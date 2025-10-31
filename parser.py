import asyncio
import logging
import aiohttp
import feedparser
from typing import List, Dict, Any

async def fetch_feed(session: aiohttp.ClientSession, url: str) -> List[Dict[str, Any]]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            content = await resp.read()
            feed = feedparser.parse(content)
            items = []
            for e in feed.entries[:15]:
                guid = e.get("id") or e.get("guid") or e.get("link")
                title = e.get("title", "").strip()
                link = e.get("link", "").strip()
                summary = (e.get("summary") or e.get("description") or "").strip()
                items.append({"guid": guid, "title": title, "link": link, "summary": summary})
            return items
    except Exception as e:
        logging.exception(f"Failed to fetch {url}: {e}")
        return []

async def fetch_all(feeds: List[str]) -> List[Dict[str, Any]]:
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_feed(session, u) for u in feeds]
        batches = await asyncio.gather(*tasks, return_exceptions=True)
        results = []
        for b in batches:
            if isinstance(b, list):
                results.extend(b)
        return results
