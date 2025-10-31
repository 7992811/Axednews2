import logging
from aiogram import Bot
from aiogram.enums import ParseMode

def format_post(item: dict) -> str:
    title = item.get("title") or "Без заголовка"
    link = item.get("link") or ""
    summary = item.get("summary") or ""
    # Компактное форматирование
    text = f"<b>{title}</b>\n{summary[:300]}{'…' if len(summary) > 300 else ''}\n\n<a href=\"{link}\">Читать</a>"
    return text

async def post_to_channel(bot: Bot, channel_id: int, item: dict) -> bool:
    try:
        text = format_post(item)
        await bot.send_message(chat_id=channel_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
        return True
    except Exception as e:
        logging.exception(f"Failed to post item: {e}")
        return False
