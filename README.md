# Axed News 2.0 — Minimal Working Template (Render + GitHub + iPhone)

Рабочий минимальный агрегатор новостей для Telegram:
- aiogram v3 (long polling)
- feedparser + aiohttp
- дедупликация через SQLite (`data.db`)
- готов для деплоя на Render как **Background Worker**

## Запуск локально
1) Создай `.env` на основе `.env.example`
2) Установи зависимости:  
   ```bash
   pip install -r requirements.txt
   ```
3) Запусти:  
   ```bash
   python main.py
   ```

## Деплой на Render
1) Тип сервиса: **Background Worker**
2) Build Command:
   ```
   pip install -r requirements.txt
   ```
3) Start Command:
   ```
   python main.py
   ```
4) Переменные окружения (Environment):
   - `BOT_TOKEN`
   - `CHANNEL_ID` (формат -100XXXXXXXXXX)
   - `OPENAI_API_KEY` (опционально)

> Файлы `.env` и `data.db` лучше **не коммитить**. На Render задаём переменные в UI.

## Структура
```
.
├── main.py
├── requirements.txt
├── README.md
├── .env.example
├── feeds/
│   └── sources.txt
└── modules/
    ├── parser.py
    └── poster.py
```

## Настройка источников
Отредактируй `feeds/sources.txt` — по одному RSS/Atom URL на строку.

## Логи
Логи печатаются в stdout (видно на Render → Logs).



## Новое в этой сборке
- `feeds/sources.csv` — 120+ источников (url, priority, tags, lang)
- `config.json` — ключевые слова (include/exclude), окно постинга (08–23), лимит в день, минимальный интервал, скоринг.
- Дедуп + квоты в SQLite (`data.db`): суточный счётчик и `last_post_ts`.
- Простая модель оценки важности: `score = priority*weight + keyword_hits*2`

### Кастомизация
- Меняй `priority` в `feeds/sources.csv` (1..3). 3 — максимально важно.
- Ключевые слова/стоп-слова — в `config.json`.
- Окно постинга, лимиты и интервал — в `config.json`.

