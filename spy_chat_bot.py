"""
Spy Chat Bot — архив бизнес-чатов Telegram.

Бот подключается к аккаунту через «Telegram для бизнеса», сохраняет переписку
и присылает владельцу удалённые (и, по желанию, изменённые) сообщения.

Команды владельца: /settings /search /export /stats /history
Команды админа:    /admin /backup /givepremium /refund
"""
from __future__ import annotations

import asyncio
import gzip
import html
import logging
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import (
    BotCommand,
    BufferedInputFile,
    BusinessConnection,
    BusinessMessagesDeleted,
    CallbackQuery,
    ErrorEvent,
    FSInputFile,
    InlineKeyboardButton as Btn,
    InlineKeyboardMarkup,
    LabeledPrice,
    LinkPreviewOptions,
    Message,
    PreCheckoutQuery,
)
from dotenv import load_dotenv

# ════════════════════════════ Настройки ════════════════════════════

load_dotenv()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
ADMIN_IDS = {
    int(x) for x in re.split(r"[,\s]+", os.getenv("ADMIN_IDS") or os.getenv("ADMIN_ID") or "") if x.isdigit()
}
DB_PATH = os.getenv("DB_PATH", "bot_archive.db")
MEDIA_DIR = os.getenv("MEDIA_DIR", "bot_media")
TIMEZONE = ZoneInfo(os.getenv("TIMEZONE", "Asia/Tashkent"))
RETENTION_DAYS = max(1, _env_int("RETENTION_DAYS", 30))  # через сколько дней удалять ВСЕ данные
BACKUP_HOUR = _env_int("BACKUP_HOUR", 4)    # во сколько (по TIMEZONE) делать бэкап
BACKUP_KEEP = max(1, _env_int("BACKUP_KEEP", 7))  # сколько последних бэкапов хранить на сервере
BACKUP_DIR = os.getenv("BACKUP_DIR") or os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "backups")
CLEANUP_HOUR = _env_int("CLEANUP_HOUR", 5)  # во сколько удалять старые данные
MAX_DOWNLOAD_MB = min(_env_int("MAX_DOWNLOAD_MB", 20), 20)  # Bot API не скачивает файлы > 20 МБ
MEDIA_QUOTA_MB = _env_int("MEDIA_QUOTA_MB", 500)            # лимит медиа на одного владельца

PREMIUM_PRICE = _env_int("PREMIUM_PRICE", 100)  # в Telegram Stars
PREMIUM_DAYS = _env_int("PREMIUM_DAYS", 30)
SUPPORT_CONTACT = (os.getenv("SUPPORT_CONTACT") or "").strip()  # например @your_username

QUIET_START, QUIET_END = 23, 8               # «тихая ночь»
NO_CAPTION_TYPES = {"video_note", "sticker"}
MEDIA_LABELS = {
    "photo": "фото", "video": "видео", "animation": "GIF", "voice": "голосовое",
    "video_note": "кружочек", "audio": "аудио", "document": "документ", "sticker": "стикер",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("spy_chat_bot")

router = Router()

# ════════════════════════════ Утилиты ════════════════════════════


def now() -> datetime:
    return datetime.now(TIMEZONE)


def ts(dt: Optional[datetime] = None) -> str:
    return (dt or now()).strftime("%Y-%m-%d %H:%M:%S")


def parse_ts(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TIMEZONE)


def esc(value: Any) -> str:
    return html.escape(str(value)) if value is not None else ""


def fit(raw: Optional[str], limit: int) -> str:
    """Экранирует текст для HTML и обрезает так, чтобы итог был не длиннее limit."""
    raw = raw or ""
    if len(esc(raw)) <= limit:
        return esc(raw)
    lo, hi = 0, min(len(raw), limit)  # двоичный поиск самой длинной подходящей обрезки
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if len(esc(raw[:mid])) + 1 <= limit:
            lo = mid
        else:
            hi = mid - 1
    return esc(raw[:lo]) + "…"


def is_quiet_time() -> bool:
    h = now().hour
    return h >= QUIET_START or h < QUIET_END


def safe_name(value: str, maxlen: int = 40) -> str:
    return (re.sub(r"[^\w\-]+", "_", value or "", flags=re.UNICODE)[:maxlen]) or "x"


def file_ext(name: Optional[str], default: str) -> str:
    ext = os.path.splitext(name or "")[1].lower()
    return ext if re.fullmatch(r"\.[a-z0-9]{1,8}", ext) else default


def user_label(user) -> str:
    if not user:
        return "Неизвестный"
    name = user.full_name or "Без имени"
    return f"{name} (@{user.username})" if user.username else name


def chat_label(chat) -> str:
    return chat.title or chat.full_name or "Личные сообщения"


def human_size(n: int) -> str:
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if n < 1024 or unit == "ГБ":
            return f"{n:.0f} {unit}" if unit == "Б" else f"{n:.1f} {unit}"
        n /= 1024
    return str(n)


# ════════════════════════════ База данных ════════════════════════════

MESSAGES_DDL = """
CREATE TABLE IF NOT EXISTS {name} (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id TEXT    NOT NULL DEFAULT '',
    chat_id       INTEGER NOT NULL,
    message_id    INTEGER NOT NULL,
    user_id       INTEGER NOT NULL,
    user_name     TEXT    NOT NULL,
    chat_title    TEXT    NOT NULL,
    created_at    TEXT    NOT NULL,
    is_deleted    INTEGER NOT NULL DEFAULT 0,
    deleted_at    TEXT,
    media_type    TEXT,
    media_path    TEXT,
    file_id       TEXT,
    media_size    INTEGER,
    UNIQUE(connection_id, chat_id, message_id)
);
"""

SCHEMA = MESSAGES_DDL.format(name="messages") + """
CREATE INDEX IF NOT EXISTS idx_messages_conn_chat ON messages(connection_id, chat_id);
CREATE INDEX IF NOT EXISTS idx_messages_created   ON messages(created_at);

CREATE TABLE IF NOT EXISTS message_versions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    message_ref_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    version_num    INTEGER NOT NULL,
    text           TEXT    NOT NULL,
    saved_at       TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_versions_ref ON message_versions(message_ref_id);

CREATE TABLE IF NOT EXISTS business_connections (
    connection_id TEXT PRIMARY KEY,
    user_id       INTEGER NOT NULL,
    user_name     TEXT    NOT NULL,
    user_chat_id  INTEGER,
    is_enabled    INTEGER NOT NULL DEFAULT 1,
    updated_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_conn_user ON business_connections(user_id);

CREATE TABLE IF NOT EXISTS settings (
    owner_id       INTEGER PRIMARY KEY,
    notify_deleted INTEGER NOT NULL DEFAULT 1,
    notify_edited  INTEGER NOT NULL DEFAULT 0,
    quiet_night    INTEGER NOT NULL DEFAULT 0,
    retention_days INTEGER
);

CREATE TABLE IF NOT EXISTS muted_chats (
    owner_id   INTEGER NOT NULL,
    chat_id    INTEGER NOT NULL,
    chat_title TEXT    NOT NULL,
    PRIMARY KEY (owner_id, chat_id)
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS subscriptions (
    user_id              INTEGER PRIMARY KEY,
    premium_until        TEXT NOT NULL,
    reminded_for         TEXT,
    expired_notified_for TEXT
);

CREATE TABLE IF NOT EXISTS payments (
    charge_id  TEXT PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    amount     INTEGER NOT NULL,
    days       INTEGER NOT NULL,
    created_at TEXT    NOT NULL,
    refunded   INTEGER NOT NULL DEFAULT 0
);
"""

SETTING_KEYS = {"notify_deleted", "notify_edited", "quiet_night", "retention_days"}
DEFAULT_SETTINGS = {"notify_deleted": 1, "notify_edited": 0, "quiet_night": 0, "retention_days": None}

# Все сообщения, принадлежащие владельцу (через его бизнес-подключения)
OWNER_JOIN = "FROM messages m JOIN business_connections c ON c.connection_id = m.connection_id"


class Database:
    """Одно постоянное соединение с SQLite на всё время работы бота."""

    def __init__(self, path: str):
        self.path = path
        self.conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        folder = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(folder, exist_ok=True)
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA synchronous=NORMAL")
        await self.conn.create_function(
            "py_lower", 1, lambda s: s.lower() if isinstance(s, str) else s, deterministic=True
        )
        await self._migrate_old_schema()
        await self.conn.executescript(SCHEMA)
        msg_cols = {r[1] for r in await self.fetchall("PRAGMA table_info(messages)")}
        if "media_size" not in msg_cols:
            await self.conn.execute("ALTER TABLE messages ADD COLUMN media_size INTEGER")
        await self.conn.execute("PRAGMA foreign_keys=ON")
        await self.conn.commit()

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()
            self.conn = None

    async def _migrate_old_schema(self) -> None:
        """Переносит базу от старой версии бота (UNIQUE без подключения) в новую схему."""
        cols = {r[1] for r in await self.fetchall("PRAGMA table_info(messages)")}
        if cols and "connection_id" not in cols:
            logger.info("Обнаружена база старой версии — выполняю миграцию…")
            old_conn = "COALESCE(business_connection_id, '')" if "business_connection_id" in cols else "''"
            old_type = "media_type" if "media_type" in cols else "NULL"
            old_path = "media_path" if "media_path" in cols else "NULL"
            await self.conn.execute("PRAGMA foreign_keys=OFF")
            await self.conn.executescript(
                MESSAGES_DDL.format(name="messages_new")
                + f"""
                INSERT INTO messages_new (id, connection_id, chat_id, message_id, user_id, user_name,
                                          chat_title, created_at, is_deleted, media_type, media_path, file_id)
                SELECT id, {old_conn}, chat_id, message_id, user_id, user_name, chat_title, created_at,
                       is_deleted, {old_type},
                       CASE WHEN instr({old_path}, '/') > 0 OR instr({old_path}, '\\') > 0 THEN {old_path} END,
                       CASE WHEN instr({old_path}, '/') = 0 AND instr({old_path}, '\\') = 0 THEN {old_path} END
                FROM messages;
                DROP TABLE messages;
                ALTER TABLE messages_new RENAME TO messages;
                """
            )
            await self.conn.commit()
            logger.info("Миграция сообщений завершена.")

        conn_cols = {r[1] for r in await self.fetchall("PRAGMA table_info(business_connections)")}
        if conn_cols:
            for col, ddl in (
                ("user_chat_id", "user_chat_id INTEGER"),
                ("is_enabled", "is_enabled INTEGER NOT NULL DEFAULT 1"),
                ("updated_at", "updated_at TEXT"),
            ):
                if col not in conn_cols:
                    await self.conn.execute(f"ALTER TABLE business_connections ADD COLUMN {ddl}")
            await self.conn.commit()

    # ── низкоуровневые помощники ──
    async def fetchone(self, sql: str, params: tuple = ()) -> Optional[aiosqlite.Row]:
        async with self.conn.execute(sql, params) as cur:
            return await cur.fetchone()

    async def fetchall(self, sql: str, params: tuple = ()) -> list:
        async with self.conn.execute(sql, params) as cur:
            return list(await cur.fetchall())

    async def run(self, sql: str, params: tuple = ()) -> aiosqlite.Cursor:
        cur = await self.conn.execute(sql, params)
        await self.conn.commit()
        return cur

    # ── подключения ──
    async def save_connection(self, conn_id: str, user_id: int, user_name: str,
                              user_chat_id: Optional[int], enabled: bool) -> None:
        await self.run(
            """
            INSERT INTO business_connections (connection_id, user_id, user_name, user_chat_id, is_enabled, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(connection_id) DO UPDATE SET
                user_id = excluded.user_id, user_name = excluded.user_name,
                user_chat_id = excluded.user_chat_id, is_enabled = excluded.is_enabled,
                updated_at = excluded.updated_at
            """,
            (conn_id, user_id, user_name, user_chat_id, int(enabled), ts()),
        )

    async def get_connection(self, conn_id: str):
        return await self.fetchone("SELECT * FROM business_connections WHERE connection_id = ?", (conn_id,))

    async def is_owner(self, user_id: int) -> bool:
        return await self.fetchone("SELECT 1 FROM business_connections WHERE user_id = ? LIMIT 1", (user_id,)) is not None

    async def owner_ids(self) -> list[int]:
        return [r[0] for r in await self.fetchall("SELECT DISTINCT user_id FROM business_connections")]

    # ── настройки ──
    async def get_settings(self, owner_id: int) -> dict:
        row = await self.fetchone("SELECT * FROM settings WHERE owner_id = ?", (owner_id,))
        return dict(row) if row else {"owner_id": owner_id, **DEFAULT_SETTINGS}

    async def set_setting(self, owner_id: int, key: str, value) -> None:
        if key not in SETTING_KEYS:
            raise ValueError(key)
        await self.conn.execute("INSERT OR IGNORE INTO settings (owner_id) VALUES (?)", (owner_id,))
        await self.run(f"UPDATE settings SET {key} = ? WHERE owner_id = ?", (value, owner_id))

    # ── скрытые чаты ──
    async def is_muted(self, owner_id: int, chat_id: int) -> bool:
        return await self.fetchone(
            "SELECT 1 FROM muted_chats WHERE owner_id = ? AND chat_id = ?", (owner_id, chat_id)
        ) is not None

    async def mute(self, owner_id: int, chat_id: int, title: str) -> None:
        await self.run("INSERT OR REPLACE INTO muted_chats VALUES (?, ?, ?)", (owner_id, chat_id, title))

    async def unmute(self, owner_id: int, chat_id: int) -> None:
        await self.run("DELETE FROM muted_chats WHERE owner_id = ? AND chat_id = ?", (owner_id, chat_id))

    async def list_muted(self, owner_id: int) -> list:
        return await self.fetchall(
            "SELECT chat_id, chat_title FROM muted_chats WHERE owner_id = ? ORDER BY chat_title", (owner_id,)
        )

    # ── сообщения ──
    async def save_message(self, conn_id: str, chat_id: int, message_id: int, user_id: int, user_name: str,
                           chat_title: str, text: Optional[str] = None, media_type: Optional[str] = None,
                           media_path: Optional[str] = None, file_id: Optional[str] = None,
                           media_size: Optional[int] = None) -> Optional[int]:
        """text=None — бесплатный тариф: сохраняем только факт сообщения (без текста и медиа)."""
        created = ts()
        try:
            cur = await self.conn.execute(
                """
                INSERT INTO messages (connection_id, chat_id, message_id, user_id, user_name, chat_title,
                                      created_at, media_type, media_path, file_id, media_size)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (conn_id, chat_id, message_id, user_id, user_name, chat_title, created,
                 media_type, media_path, file_id, media_size),
            )
        except aiosqlite.IntegrityError:
            return None
        ref_id = cur.lastrowid
        if text is not None:
            await self.conn.execute(
                "INSERT INTO message_versions (message_ref_id, version_num, text, saved_at) VALUES (?, 1, ?, ?)",
                (ref_id, text, created),
            )
        await self.conn.commit()
        return ref_id

    async def find_message(self, conn_id: str, chat_id: int, message_id: int):
        return await self.fetchone(
            "SELECT * FROM messages WHERE connection_id = ? AND chat_id = ? AND message_id = ?",
            (conn_id, chat_id, message_id),
        )

    async def get_message(self, ref_id: int):
        """Сообщение по внутреннему ID + ID владельца (для проверки доступа)."""
        return await self.fetchone(
            """
            SELECT m.*, c.user_id AS owner_id FROM messages m
            LEFT JOIN business_connections c ON c.connection_id = m.connection_id
            WHERE m.id = ?
            """,
            (ref_id,),
        )

    async def versions(self, ref_id: int) -> list:
        return await self.fetchall(
            "SELECT version_num, text, saved_at FROM message_versions WHERE message_ref_id = ? ORDER BY version_num",
            (ref_id,),
        )

    async def add_version(self, ref_id: int, text: str) -> int:
        row = await self.fetchone(
            "SELECT COALESCE(MAX(version_num), 0) FROM message_versions WHERE message_ref_id = ?", (ref_id,)
        )
        num = row[0] + 1
        await self.run(
            "INSERT INTO message_versions (message_ref_id, version_num, text, saved_at) VALUES (?, ?, ?, ?)",
            (ref_id, num, text, ts()),
        )
        return num

    async def mark_deleted(self, ref_id: int) -> None:
        await self.run("UPDATE messages SET is_deleted = 1, deleted_at = ? WHERE id = ?", (ts(), ref_id))

    async def delete_message(self, ref_id: int) -> Optional[str]:
        row = await self.fetchone("SELECT media_path FROM messages WHERE id = ?", (ref_id,))
        await self.conn.execute("DELETE FROM message_versions WHERE message_ref_id = ?", (ref_id,))
        await self.run("DELETE FROM messages WHERE id = ?", (ref_id,))
        return row["media_path"] if row else None

    # ── поиск / экспорт / статистика ──
    async def search(self, owner_id: int, query: str, limit: int = 20) -> list:
        pattern = "%" + re.sub(r"([\\%_])", r"\\\1", query.lower()) + "%"
        return await self.fetchall(
            f"""
            SELECT m.id, m.chat_title, m.user_name, m.created_at, m.is_deleted, v.text
            {OWNER_JOIN}
            JOIN message_versions v ON v.message_ref_id = m.id
            WHERE c.user_id = ? AND py_lower(v.text) LIKE ? ESCAPE '\\'
            GROUP BY m.id
            ORDER BY m.created_at DESC
            LIMIT ?
            """,
            (owner_id, pattern, limit),
        )

    async def owner_chats(self, owner_id: int, limit: int = 30) -> list:
        return await self.fetchall(
            f"""
            SELECT m.chat_id, MAX(m.chat_title) AS title, COUNT(*) AS cnt, MAX(m.created_at) AS last
            {OWNER_JOIN}
            WHERE c.user_id = ?
            GROUP BY m.chat_id
            ORDER BY last DESC
            LIMIT ?
            """,
            (owner_id, limit),
        )

    async def export_chat(self, owner_id: int, chat_id: int) -> tuple[list, dict]:
        rows = await self.fetchall(
            f"SELECT m.* {OWNER_JOIN} WHERE c.user_id = ? AND m.chat_id = ? ORDER BY m.created_at, m.message_id",
            (owner_id, chat_id),
        )
        vrows = await self.fetchall(
            f"""
            SELECT v.message_ref_id, v.version_num, v.text, v.saved_at
            {OWNER_JOIN}
            JOIN message_versions v ON v.message_ref_id = m.id
            WHERE c.user_id = ? AND m.chat_id = ?
            ORDER BY v.message_ref_id, v.version_num
            """,
            (owner_id, chat_id),
        )
        versions: dict[int, list] = {}
        for v in vrows:
            versions.setdefault(v["message_ref_id"], []).append(v)
        return rows, versions

    async def stats(self, owner_id: int) -> dict:
        base = await self.fetchone(
            f"""
            SELECT COUNT(*) AS total, COALESCE(SUM(m.is_deleted), 0) AS deleted,
                   COALESCE(SUM(m.media_type IS NOT NULL), 0) AS media, COUNT(DISTINCT m.chat_id) AS chats,
                   MIN(m.created_at) AS since
            {OWNER_JOIN} WHERE c.user_id = ?
            """,
            (owner_id,),
        )
        edited = await self.fetchone(
            f"""
            SELECT COUNT(*) FROM (
                SELECT m.id {OWNER_JOIN}
                JOIN message_versions v ON v.message_ref_id = m.id
                WHERE c.user_id = ? GROUP BY m.id HAVING COUNT(v.id) > 1
            )
            """,
            (owner_id,),
        )
        deleters = await self.fetchall(
            f"""
            SELECT MAX(m.user_name) AS name, COUNT(*) AS n
            {OWNER_JOIN}
            WHERE c.user_id = ? AND m.is_deleted = 1 AND m.user_id != c.user_id
            GROUP BY m.user_id ORDER BY n DESC LIMIT 5
            """,
            (owner_id,),
        )
        paths = await self.fetchall(
            f"SELECT m.media_path {OWNER_JOIN} WHERE c.user_id = ? AND m.media_path IS NOT NULL", (owner_id,)
        )
        return {**dict(base), "edited": edited[0], "deleters": deleters, "paths": [p[0] for p in paths]}

    async def chat_title(self, owner_id: int, chat_id: int) -> Optional[str]:
        row = await self.fetchone(
            f"SELECT m.chat_title {OWNER_JOIN} WHERE c.user_id = ? AND m.chat_id = ? ORDER BY m.id DESC LIMIT 1",
            (owner_id, chat_id),
        )
        return row[0] if row else None

    async def media_usage(self, owner_id: int) -> int:
        row = await self.fetchone(
            f"SELECT COALESCE(SUM(m.media_size), 0) {OWNER_JOIN} WHERE c.user_id = ? AND m.media_path IS NOT NULL",
            (owner_id,),
        )
        return row[0]

    async def _delete_ids(self, ids: list[int]) -> None:
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            marks = ",".join("?" * len(chunk))
            await self.conn.execute(f"DELETE FROM message_versions WHERE message_ref_id IN ({marks})", chunk)
            await self.conn.execute(f"DELETE FROM messages WHERE id IN ({marks})", chunk)
        await self.conn.commit()

    async def delete_owner_data(self, owner_id: int) -> tuple[int, list[str]]:
        """Полное удаление архива владельца (/delete_my_data). Подписка и оплаты сохраняются."""
        rows = await self.fetchall(f"SELECT m.id, m.media_path {OWNER_JOIN} WHERE c.user_id = ?", (owner_id,))
        await self._delete_ids([r["id"] for r in rows])
        await self.run("DELETE FROM muted_chats WHERE owner_id = ?", (owner_id,))
        await self.run("DELETE FROM settings WHERE owner_id = ?", (owner_id,))
        return len(rows), [r["media_path"] for r in rows if r["media_path"]]

    async def delete_older_than(self, cutoff: str) -> tuple[int, list[str]]:
        rows = await self.fetchall("SELECT id, media_path FROM messages WHERE created_at < ?", (cutoff,))
        await self._delete_ids([r["id"] for r in rows])
        return len(rows), [r["media_path"] for r in rows if r["media_path"]]

    # ── премиум ──
    async def premium_until(self, user_id: int) -> Optional[str]:
        row = await self.fetchone("SELECT premium_until FROM subscriptions WHERE user_id = ?", (user_id,))
        return row[0] if row else None

    async def is_premium(self, user_id: int) -> bool:
        until = await self.premium_until(user_id)
        return bool(until and until > ts())

    async def set_premium_until(self, user_id: int, until: str) -> None:
        await self.run(
            """
            INSERT INTO subscriptions (user_id, premium_until) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET premium_until = excluded.premium_until
            """,
            (user_id, until),
        )

    async def extend_premium(self, user_id: int, days: int) -> str:
        current = await self.premium_until(user_id)
        base = parse_ts(current) if current and current > ts() else now()
        until = ts(base + timedelta(days=days))
        await self.set_premium_until(user_id, until)
        return until

    async def add_payment(self, charge_id: str, user_id: int, amount: int, days: int) -> bool:
        try:
            await self.run(
                "INSERT INTO payments (charge_id, user_id, amount, days, created_at) VALUES (?, ?, ?, ?, ?)",
                (charge_id, user_id, amount, days, ts()),
            )
            return True
        except aiosqlite.IntegrityError:  # повторное уведомление об уже учтённой оплате
            return False

    async def get_payment(self, charge_id: str):
        return await self.fetchone("SELECT * FROM payments WHERE charge_id = ?", (charge_id,))

    async def mark_refunded(self, charge_id: str) -> None:
        await self.run("UPDATE payments SET refunded = 1 WHERE charge_id = ?", (charge_id,))

    async def premium_expiring_soon(self, within_days: int = 3) -> list:
        return await self.fetchall(
            """
            SELECT user_id, premium_until FROM subscriptions
            WHERE premium_until > ? AND premium_until <= ?
              AND (reminded_for IS NULL OR reminded_for != premium_until)
            """,
            (ts(), ts(now() + timedelta(days=within_days))),
        )

    async def premium_just_expired(self) -> list:
        return await self.fetchall(
            """
            SELECT user_id, premium_until FROM subscriptions
            WHERE premium_until <= ? AND (expired_notified_for IS NULL OR expired_notified_for != premium_until)
            """,
            (ts(),),
        )

    async def mark_reminded(self, user_id: int, column: str) -> None:
        if column not in ("reminded_for", "expired_notified_for"):
            raise ValueError(column)
        await self.run(f"UPDATE subscriptions SET {column} = premium_until WHERE user_id = ?", (user_id,))

    async def admin_summary(self) -> dict:
        owners = await self.fetchone("SELECT COUNT(DISTINCT user_id) FROM business_connections WHERE is_enabled = 1")
        premium = await self.fetchone("SELECT COUNT(*) FROM subscriptions WHERE premium_until > ?", (ts(),))
        msgs = await self.fetchone("SELECT COUNT(*), COALESCE(SUM(media_size), 0) FROM messages")
        stars = await self.fetchone("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE refunded = 0")
        return {"owners": owners[0], "premium": premium[0], "messages": msgs[0], "media": msgs[1], "stars": stars[0]}

    # ── служебное ──
    async def get_meta(self, key: str) -> Optional[str]:
        row = await self.fetchone("SELECT value FROM meta WHERE key = ?", (key,))
        return row[0] if row else None

    async def set_meta(self, key: str, value: str) -> None:
        await self.run("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))


db = Database(DB_PATH)

# ════════════════════════════ Владельцы и доступ ════════════════════════════


@dataclass
class Owner:
    user_id: int   # кто подключил бота
    chat_id: int   # куда ему писать


async def save_connection(conn: BusinessConnection) -> None:
    await db.save_connection(conn.id, conn.user.id, user_label(conn.user), conn.user_chat_id, conn.is_enabled)
    logger.info("Подключение %s: %s (%s)", "включено" if conn.is_enabled else "отключено",
                user_label(conn.user), conn.user.id)


async def resolve_owner(bot: Bot, conn_id: Optional[str]) -> Optional[Owner]:
    """Находит владельца подключения. Если его нет в базе — спрашивает у Telegram."""
    if not conn_id:
        return None
    row = await db.get_connection(conn_id)
    if row is None:
        try:
            await save_connection(await bot.get_business_connection(business_connection_id=conn_id))
            row = await db.get_connection(conn_id)
        except Exception as e:
            logger.warning("Не удалось получить подключение %s: %s", conn_id, e)
            return None
    return Owner(row["user_id"], row["user_chat_id"] or row["user_id"]) if row else None


async def is_allowed(user_id: int) -> bool:
    return user_id in ADMIN_IDS or await db.is_owner(user_id)


def can_access(row, user_id: int) -> bool:
    return row is not None and (row["owner_id"] == user_id or user_id in ADMIN_IDS)


# ════════════════════════════ Медиа ════════════════════════════


@dataclass
class Extracted:
    text: str
    media_type: Optional[str] = None
    media_obj: Any = None
    ext: str = ""


def extract(m: Message) -> Extracted:
    cap = m.text or m.caption or ""
    if m.photo:
        return Extracted(cap or "[Фото]", "photo", m.photo[-1], ".jpg")
    if m.animation:  # проверяем до document: у GIF заполнены оба поля
        return Extracted(cap or "[GIF]", "animation", m.animation, ".mp4")
    if m.video:
        return Extracted(cap or "[Видео]", "video", m.video, file_ext(m.video.file_name, ".mp4"))
    if m.video_note:
        return Extracted("[Видеосообщение (кружочек)]", "video_note", m.video_note, ".mp4")
    if m.voice:
        return Extracted(cap or "[Голосовое сообщение]", "voice", m.voice, ".ogg")
    if m.audio:
        name = m.audio.title or m.audio.file_name or "аудио"
        return Extracted(cap or f"[Аудио: {name}]", "audio", m.audio, file_ext(m.audio.file_name, ".mp3"))
    if m.document:
        name = m.document.file_name or "документ"
        return Extracted(cap or f"[Документ: {name}]", "document", m.document, file_ext(m.document.file_name, ".bin"))
    if m.sticker:
        ext = ".tgs" if m.sticker.is_animated else ".webm" if m.sticker.is_video else ".webp"
        return Extracted(f"[Стикер {m.sticker.emoji or ''}]".replace(" ]", "]"), "sticker", m.sticker, ext)
    if m.location:
        return Extracted(f"[Геопозиция: {m.location.latitude:.5f}, {m.location.longitude:.5f}]")
    if m.contact:
        return Extracted(f"[Контакт: {m.contact.first_name} {m.contact.phone_number}]")
    if m.poll:
        return Extracted(f"[Опрос: {m.poll.question}]")
    return Extracted(cap)


async def download_media(bot: Bot, conn_id: str, m: Message, obj: Any, ext: str) -> Optional[str]:
    size = getattr(obj, "file_size", None) or 0
    if size > MAX_DOWNLOAD_MB * 1024 * 1024:
        logger.info("Файл %s слишком большой для скачивания (%s), сохраняю только file_id", m.message_id, human_size(size))
        return None
    folder = os.path.join(MEDIA_DIR, safe_name(conn_id or "no_connection"))
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{m.chat.id}_{m.message_id}{ext}")
    try:
        await bot.download(obj, destination=path)
        return path
    except Exception as e:
        logger.warning("Не удалось скачать медиа сообщения %s: %s", m.message_id, e)
        return None


def remove_file(path: Optional[str]) -> int:
    if not path:
        return 0
    try:
        size = os.path.getsize(path)
        os.remove(path)
        return size
    except FileNotFoundError:
        return 0
    except OSError as e:
        logger.warning("Не удалось удалить %s: %s", path, e)
        return 0


# ════════════════════════════ Отправка уведомлений ════════════════════════════


async def _send_media(bot: Bot, chat_id: int, mtype: str, src, caption: Optional[str],
                      kb: Optional[InlineKeyboardMarkup], silent: bool) -> None:
    kw = {"chat_id": chat_id, "reply_markup": kb, "disable_notification": silent}
    senders = {
        "photo": bot.send_photo, "video": bot.send_video, "animation": bot.send_animation,
        "voice": bot.send_voice, "audio": bot.send_audio, "document": bot.send_document,
        "video_note": bot.send_video_note, "sticker": bot.send_sticker,
    }
    if mtype in NO_CAPTION_TYPES:
        await senders[mtype](**{mtype: src}, **kw)
    else:
        await senders[mtype](**{mtype: src}, caption=caption, **kw)


async def deliver(bot: Bot, chat_id: int, text: str, kb: Optional[InlineKeyboardMarkup] = None,
                  media_type: Optional[str] = None, media_path: Optional[str] = None,
                  file_id: Optional[str] = None, silent: bool = False) -> bool:
    """Отправляет уведомление: медиа (с подписью, если влезает) + текст. Сначала локальный файл, потом file_id."""
    if media_type in MEDIA_LABELS:
        sources = []
        if media_path and os.path.exists(media_path):
            sources.append(FSInputFile(media_path))
        if file_id:
            sources.append(file_id)
        with_caption = media_type not in NO_CAPTION_TYPES and len(text) <= 1024
        for src in sources:
            try:
                await _send_media(bot, chat_id, media_type, src,
                                  text if with_caption else None, kb if with_caption else None, silent)
                if with_caption:
                    return True
                break  # медиа ушло, текст отправим ниже отдельным сообщением
            except Exception as e:
                logger.warning("Не удалось отправить %s (%s): %s", media_type, type(src).__name__, e)
        else:
            text += "\n\n⚠️ <i>Медиафайл больше недоступен.</i>"
    try:
        await bot.send_message(chat_id, text, reply_markup=kb, disable_notification=silent,
                               link_preview_options=LinkPreviewOptions(is_disabled=True))
        return True
    except Exception as e:
        logger.error("Ошибка отправки пользователю %s: %s", chat_id, e)
        return False


async def notify_admins(bot: Bot, text: str) -> None:
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception as e:
            logger.warning("Не удалось написать админу %s: %s", admin_id, e)


def plural(n: int, one: str, few: str, many: str) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return one
    if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14):
        return few
    return many


def buy_button(text: Optional[str] = None) -> Btn:
    return Btn(text=text or f"⭐ Премиум — {PREMIUM_PRICE} ⭐ / {PREMIUM_DAYS} дн.", callback_data="buy")


def message_kb(ref_id: int, chat_id: int) -> InlineKeyboardMarkup:
    """Компактные кнопки под историей сообщения."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [Btn(text="🔇 Скрыть чат", callback_data=f"mc:{chat_id}"),
         Btn(text="🗑 Удалить", callback_data=f"d:{ref_id}")],
    ])


def free_kb(chat_id: int) -> InlineKeyboardMarkup:
    """Кнопки под уведомлением для бесплатного тарифа."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [buy_button("⭐ Узнать, что там было")],
    ])


def deleted_text(row, versions: list) -> str:
    head = [
        f"🗑 <b>Удалено сообщение</b> · #{row['id']}",
        "",
        f"👤 <b>Автор:</b> {esc(row['user_name'])}",
        f"💬 <b>Чат:</b> {esc(row['chat_title'])}",
        f"🕒 <b>Отправлено:</b> {esc(row['created_at'])}",
    ]
    if len(versions) > 1:
        n = len(versions) - 1
        head.append(f"✏️ <b>Изменялось:</b> {n} {plural(n, 'раз', 'раза', 'раз')}")
    head += ["", "📝 <b>Текст / подпись:</b>"]
    header = "\n".join(head)
    footer = f"\n\n📜 История: /h{row['id']}"
    last = versions[-1]["text"] if versions else "—"
    return f"{header}\n{fit(last, 3900 - len(header) - len(footer))}{footer}"


def edited_text(row, old: Optional[str], new: str) -> str:
    old_part = fit(old, 1700) if old is not None else "<i>не сохранено (сообщение пришло до Премиума)</i>"
    return (
        f"✏️ <b>Сообщение изменено</b> · #{row['id'] if row else '—'}\n\n"
        f"👤 <b>Автор:</b> {esc(row['user_name']) if row else '—'}\n"
        f"💬 <b>Чат:</b> {esc(row['chat_title']) if row else '—'}\n\n"
        f"<b>Было:</b>\n{old_part}\n\n"
        f"<b>Стало:</b>\n{fit(new, 1700)}"
        + (f"\n\n📜 История: /h{row['id']}" if row else "")
    )


def free_notice(kind: str, chat_title: str, count: int = 1) -> str:
    if kind == "deleted":
        what = "удалено сообщение" if count == 1 else f"удалено {count} {plural(count, 'сообщение', 'сообщения', 'сообщений')}"
        icon, hidden = "🗑", "Текст и медиа удалённых сообщений"
    else:
        what, icon, hidden = "изменено сообщение", "✏️", "Что было и что стало"
    return (
        f"{icon} <b>В чате «{esc(chat_title)}» {what}</b>\n"
        f"🕒 {esc(ts()[:16])}\n\n"
        f"🔒 {hidden} — в ⭐ Премиум ({PREMIUM_PRICE} ⭐ на {PREMIUM_DAYS} дней). Подробнее: /premium"
    )


def history_text(row, versions: list) -> str:
    status = f"🗑 удалено {esc(row['deleted_at'] or '')}".strip() if row["is_deleted"] else "✅ активно"
    lines = [
        f"📜 <b>История сообщения #{row['id']}</b>",
        f"<b>Статус:</b> {status}",
        f"<b>Автор:</b> {esc(row['user_name'])}",
        f"<b>Чат:</b> {esc(row['chat_title'])}",
        f"<b>Создано:</b> {esc(row['created_at'])}",
    ]
    if row["media_type"]:
        lines.append(f"<b>Медиа:</b> {MEDIA_LABELS.get(row['media_type'], row['media_type'])}")
    lines.append("")
    if not versions:
        lines.append("<i>Текст не сохранён (сообщение пришло на бесплатном тарифе).</i>")
    shown = versions[-15:]
    if len(versions) > len(shown):
        lines.append(f"<i>Показаны последние {len(shown)} из {len(versions)} версий</i>\n")
    budget = max(150, 3200 // max(1, len(shown)))
    for v in shown:
        lines.append(f"<b>Версия {v['version_num']}</b> · {esc(v['saved_at'])}\n{fit(v['text'], budget)}\n")
    return "\n".join(lines)


# ════════════════════════════ Бизнес-события ════════════════════════════


@router.business_connection()
async def on_business_connection(conn: BusinessConnection, bot: Bot):
    await save_connection(conn)
    if conn.is_enabled:
        premium = await db.is_premium(conn.user.id)
        tariff = ("⭐ У вас Премиум — сохраняю текст, правки и медиа."
                  if premium else
                  "🆓 Бесплатный тариф: сообщу, что в чате удалили или изменили сообщение.\n"
                  f"⭐ Премиум ({PREMIUM_PRICE} ⭐ / {PREMIUM_DAYS} дн.) — покажу сам текст, медиа и историю правок: /premium")
        text = (f"✅ <b>Бот подключён к вашему аккаунту.</b>\n\n{tariff}\n\n"
                f"🗓 Данные хранятся {RETENTION_DAYS} дней, затем удаляются автоматически.\n"
                "Настройки: /settings · Конфиденциальность: /privacy")
    else:
        text = ("⛔ <b>Бот отключён от вашего аккаунта.</b>\n\n"
                f"Новые сообщения больше не сохраняются. Сохранённое удалится через {RETENTION_DAYS} дней "
                "или сразу — командой /delete_my_data.")
    try:
        await bot.send_message(conn.user_chat_id, text)
    except Exception as e:
        logger.info("Не удалось написать владельцу %s (возможно, он не нажал /start): %s", conn.user.id, e)


@router.business_message()
async def on_business_message(message: Message, bot: Bot):
    conn_id = message.business_connection_id or ""
    owner = await resolve_owner(bot, conn_id)
    if owner and await db.is_muted(owner.user_id, message.chat.id):
        return
    premium = bool(owner) and await db.is_premium(owner.user_id)
    common = dict(
        conn_id=conn_id, chat_id=message.chat.id, message_id=message.message_id,
        user_id=message.from_user.id if message.from_user else 0,
        user_name=user_label(message.from_user), chat_title=chat_label(message.chat),
    )
    if not premium:
        # Бесплатно: только факт сообщения (кто и когда) — без текста и медиа.
        # Нужно, чтобы не присылать владельцу уведомления о его собственных удалениях.
        await db.save_message(**common)
        return

    ex = extract(message)
    if not ex.text and not ex.media_type:
        return
    media_path, media_size = None, None
    if ex.media_obj:
        if await db.media_usage(owner.user_id) < MEDIA_QUOTA_MB * 1024 * 1024:
            media_path = await download_media(bot, conn_id, message, ex.media_obj, ex.ext)
            if media_path:
                media_size = os.path.getsize(media_path)
        else:
            logger.info("Владелец %s превысил лимит медиа (%s МБ) — сохраняю только file_id", owner.user_id, MEDIA_QUOTA_MB)
    ref_id = await db.save_message(
        **common, text=ex.text, media_type=ex.media_type, media_path=media_path,
        file_id=ex.media_obj.file_id if ex.media_obj else None, media_size=media_size,
    )
    if ref_id:
        logger.info("Сохранено #%s (%s) из «%s»", ref_id, ex.media_type or "текст", common["chat_title"])


@router.edited_business_message()
async def on_edited_business_message(message: Message, bot: Bot):
    conn_id = message.business_connection_id or ""
    owner = await resolve_owner(bot, conn_id)
    if not owner or await db.is_muted(owner.user_id, message.chat.id):
        return
    premium = await db.is_premium(owner.user_id)
    row = await db.find_message(conn_id, message.chat.id, message.message_id)
    new_text = message.text or message.caption or "[Медиа без подписи]"

    old_text: Optional[str] = None
    if premium and row:
        versions = await db.versions(row["id"])
        old_text = versions[-1]["text"] if versions else None
        if new_text == old_text:
            return
        await db.add_version(row["id"], new_text)

    if message.from_user and message.from_user.id == owner.user_id:  # свои правки не присылаем
        return
    st = await db.get_settings(owner.user_id)
    if not st["notify_edited"]:
        return
    silent = bool(st["quiet_night"]) and is_quiet_time()
    if premium:
        await deliver(bot, owner.chat_id, edited_text(row, old_text, new_text), silent=silent)
    else:
        await deliver(bot, owner.chat_id, free_notice("edited", chat_label(message.chat)),
                      free_kb(message.chat.id), silent=silent)


@router.deleted_business_messages()
async def on_deleted_business_messages(event: BusinessMessagesDeleted, bot: Bot):
    conn_id = event.business_connection_id
    owner = await resolve_owner(bot, conn_id)
    if owner is None:
        # Владелец неизвестен — никому не отправляем
        logger.warning("Удаление в чате %s: владелец подключения %s не найден", event.chat.id, conn_id)
        return
    if await db.is_muted(owner.user_id, event.chat.id):
        return
    st = await db.get_settings(owner.user_id)
    silent = bool(st["quiet_night"]) and is_quiet_time()
    premium = await db.is_premium(owner.user_id)
    title = chat_label(event.chat)
    free_count = 0

    for msg_id in event.message_ids:
        row = await db.find_message(conn_id, event.chat.id, msg_id)
        if row:
            await db.mark_deleted(row["id"])
            if row["user_id"] == owner.user_id:  # владелец удалил своё сообщение — не шумим
                continue
        if not st["notify_deleted"]:
            continue
        if not premium:
            free_count += 1
            continue
        versions = await db.versions(row["id"]) if row else []
        if not versions:
            text = (f"🗑 <b>Удалено сообщение</b>\n\n💬 <b>Чат:</b> {esc(title)}\n"
                    "ℹ️ Содержимое не сохранено (сообщение пришло до подключения бота или до Премиума).")
            await deliver(bot, owner.chat_id, text, silent=silent)
            continue
        await deliver(bot, owner.chat_id, deleted_text(row, versions), None,
                      row["media_type"], row["media_path"], row["file_id"], silent=silent)

    if free_count:  # бесплатно — одно уведомление на всю пачку удалений
        await deliver(bot, owner.chat_id, free_notice("deleted", title, free_count), free_kb(event.chat.id), silent=silent)


# ════════════════════════════ Команды: общие ════════════════════════════

HELP_OWNER = (
    "👋 <b>Spy Chat Bot</b> — следит за вашими бизнес-чатами.\n\n"
    "/settings — уведомления, тихая ночь, скрытые чаты\n"
    "/premium — тариф и покупка Премиум\n"
    "/privacy — какие данные хранятся\n"
    "/delete_my_data — удалить все мои данные\n\n"
    "<b>⭐ Премиум:</b>\n"
    "/search <i>текст</i> — поиск по архиву\n"
    "/export — выгрузить переписку в файл\n"
    "/stats — статистика\n"
    "/history <i>ID</i> — все версии сообщения"
)

ADMIN_HELP = (
    "\n\n🛠 <b>Админ:</b>\n"
    "/admin — сводка\n"
    "/backup — бэкапы базы (скачать / сделать сейчас)\n"
    "/givepremium <i>user_id дни</i> — выдать Премиум\n"
    "/refund <i>user_id charge_id</i> — вернуть звёзды"
)


@router.message(CommandStart())
async def cmd_start(message: Message, bot: Bot):
    if await is_allowed(message.from_user.id):
        await message.answer(HELP_OWNER + (ADMIN_HELP if message.from_user.id in ADMIN_IDS else ""))
        return
    me = await bot.me()
    await message.answer(
        "👋 Этот бот следит за вашими личными чатами и сообщает, когда собеседник <b>удалил или изменил</b> сообщение.\n\n"
        "🆓 <b>Бесплатно</b> — уведомление о том, что в чате что-то удалили или изменили.\n"
        f"⭐ <b>Премиум</b> ({PREMIUM_PRICE} ⭐ / {PREMIUM_DAYS} дней) — сам текст, фото, видео, голосовые, "
        "история правок, поиск и выгрузка переписки.\n\n"
        "<b>Как подключить:</b>\n"
        "1. Нужен Telegram Premium (это требование Telegram для бизнес-ботов).\n"
        "2. Настройки → Telegram для бизнеса → Чат-боты.\n"
        f"3. Введите @{esc(me.username)} и разрешите доступ к сообщениям.\n\n"
        f"🗓 Данные хранятся {RETENTION_DAYS} дней. Подробнее: /privacy"
    )


@router.message(Command("privacy"))
async def cmd_privacy(message: Message):
    await message.answer(
        "🔐 <b>Какие данные хранит бот</b>\n\n"
        "🆓 <b>Бесплатный тариф:</b> только служебные данные о сообщениях — ID, автор и время. "
        "Текст, правки и медиа <b>не сохраняются</b>.\n"
        "⭐ <b>Премиум:</b> текст, правки и медиа сообщений в подключённых чатах.\n\n"
        f"🗓 Всё автоматически удаляется через <b>{RETENTION_DAYS} дней</b>.\n"
        "🔇 Скрытые чаты (/settings) не сохраняются совсем.\n"
        "🗑 /delete_my_data — удалить все ваши данные прямо сейчас.\n"
        "👁 Архив видите только вы. Данные не передаются третьим лицам.\n\n"
        "Чтобы бот перестал получать сообщения — отключите его: "
        "Настройки → Telegram для бизнеса → Чат-боты."
    )


@router.message(Command("paysupport"))
async def cmd_paysupport(message: Message):
    contact = f"напишите {esc(SUPPORT_CONTACT)}" if SUPPORT_CONTACT else "ответьте на это сообщение — администратор свяжется с вами"
    await message.answer(
        "💬 <b>Поддержка по оплате</b>\n\n"
        f"Если с оплатой Премиум что-то пошло не так — {contact}. "
        "Укажите дату покупки. Возврат звёзд возможен, если Премиум не работал по нашей вине."
    )


async def guard(message: Message) -> bool:
    if await is_allowed(message.from_user.id):
        return True
    await message.answer("Сначала подключите бота к аккаунту — подробности в /start.")
    return False


async def premium_guard(message: Message) -> bool:
    if not await guard(message):
        return False
    uid = message.from_user.id
    if uid in ADMIN_IDS or await db.is_premium(uid):
        return True
    await message.answer(
        "🔒 Эта функция доступна в ⭐ <b>Премиум</b>.\n\n"
        "Премиум сохраняет текст, медиа и правки сообщений — по ним работают поиск, выгрузка и статистика.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[buy_button()]]),
    )
    return False


@router.message(Command("settings"))
async def cmd_settings(message: Message):
    if not await guard(message):
        return
    text, kb = await settings_view(message.from_user.id)
    await message.answer(text, reply_markup=kb)


@router.message(Command("delete_my_data"))
async def cmd_delete_my_data(message: Message):
    if not await guard(message):
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        Btn(text="🗑 Да, удалить всё", callback_data="dd:yes"),
        Btn(text="↩️ Отмена", callback_data="dd:no"),
    ]])
    await message.answer(
        "⚠️ <b>Удалить все ваши данные?</b>\n\n"
        "Будут удалены все сохранённые сообщения, правки, медиа, настройки и скрытые чаты. "
        "Восстановить их будет нельзя.\nПодписка Премиум при этом сохранится.",
        reply_markup=kb,
    )


# ════════════════════════════ Команды: Премиум-функции ════════════════════════════


@router.message(Command("history"))
@router.message(Command(re.compile(r"h(\d+)$")))
async def cmd_history(message: Message, command: CommandObject):
    if not await premium_guard(message):
        return
    if command.regexp_match:
        arg = command.regexp_match.group(1)
    else:
        arg = (command.args or "").strip().lstrip("#")
    if not arg.isdigit():
        await message.answer("ℹ️ Использование: <code>/history 123</code> — или нажмите /h123 в уведомлении.")
        return
    row = await db.get_message(int(arg))
    if not can_access(row, message.from_user.id):
        await message.answer("❌ Сообщение не найдено в вашем архиве.")
        return
    await message.answer(history_text(row, await db.versions(row["id"])),
                         reply_markup=message_kb(row["id"], row["chat_id"]))


@router.message(Command("search"))
async def cmd_search(message: Message, command: CommandObject):
    if not await premium_guard(message):
        return
    query = (command.args or "").strip()
    if len(query) < 2:
        await message.answer("🔎 Использование: <code>/search слово</code> (минимум 2 символа)")
        return
    rows = await db.search(message.from_user.id, query)
    if not rows:
        await message.answer(f"🔎 По запросу «{esc(query)}» ничего не найдено.")
        return
    lines = [f"🔎 <b>Найдено:</b> {len(rows)}{' (последние 20)' if len(rows) == 20 else ''}\n"]
    for r in rows:
        mark = " 🗑" if r["is_deleted"] else ""
        lines.append(
            f"<b>#{r['id']}</b> · {esc(r['created_at'][:16])}{mark}\n"
            f"💬 {fit(r['chat_title'], 40)} — 👤 {fit(r['user_name'], 40)}\n"
            f"{fit(r['text'], 150)}\n"
        )
    lines.append("Подробнее: <code>/history ID</code>")
    await message.answer("\n".join(lines)[:4096])


@router.message(Command("export"))
async def cmd_export(message: Message):
    if not await premium_guard(message):
        return
    chats = await db.owner_chats(message.from_user.id)
    if not chats:
        await message.answer("📭 В архиве пока нет переписок.")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [Btn(text=f"{c['title'][:40]} ({c['cnt']})", callback_data=f"e:{c['chat_id']}")] for c in chats
    ])
    await message.answer("📤 <b>Выберите переписку для выгрузки</b> (HTML-файл, открывается в браузере):", reply_markup=kb)


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if not await premium_guard(message):
        return
    uid = message.from_user.id
    s = await db.stats(uid)
    if not s["total"]:
        await message.answer("📭 В архиве пока нет сообщений.")
        return
    usage = await db.media_usage(uid)
    lines = [
        "📊 <b>Статистика архива</b>\n",
        f"💬 Сообщений: <b>{s['total']}</b> в {s['chats']} чатах",
        f"🗑 Удалено: <b>{s['deleted']}</b>",
        f"✏️ Изменено: <b>{s['edited']}</b>",
        f"📎 С медиа: <b>{s['media']}</b> · занято {human_size(usage)} из {MEDIA_QUOTA_MB} МБ",
        f"📅 Самое старое сообщение: {esc((s['since'] or '')[:10])} (храним {RETENTION_DAYS} дней)",
    ]
    if s["deleters"]:
        lines.append("\n<b>Чаще всего удаляют:</b>")
        lines += [f"{i}. {fit(d['name'], 50)} — {d['n']}" for i, d in enumerate(s["deleters"], 1)]
    await message.answer("\n".join(lines))


# ════════════════════════════ Премиум и оплата Stars ════════════════════════════


async def premium_view(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    until = await db.premium_until(user_id)
    active = bool(until and until > ts())
    features = (
        "<b>Бесплатно:</b>\n"
        "• уведомление, что в чате удалили или изменили сообщение\n\n"
        "<b>⭐ Премиум:</b>\n"
        "• текст удалённых сообщений\n"
        "• фото, видео, голосовые, кружочки, документы, стикеры\n"
        "• что было и что стало при редактировании\n"
        "• поиск /search, выгрузка /export, статистика /stats\n"
        "• история всех версий сообщения\n\n"
        "<i>Премиум сохраняет сообщения, пришедшие после покупки.</i>"
    )
    if active:
        text = f"⭐ <b>Премиум активен до {esc(until[:16])}</b>\n\n{features}"
        btn = buy_button(f"Продлить на {PREMIUM_DAYS} дней — {PREMIUM_PRICE} ⭐")
    else:
        text = f"⭐ <b>Премиум — {PREMIUM_PRICE} ⭐ на {PREMIUM_DAYS} дней</b>\n\n{features}"
        btn = buy_button(f"Купить Премиум — {PREMIUM_PRICE} ⭐")
    return text, InlineKeyboardMarkup(inline_keyboard=[[btn]])


async def send_invoice(bot: Bot, chat_id: int, user_id: int) -> None:
    await bot.send_invoice(
        chat_id=chat_id,
        title=f"Премиум на {PREMIUM_DAYS} дней",
        description="Текст и медиа удалённых сообщений, история правок, поиск, выгрузка и статистика.",
        payload=f"premium:{PREMIUM_DAYS}:{user_id}",
        currency="XTR",
        prices=[LabeledPrice(label=f"Премиум {PREMIUM_DAYS} дней", amount=PREMIUM_PRICE)],
    )


@router.message(Command("premium", "buy"))
async def cmd_premium(message: Message):
    text, kb = await premium_view(message.from_user.id)
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data == "buy")
async def cb_buy(cb: CallbackQuery, bot: Bot):
    await send_invoice(bot, cb.from_user.id, cb.from_user.id)
    await cb.answer()


@router.callback_query(F.data == "prem")
async def cb_premium_info(cb: CallbackQuery, bot: Bot):
    text, kb = await premium_view(cb.from_user.id)
    await bot.send_message(cb.from_user.id, text, reply_markup=kb)
    await cb.answer()


@router.pre_checkout_query()
async def on_pre_checkout(query: PreCheckoutQuery):
    if query.currency != "XTR" or not query.invoice_payload.startswith("premium:"):
        await query.answer(ok=False, error_message="Неизвестный товар. Откройте /premium и попробуйте снова.")
        return
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def on_successful_payment(message: Message, bot: Bot):
    sp = message.successful_payment
    try:
        days = int(sp.invoice_payload.split(":")[1])
    except (IndexError, ValueError):
        days = PREMIUM_DAYS
    uid = message.from_user.id
    if not await db.add_payment(sp.telegram_payment_charge_id, uid, sp.total_amount, days):
        return  # эта оплата уже учтена
    until = await db.extend_premium(uid, days)
    await message.answer(
        f"🎉 <b>Спасибо! Премиум активен до {esc(until[:16])}.</b>\n\n"
        "Теперь я сохраняю текст, правки и медиа новых сообщений и покажу их, если собеседник что-то удалит.\n"
        "Попробуйте /search, /export и /stats."
    )
    await notify_admins(
        bot,
        f"💰 <b>Оплата Премиум</b>\n👤 {esc(user_label(message.from_user))} (<code>{uid}</code>)\n"
        f"⭐ {sp.total_amount} · {days} дн. · до {esc(until[:16])}\n"
        f"charge_id: <code>{esc(sp.telegram_payment_charge_id)}</code>",
    )


# ════════════════════════════ Команды: админ ════════════════════════════


@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    s = await db.admin_summary()
    db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
    await message.answer(
        "🛠 <b>Сводка</b>\n\n"
        f"👥 Активных подключений: <b>{s['owners']}</b>\n"
        f"⭐ С Премиум: <b>{s['premium']}</b>\n"
        f"💬 Сообщений в базе: <b>{s['messages']}</b>\n"
        f"📎 Медиа: {human_size(s['media'])} · база: {human_size(db_size)}\n"
        f"💰 Получено звёзд (без возвратов): <b>{s['stars']}</b>"
        + ADMIN_HELP
    )


def list_backups() -> list[str]:
    try:
        names = [n for n in os.listdir(BACKUP_DIR) if re.fullmatch(r"backup_[\d\-_]+\.db\.gz", n)]
    except FileNotFoundError:
        return []
    return sorted(names, reverse=True)


def backups_view() -> tuple[str, InlineKeyboardMarkup]:
    names = list_backups()
    rows = []
    for n in names:
        size = os.path.getsize(os.path.join(BACKUP_DIR, n))
        stamp = n[len("backup_"):-len(".db.gz")]  # 2026-11-18_0400
        label = f"💾 {stamp[8:10]}.{stamp[5:7]} {stamp[11:13]}:{stamp[13:15]} · {human_size(size)}"
        rows.append([Btn(text=label, callback_data=f"bk:{n}")])
    rows.append([Btn(text="➕ Сделать бэкап сейчас", callback_data="bk:new")])
    text = (f"💾 <b>Бэкапы базы</b>\n\nДелаются автоматически каждый день в {BACKUP_HOUR}:00 и хранятся на сервере "
            f"(последние {BACKUP_KEEP}). Нажмите, чтобы скачать.\n<i>Медиафайлы в бэкап не входят.</i>")
    if not names:
        text += "\n\nПока бэкапов нет."
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(Command("backup"))
async def cmd_backup(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    text, kb = backups_view()
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("bk:"))
async def cb_backup(cb: CallbackQuery, bot: Bot):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("Нет доступа", show_alert=True)
        return
    name = cb.data[3:]
    if name == "new":
        await cb.answer("⏳ Делаю бэкап…")
        await make_backup()
        await edit_cb(cb, *backups_view())
        return
    if name not in list_backups():  # защита от подмены пути
        await cb.answer("Бэкап не найден", show_alert=True)
        return
    path = os.path.join(BACKUP_DIR, name)
    if os.path.getsize(path) > 49 * 1024 * 1024:
        await cb.answer("Файл больше 50 МБ — Telegram не даст отправить. Скачайте через «Файлы» на хостинге.", show_alert=True)
        return
    await cb.answer("📤 Отправляю…")
    await bot.send_document(cb.from_user.id, FSInputFile(path, filename=name),
                            caption=f"💾 <b>Бэкап базы</b> · {human_size(os.path.getsize(path))}")


@router.message(Command("givepremium"))
async def cmd_givepremium(message: Message, command: CommandObject, bot: Bot):
    if message.from_user.id not in ADMIN_IDS:
        return
    parts = (command.args or "").split()
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].lstrip("-").isdigit():
        await message.answer("Использование: <code>/givepremium user_id дни</code> (отрицательные дни — убрать)")
        return
    uid, days = int(parts[0]), int(parts[1])
    if days > 0:
        until = await db.extend_premium(uid, days)
    else:
        current = await db.premium_until(uid) or ts()
        until = ts(parse_ts(current) + timedelta(days=days))
        await db.set_premium_until(uid, until)
    await message.answer(f"✅ Премиум пользователя <code>{uid}</code> теперь до {esc(until[:16])}")
    if days > 0:
        try:
            await bot.send_message(uid, f"🎁 Вам выдан ⭐ Премиум до {esc(until[:16])}!")
        except Exception:
            pass


@router.message(Command("refund"))
async def cmd_refund(message: Message, command: CommandObject, bot: Bot):
    if message.from_user.id not in ADMIN_IDS:
        return
    parts = (command.args or "").split()
    if len(parts) != 2 or not parts[0].isdigit():
        await message.answer("Использование: <code>/refund user_id charge_id</code> (charge_id — из уведомления об оплате)")
        return
    uid, charge_id = int(parts[0]), parts[1]
    payment = await db.get_payment(charge_id)
    if not payment or payment["user_id"] != uid:
        await message.answer("❌ Такая оплата не найдена.")
        return
    if payment["refunded"]:
        await message.answer("ℹ️ Эта оплата уже возвращена.")
        return
    try:
        await bot.refund_star_payment(user_id=uid, telegram_payment_charge_id=charge_id)
    except Exception as e:
        await message.answer(f"❌ Telegram отклонил возврат: <code>{esc(e)}</code>")
        return
    await db.mark_refunded(charge_id)
    current = await db.premium_until(uid)
    if current:
        await db.set_premium_until(uid, ts(parse_ts(current) - timedelta(days=payment["days"])))
    await message.answer(f"✅ Возвращено {payment['amount']} ⭐, Премиум уменьшен на {payment['days']} дн.")


@router.message(F.chat.type == "private")
async def fallback(message: Message, bot: Bot):
    await cmd_start(message, bot)


# ════════════════════════════ Настройки ════════════════════════════


async def settings_view(owner_id: int) -> tuple[str, InlineKeyboardMarkup]:
    st = await db.get_settings(owner_id)
    muted = await db.list_muted(owner_id)
    until = await db.premium_until(owner_id)
    tariff = f"⭐ Премиум до {esc(until[:16])}" if until and until > ts() else "🆓 Бесплатный"
    mark = lambda v: "✅" if v else "❌"  # noqa: E731
    text = (
        "⚙️ <b>Настройки</b>\n\n"
        f"Тариф: <b>{tariff}</b>\n\n"
        f"{mark(st['notify_deleted'])} Уведомлять об удалённых сообщениях\n"
        f"{mark(st['notify_edited'])} Уведомлять об изменённых сообщениях\n"
        f"{mark(st['quiet_night'])} Тихие уведомления ночью ({QUIET_START}:00–0{QUIET_END}:00)\n"
        f"🔇 Скрытых чатов: <b>{len(muted)}</b> (не сохраняются и не уведомляют)\n"
        f"🗓 Данные хранятся {RETENTION_DAYS} дней, затем удаляются автоматически.\n\n"
        "Нажмите кнопку, чтобы изменить."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [Btn(text=f"{mark(st['notify_deleted'])} Удалённые", callback_data="s:del"),
         Btn(text=f"{mark(st['notify_edited'])} Изменённые", callback_data="s:edit")],
        [Btn(text=f"{mark(st['quiet_night'])} Тихая ночь", callback_data="s:quiet"),
         Btn(text=f"🔇 Скрытые чаты ({len(muted)})", callback_data="s:muted")],
        [Btn(text="⭐ Премиум", callback_data="prem")],
    ])
    return text, kb


async def muted_view(owner_id: int) -> tuple[str, InlineKeyboardMarkup]:
    muted = await db.list_muted(owner_id)
    rows = [[Btn(text=f"🔊 {m['chat_title'][:40]}", callback_data=f"u:{m['chat_id']}")] for m in muted[:40]]
    rows.append([Btn(text="⬅️ Назад", callback_data="s:back")])
    text = ("🔇 <b>Скрытые чаты</b>\n\nНажмите на чат, чтобы снова его отслеживать."
            if muted else "🔇 <b>Скрытых чатов нет.</b>\n\nСкрыть чат можно кнопкой под любым уведомлением.")
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def edit_cb(cb: CallbackQuery, text: str, kb: Optional[InlineKeyboardMarkup]) -> None:
    if not isinstance(cb.message, Message):
        return
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest as e:
        if "not modified" not in str(e):
            raise


@router.callback_query(F.data.startswith("s:"))
async def cb_settings(cb: CallbackQuery):
    uid = cb.from_user.id
    if not await is_allowed(uid):
        await cb.answer("Нет доступа", show_alert=True)
        return
    action = cb.data[2:]
    if action == "muted":
        await edit_cb(cb, *await muted_view(uid))
        await cb.answer()
        return
    toggles = {"del": "notify_deleted", "edit": "notify_edited", "quiet": "quiet_night"}
    if action in toggles:
        st = await db.get_settings(uid)
        key = toggles[action]
        await db.set_setting(uid, key, 0 if st[key] else 1)
    await edit_cb(cb, *await settings_view(uid))
    await cb.answer()


@router.callback_query(F.data.startswith("u:"))
async def cb_unmute(cb: CallbackQuery):
    try:
        chat_id = int(cb.data[2:])
    except ValueError:
        await cb.answer()
        return
    await db.unmute(cb.from_user.id, chat_id)
    await edit_cb(cb, *await muted_view(cb.from_user.id))
    await cb.answer("🔊 Чат снова отслеживается")


@router.callback_query(F.data.startswith("mc:"))
async def cb_mute_chat(cb: CallbackQuery):
    try:
        chat_id = int(cb.data[3:])
    except ValueError:
        await cb.answer()
        return
    uid = cb.from_user.id
    if not await is_allowed(uid):
        await cb.answer("Нет доступа", show_alert=True)
        return
    title = await db.chat_title(uid, chat_id) or f"Чат {chat_id}"
    await db.mute(uid, chat_id, title)
    await cb.answer(f"🔇 Чат «{title[:60]}» больше не отслеживается.\nВернуть: /settings → Скрытые чаты",
                    show_alert=True)


@router.callback_query(F.data.startswith("dd:"))
async def cb_delete_my_data(cb: CallbackQuery):
    if cb.data != "dd:yes":
        if isinstance(cb.message, Message):
            await cb.message.edit_text("↩️ Удаление отменено.")
        await cb.answer()
        return
    count, paths = await db.delete_owner_data(cb.from_user.id)
    freed = sum(remove_file(p) for p in paths)
    if isinstance(cb.message, Message):
        await cb.message.edit_text(
            f"✅ <b>Ваши данные удалены.</b>\nСообщений: {count}, медиа: {human_size(freed)}.\n\n"
            "Чтобы бот больше ничего не получал — отключите его в Настройки → Telegram для бизнеса → Чат-боты."
        )
    await cb.answer()


# ════════════════════════════ Кнопки под уведомлениями (Премиум) ════════════════════════════


async def row_for_callback(cb: CallbackQuery):
    try:
        ref_id = int(cb.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await cb.answer()
        return None
    row = await db.get_message(ref_id)
    if not can_access(row, cb.from_user.id):
        await cb.answer("Сообщение не найдено в вашем архиве", show_alert=True)
        return None
    return row


@router.callback_query(F.data.startswith("h:"))
async def cb_history(cb: CallbackQuery, bot: Bot):
    row = await row_for_callback(cb)
    if row is None:
        return
    if cb.from_user.id not in ADMIN_IDS and not await db.is_premium(cb.from_user.id):
        await cb.answer("🔒 История доступна в Премиум — /premium", show_alert=True)
        return
    await bot.send_message(cb.from_user.id, history_text(row, await db.versions(row["id"])))
    await cb.answer()


@router.callback_query(F.data.startswith("d:"))
async def cb_delete_ask(cb: CallbackQuery):
    row = await row_for_callback(cb)
    if row is None:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        Btn(text="✅ Да, удалить", callback_data=f"dc:{row['id']}"),
        Btn(text="↩️ Отмена", callback_data=f"dn:{row['id']}"),
    ]])
    if isinstance(cb.message, Message):
        await cb.message.edit_reply_markup(reply_markup=kb)
    await cb.answer("Удалить сообщение и его медиа из архива?")


@router.callback_query(F.data.startswith("dc:"))
async def cb_delete_confirm(cb: CallbackQuery):
    row = await row_for_callback(cb)
    if row is None:
        return
    remove_file(await db.delete_message(row["id"]))
    if isinstance(cb.message, Message):
        await cb.message.edit_reply_markup(reply_markup=None)
    await cb.answer("🗑 Удалено из архива")


@router.callback_query(F.data.startswith("dn:"))
async def cb_delete_cancel(cb: CallbackQuery):
    row = await row_for_callback(cb)
    if row is None:
        return
    if isinstance(cb.message, Message):
        await cb.message.edit_reply_markup(reply_markup=message_kb(row["id"], row["chat_id"]))
    await cb.answer()


# ════════════════════════════ Экспорт ════════════════════════════

EXPORT_CSS = """
body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;background:#f4f5f7;color:#1c1d1f;max-width:820px;margin:0 auto;padding:24px}
h1{font-size:22px;margin:0 0 4px}.sub{color:#6b7280;margin-bottom:20px;font-size:14px}
.msg{background:#fff;border-radius:12px;padding:12px 14px;margin:10px 0;box-shadow:0 1px 2px rgba(0,0,0,.06)}
.msg.del{border-left:4px solid #e5484d}.meta{font-size:13px;color:#6b7280;margin-bottom:6px}
.meta b{color:#1c1d1f}.text{white-space:pre-wrap;word-wrap:break-word}
.tag{display:inline-block;font-size:11px;padding:1px 7px;border-radius:999px;margin-left:6px}
.t-del{background:#fde8e8;color:#c62828}.t-ed{background:#fff4d6;color:#8a6100}.t-md{background:#e6f0ff;color:#1d4ed8}
details{margin-top:8px;font-size:13px}summary{cursor:pointer;color:#6b7280}
.ver{border-top:1px dashed #e5e7eb;padding:6px 0}.ver span{color:#6b7280;font-size:12px}
"""


def build_export_html(title: str, rows: list, versions: dict) -> str:
    rows = [r for r in rows if versions.get(r["id"])]  # без текста (бесплатный период) — пропускаем
    deleted = sum(1 for r in rows if r["is_deleted"])
    items = []
    for r in rows:
        vs = versions[r["id"]]
        tags = ""
        if r["is_deleted"]:
            tags += '<span class="tag t-del">удалено</span>'
        if len(vs) > 1:
            tags += f'<span class="tag t-ed">изменено ×{len(vs) - 1}</span>'
        if r["media_type"]:
            tags += f'<span class="tag t-md">{esc(MEDIA_LABELS.get(r["media_type"], r["media_type"]))}</span>'
        item = (f'<div class="msg{" del" if r["is_deleted"] else ""}">'
                f'<div class="meta"><b>{esc(r["user_name"])}</b> · {esc(r["created_at"])}{tags}</div>'
                f'<div class="text">{esc(vs[-1]["text"])}</div>')
        if len(vs) > 1:
            item += f"<details><summary>Предыдущие версии ({len(vs) - 1})</summary>"
            item += "".join(
                f'<div class="ver"><span>Версия {v["version_num"]} · {esc(v["saved_at"])}</span>'
                f'<div class="text">{esc(v["text"])}</div></div>'
                for v in vs[:-1]
            )
            item += "</details>"
        items.append(item + "</div>")
    return (
        f'<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8">'
        f'<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{esc(title)}</title><style>{EXPORT_CSS}</style></head><body>"
        f"<h1>💬 {esc(title)}</h1>"
        f'<div class="sub">Выгружено {esc(ts())} · сообщений: {len(rows)} · удалено: {deleted} · '
        f"хранится {RETENTION_DAYS} дней</div>"
        + "".join(items) + "</body></html>"
    )


@router.callback_query(F.data.startswith("e:"))
async def cb_export(cb: CallbackQuery, bot: Bot):
    if cb.from_user.id not in ADMIN_IDS and not await db.is_premium(cb.from_user.id):
        await cb.answer("🔒 Выгрузка доступна в Премиум — /premium", show_alert=True)
        return
    try:
        chat_id = int(cb.data[2:])
    except ValueError:
        await cb.answer()
        return
    rows, versions = await db.export_chat(cb.from_user.id, chat_id)
    if not any(versions.get(r["id"]) for r in rows):
        await cb.answer("В этой переписке нет сохранённых текстов", show_alert=True)
        return
    await cb.answer("⏳ Готовлю файл…")
    title = rows[-1]["chat_title"]
    data = build_export_html(title, rows, versions).encode("utf-8")
    filename = f"chat_{safe_name(title, 30)}_{now():%Y-%m-%d}.html"
    await bot.send_document(
        cb.from_user.id, BufferedInputFile(data, filename=filename),
        caption=f"📤 Переписка: <b>{esc(title)}</b>",
    )


# ════════════════════════════ Обслуживание: очистка, бэкап, напоминания ════════════════════════════


async def cleanup_old_data() -> tuple[int, int]:
    """Удаляет ВСЕ данные (сообщения, правки, медиа) старше RETENTION_DAYS дней."""
    count, paths = await db.delete_older_than(ts(now() - timedelta(days=RETENTION_DAYS)))
    freed = sum(remove_file(p) for p in paths)
    logger.info("Очистка: удалено сообщений %s, освобождено %s", count, human_size(freed))
    return count, freed


def _gzip_file(src: str, dst: str) -> None:
    with open(src, "rb") as f_in, gzip.open(dst, "wb", compresslevel=6) as f_out:
        shutil.copyfileobj(f_in, f_out)


async def make_backup() -> Optional[str]:
    """Тихо сохраняет сжатую копию базы в BACKUP_DIR и удаляет старые (оставляет BACKUP_KEEP)."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    raw = os.path.join(BACKUP_DIR, "tmp_backup.db")
    final = os.path.join(BACKUP_DIR, f"backup_{now():%Y-%m-%d_%H%M}.db.gz")
    try:
        if os.path.exists(raw):
            os.remove(raw)
        await db.conn.commit()
        await db.conn.execute("VACUUM INTO ?", (raw,))
        await asyncio.to_thread(_gzip_file, raw, final)
    finally:
        try:
            os.remove(raw)
        except OSError:
            pass
    for old in list_backups()[BACKUP_KEEP:]:
        remove_file(os.path.join(BACKUP_DIR, old))
    logger.info("Бэкап сохранён: %s (%s)", final, human_size(os.path.getsize(final)))
    return final


async def premium_reminders(bot: Bot) -> None:
    kb = InlineKeyboardMarkup(inline_keyboard=[[buy_button(f"Продлить — {PREMIUM_PRICE} ⭐")]])
    for r in await db.premium_expiring_soon(3):
        try:
            await bot.send_message(
                r["user_id"],
                f"⏳ <b>Премиум закончится {esc(r['premium_until'][:16])}.</b>\n\n"
                "После этого я буду только сообщать об удалениях и правках — без текста и медиа.",
                reply_markup=kb,
            )
        except Exception as e:
            logger.info("Напоминание %s не доставлено: %s", r["user_id"], e)
        await db.mark_reminded(r["user_id"], "reminded_for")
    for r in await db.premium_just_expired():
        try:
            await bot.send_message(
                r["user_id"],
                "⌛ <b>Премиум закончился.</b>\n\n"
                "Теперь я только сообщаю об удалениях и правках, без текста. "
                f"Уже сохранённый архив снова откроется после продления (пока не прошло {RETENTION_DAYS} дней):",
                reply_markup=kb,
            )
        except Exception as e:
            logger.info("Уведомление об окончании %s не доставлено: %s", r["user_id"], e)
        await db.mark_reminded(r["user_id"], "expired_notified_for")


async def scheduler(bot: Bot) -> None:
    """Раз в минуту проверяет задачи: бэкап и очистка — раз в сутки, напоминания — раз в час днём."""
    last_reminder_hour = None
    while True:
        try:
            n = now()
            today = n.strftime("%Y-%m-%d")
            if n.hour >= BACKUP_HOUR and await db.get_meta("last_backup") != today:
                await db.set_meta("last_backup", today)
                try:
                    await make_backup()
                except Exception as e:  # пишем админам только если бэкап НЕ получился
                    logger.exception("Ошибка бэкапа")
                    await notify_admins(bot, f"⚠️ <b>Бэкап не сделан</b>\n<code>{esc(e)}</code>")
            if n.hour >= CLEANUP_HOUR and await db.get_meta("last_cleanup") != today:
                await db.set_meta("last_cleanup", today)
                await cleanup_old_data()
            hour_key = n.strftime("%Y-%m-%d %H")
            if 10 <= n.hour < 21 and hour_key != last_reminder_hour:
                last_reminder_hour = hour_key
                await premium_reminders(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Ошибка в планировщике")
        await asyncio.sleep(60)


# ════════════════════════════ Запуск, остановка, ошибки ════════════════════════════

_tasks: dict[str, asyncio.Task] = {}
_last_error_sent: dict[str, float] = {}


async def on_error(event: ErrorEvent, bot: Bot) -> bool:
    exc = event.exception
    logger.error("Ошибка при обработке апдейта: %r", exc, exc_info=exc)
    key = type(exc).__name__
    if time.monotonic() - _last_error_sent.get(key, 0) > 300:  # не чаще раза в 5 минут на тип ошибки
        _last_error_sent[key] = time.monotonic()
        await notify_admins(bot, f"⚠️ <b>Ошибка в боте</b>\n<code>{esc(key)}: {esc(str(exc)[:800])}</code>")
    return True


async def on_startup(bot: Bot) -> None:
    await bot.set_my_commands([
        BotCommand(command="settings", description="Настройки уведомлений"),
        BotCommand(command="premium", description=f"Премиум — {PREMIUM_PRICE} ⭐"),
        BotCommand(command="search", description="Поиск по архиву (Премиум)"),
        BotCommand(command="export", description="Выгрузить переписку (Премиум)"),
        BotCommand(command="stats", description="Статистика (Премиум)"),
        BotCommand(command="history", description="История сообщения по ID (Премиум)"),
        BotCommand(command="privacy", description="Какие данные хранятся"),
        BotCommand(command="delete_my_data", description="Удалить мои данные"),
        BotCommand(command="paysupport", description="Помощь с оплатой"),
        BotCommand(command="start", description="Помощь"),
    ])
    _tasks["scheduler"] = asyncio.create_task(scheduler(bot))
    me = await bot.me()
    logger.info("Бот @%s запущен", me.username)
    await notify_admins(bot, f"✅ <b>Бот @{esc(me.username)} запущен</b>\n🕒 {esc(ts())}")


async def on_shutdown(bot: Bot) -> None:
    task = _tasks.pop("scheduler", None)
    if task:
        task.cancel()
    await notify_admins(bot, f"⛔ <b>Бот остановлен</b>\n🕒 {esc(ts())}")
    await db.close()
    logger.info("Бот остановлен.")


async def main() -> None:
    if not BOT_TOKEN:
        print("Ошибка: переменная BOT_TOKEN не задана (файл .env или переменные окружения)")
        sys.exit(1)
    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS не задан — бэкапы и сообщения об ошибках некому присылать")
    os.makedirs(MEDIA_DIR, exist_ok=True)
    await db.connect()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    dp.errors.register(on_error)
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
