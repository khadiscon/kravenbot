#!/usr/bin/env python3
"""
KOL Campaign Manager Bot
Full-featured Telegram bot for managing KOL link-drop sessions,
queue enforcement, auto-moderation, and campaign tracking.
"""

import os
import re
import sqlite3
import logging
from datetime import datetime
from html import escape
from io import BytesIO
from urllib.parse import urlsplit, urlunsplit

from telegram import Update, ChatPermissions, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from dotenv import load_dotenv

load_dotenv()

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("KOLBot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set in .env")

DB_PATH = os.getenv("DB_PATH", "kol_bot.db")
DEFAULT_QUEUE_SIZE = int(os.getenv("DEFAULT_QUEUE_SIZE", 15))

TWITTER_RE = re.compile(
    r"https?://(www\.)?(twitter\.com|x\.com)/\S+", re.IGNORECASE
)
ANY_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
TAGALL_MAX_LEN = 4000
PRIVATE_MENU_PREFIX = "menu:"


# ─── DATABASE LAYER ───────────────────────────────────────────────────────────

def db():
    """Open a connection with row_factory set."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with db() as conn:
        conn.executescript("""
        -- Per-chat, per-user state
        CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER NOT NULL,
            chat_id     INTEGER NOT NULL,
            username    TEXT    DEFAULT '',
            full_name   TEXT    DEFAULT '',
            warnings    INTEGER DEFAULT 0,
            whitelisted INTEGER DEFAULT 0,
            banned      INTEGER DEFAULT 0,
            total_links INTEGER DEFAULT 0,
            points      INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, chat_id)
        );

        -- Every accepted link drop goes here
        CREATE TABLE IF NOT EXISTS link_queue (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id   INTEGER NOT NULL,
            thread_id INTEGER NOT NULL DEFAULT 0,
            user_id   INTEGER NOT NULL,
            username  TEXT    DEFAULT '',
            link      TEXT    NOT NULL,
            posted_at TEXT    DEFAULT (datetime('now'))
        );

        -- Per-topic bot configuration
        -- thread_id = 0 means no topic (regular group or general)
        CREATE TABLE IF NOT EXISTS topic_settings (
            chat_id        INTEGER NOT NULL,
            thread_id      INTEGER NOT NULL DEFAULT 0,
            queue_size     INTEGER DEFAULT 15,
            session_active INTEGER DEFAULT 0,
            points_per_link INTEGER DEFAULT 10,
            PRIMARY KEY (chat_id, thread_id)
        );

        -- Campaign definitions
        CREATE TABLE IF NOT EXISTS campaigns (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id     INTEGER NOT NULL,
            thread_id   INTEGER NOT NULL DEFAULT 0,
            name        TEXT    NOT NULL,
            description TEXT    DEFAULT '',
            target      INTEGER DEFAULT 100,
            reward      TEXT    DEFAULT 'TBA',
            deadline    TEXT    DEFAULT 'Open-ended',
            active      INTEGER DEFAULT 1,
            created_by  INTEGER,
            created_at  TEXT    DEFAULT (datetime('now'))
        );

        -- Per-campaign link submissions (separate from the drop queue)
        CREATE TABLE IF NOT EXISTS campaign_submissions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id  INTEGER NOT NULL,
            user_id      INTEGER NOT NULL,
            username     TEXT    DEFAULT '',
            link         TEXT    NOT NULL,
            verified     INTEGER DEFAULT 0,
            submitted_at TEXT    DEFAULT (datetime('now')),
            UNIQUE (campaign_id, link)
        );

        -- Reward payouts log
        CREATE TABLE IF NOT EXISTS rewards (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id    INTEGER NOT NULL,
            user_id    INTEGER NOT NULL,
            username   TEXT    DEFAULT '',
            amount     TEXT    NOT NULL,
            reason     TEXT    DEFAULT '',
            paid_at    TEXT    DEFAULT (datetime('now'))
        );

        -- Per-topic, per-command user permission flags (all off by default)
        -- thread_id = 0 means no topic (regular group or general)
        CREATE TABLE IF NOT EXISTS cmd_permissions (
            chat_id   INTEGER NOT NULL,
            thread_id INTEGER NOT NULL DEFAULT 0,
            command   TEXT    NOT NULL,
            enabled   INTEGER DEFAULT 0,
            PRIMARY KEY (chat_id, thread_id, command)
        );
        """)
    logger.info("Database initialised.")
    with db() as conn:
        # Migration: add thread_id column to cmd_permissions if upgrading from older version
        cols = [r[1] for r in conn.execute("PRAGMA table_info(cmd_permissions)").fetchall()]
        if "thread_id" not in cols:
            conn.execute("ALTER TABLE cmd_permissions ADD COLUMN thread_id INTEGER NOT NULL DEFAULT 0")
            conn.commit()
            logger.info("Migrated cmd_permissions: added thread_id column.")

        # Migration: add thread_id column to link_queue if upgrading from older version
        cols = [r[1] for r in conn.execute("PRAGMA table_info(link_queue)").fetchall()]
        if "thread_id" not in cols:
            conn.execute("ALTER TABLE link_queue ADD COLUMN thread_id INTEGER NOT NULL DEFAULT 0")
            conn.commit()
            logger.info("Migrated link_queue: added thread_id column.")

        # Migration: copy any legacy chat_settings rows into topic_settings(thread_id=0)
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        if "chat_settings" in tables:
            conn.execute("""
                INSERT OR IGNORE INTO topic_settings (
                    chat_id, thread_id, queue_size, session_active, points_per_link
                )
                SELECT chat_id, 0, queue_size, session_active, points_per_link
                FROM chat_settings
            """)
            conn.commit()

        # Migration: add thread_id column to campaigns if upgrading from older version
        cols = [r[1] for r in conn.execute("PRAGMA table_info(campaigns)").fetchall()]
        if "thread_id" not in cols:
            conn.execute("ALTER TABLE campaigns ADD COLUMN thread_id INTEGER NOT NULL DEFAULT 0")
            conn.commit()
            logger.info("Migrated campaigns: added thread_id column.")


# ─── DB HELPERS ───────────────────────────────────────────────────────────────

def upsert_user(conn, user_id: int, chat_id: int, username: str, full_name: str):
    conn.execute(
        "INSERT OR IGNORE INTO users (user_id, chat_id, username, full_name) VALUES (?,?,?,?)",
        (user_id, chat_id, username, full_name),
    )
    conn.execute(
        "UPDATE users SET username=?, full_name=? WHERE user_id=? AND chat_id=?",
        (username, full_name, user_id, chat_id),
    )


def fetch_user(conn, user_id: int, chat_id: int):
    return conn.execute(
        "SELECT * FROM users WHERE user_id=? AND chat_id=?", (user_id, chat_id)
    ).fetchone()


def fetch_settings(conn, chat_id: int, thread_id: int = 0) -> dict:
    row = conn.execute(
        "SELECT * FROM topic_settings WHERE chat_id=? AND thread_id=?",
        (chat_id, thread_id),
    ).fetchone()
    if not row:
        conn.execute("""
            INSERT OR IGNORE INTO topic_settings (
                chat_id, thread_id, queue_size, session_active, points_per_link
            ) VALUES (?,?,?,?,?)
        """, (chat_id, thread_id, DEFAULT_QUEUE_SIZE, 0, 10))
        conn.commit()
        return {
            "chat_id": chat_id,
            "thread_id": thread_id,
            "queue_size": DEFAULT_QUEUE_SIZE,
            "session_active": 0,
            "points_per_link": 10,
        }
    return dict(row)


def update_chat_settings(
    conn,
    chat_id: int,
    thread_id: int = 0,
    *,
    queue_size: int | None = None,
    session_active: int | None = None,
    points_per_link: int | None = None,
):
    """Update chat settings without resetting fields that were not provided."""
    current = fetch_settings(conn, chat_id, thread_id)
    conn.execute(
        """
        INSERT OR REPLACE INTO topic_settings (
            chat_id, thread_id, queue_size, session_active, points_per_link
        ) VALUES (?,?,?,?,?)
        """,
        (
            chat_id,
            thread_id,
            current["queue_size"] if queue_size is None else queue_size,
            current["session_active"] if session_active is None else session_active,
            current["points_per_link"] if points_per_link is None else points_per_link,
        ),
    )


def queue_progress(conn, chat_id: int, thread_id: int, user_id: int, queue_size: int):
    """
    Returns (count_after, can_post).
    count_after = how many UNIQUE other users posted after this user's last post.
    can_post    = True if count_after >= queue_size OR user has never posted.
    """
    last = conn.execute(
        """SELECT id FROM link_queue
           WHERE chat_id=? AND thread_id=? AND user_id=?
           ORDER BY id DESC LIMIT 1""",
        (chat_id, thread_id, user_id),
    ).fetchone()

    if not last:
        return (queue_size, True)  # Never posted — free to go

    count = conn.execute(
        """SELECT COUNT(DISTINCT user_id) FROM link_queue
           WHERE chat_id=? AND thread_id=? AND id > ? AND user_id != ?""",
        (chat_id, thread_id, last["id"], user_id),
    ).fetchone()[0]

    return (count, count >= queue_size)


def active_campaign(conn, chat_id: int, thread_id: int = 0):
    return conn.execute(
        """SELECT * FROM campaigns
           WHERE chat_id=? AND thread_id=? AND active=1
           ORDER BY id DESC LIMIT 1""",
        (chat_id, thread_id),
    ).fetchone()


def latest_campaign(conn, chat_id: int, thread_id: int = 0):
    return conn.execute(
        """SELECT * FROM campaigns
           WHERE chat_id=? AND thread_id=?
           ORDER BY id DESC LIMIT 1""",
        (chat_id, thread_id),
    ).fetchone()


def username_to_user(conn, username: str, chat_id: int):
    return conn.execute(
        "SELECT * FROM users WHERE username=? AND chat_id=?",
        (username.lstrip("@"), chat_id),
    ).fetchone()


def normalize_twitter_link(link: str) -> str:
    """Rewrite any supported Twitter/X URL into a canonical x.com form."""
    cleaned = link.strip()
    parts = urlsplit(cleaned)
    path = parts.path.rstrip("/") or "/"
    return urlunsplit(("https", "x.com", path, "", ""))


def find_existing_link(conn, chat_id: int, thread_id: int, normalized_link: str):
    """Return the first existing submission whose normalized link matches."""
    rows = conn.execute(
        "SELECT username, link FROM link_queue WHERE chat_id=? AND thread_id=? ORDER BY id ASC",
        (chat_id, thread_id),
    ).fetchall()
    for row in rows:
        if normalize_twitter_link(row["link"]) == normalized_link:
            return row
    return None


def build_tagall_mentions(rows) -> list[str]:
    """Build mention strings for tracked chat users."""
    mentions = []
    for row in rows:
        label = f"@{row['username']}" if row["username"] else row["full_name"] or f"user_{row['user_id']}"
        mentions.append(
            f'<a href="tg://user?id={row["user_id"]}">{escape(label)}</a>'
        )
    return mentions


def chunk_tagall_messages(mentions: list[str], intro: str = "") -> list[str]:
    """Split mention batches so each outgoing message stays under 4000 chars."""
    messages = []
    prefix = f"{escape(intro.strip())}\n\n" if intro.strip() else ""
    current = prefix

    for mention in mentions:
        separator = "" if current.endswith("\n\n") or not current else " "
        candidate = f"{current}{separator}{mention}"
        if len(candidate) > TAGALL_MAX_LEN:
            if current:
                messages.append(current)
            current = mention
            prefix = ""
        else:
            current = candidate

    if current:
        messages.append(current)

    return messages


def format_drop_announcement(tg_user, link: str, wait_count: int | None = None) -> str:
    """Format the bot's reposted drop message with a tagged user and queue reminder."""
    if tg_user.username:
        mention = escape(f"@{tg_user.username}")
    else:
        mention = (
            f'<a href="tg://user?id={tg_user.id}">{escape(tg_user.full_name or str(tg_user.id))}</a>'
        )
    message = f"🚀 New link from {mention}:\n\n{escape(link)}"
    if wait_count is not None:
        message += (
            f"\n\n⏳ {mention}, you must wait for {wait_count} more posts "
            "before your next link."
        )
    return message


def build_private_menu(section: str = "home") -> tuple[str, InlineKeyboardMarkup]:
    """Return private-chat menu text and inline keyboard."""
    common_rows = []

    if section == "home":
        text = (
            "KOL Campaign Manager\n\n"
            "Use this private chat as a control panel and reference.\n"
            "Group actions like sessions and campaigns still run inside your group/topic.\n\n"
            "Pick a section below."
        )
        rows = [
            [
                InlineKeyboardButton("User Commands", callback_data=f"{PRIVATE_MENU_PREFIX}user"),
                InlineKeyboardButton("Sessions", callback_data=f"{PRIVATE_MENU_PREFIX}sessions"),
            ],
            [
                InlineKeyboardButton("Campaigns", callback_data=f"{PRIVATE_MENU_PREFIX}campaigns"),
                InlineKeyboardButton("Admin Tools", callback_data=f"{PRIVATE_MENU_PREFIX}admin"),
            ],
        ]
        return text, InlineKeyboardMarkup(rows)

    if section == "user":
        text = (
            "*User Commands*\n\n"
            "/mystatus — show your queue progress in the current topic\n"
            "/leaderboard — show top posters in the group\n"
            "/campaignstatus — show the active campaign for the current topic\n"
            "/mycampaignstats — show your stats in the active topic campaign\n"
            "/stats — show group stats plus current topic session settings"
        )
    elif section == "sessions":
        text = (
            "*Session Commands*\n\n"
            "/startsession or /startsession15 — start a 15-link session in the current topic\n"
            "/startsession28 — start a 28-link session in the current topic\n"
            "/stopsession — stop the current topic session\n"
            "/setqueue [n] — manually change queue size for the current topic\n"
            "/setpoints [n] — change points per link for the current topic"
        )
    elif section == "campaigns":
        text = (
            "*Campaign Commands*\n\n"
            "/newcampaign Name | Description | Target | Reward | Deadline\n"
            "/endcampaign — end the active campaign in the current topic\n"
            "/exportlinks — download a text file with every submitted link in this topic campaign, so you can review them, verify work, or send the list to whoever is paying\n"
            "/verifysub @user — verify submissions in the current topic campaign\n"
            "/removesub @user [partial link] — remove a submission from the current topic campaign"
        )
    else:
        text = (
            "*Admin Tools*\n\n"
            "/warn, /ban, /unban, /unmute, /reset, /whitelist\n"
            "/tagall [message] — mention tracked users\n"
            "/enablecmd, /disablecmd, /cmdstatus — toggle user commands per topic\n"
            "/logpayout @user [amount] [reason] — save a record that you paid someone, how much you paid, and why you paid them\n"
            "/payouts — show the latest payout records, so you can quickly check who has already been paid and who has not"
        )

    common_rows = [
        [
            InlineKeyboardButton("Home", callback_data=f"{PRIVATE_MENU_PREFIX}home"),
            InlineKeyboardButton("Sessions", callback_data=f"{PRIVATE_MENU_PREFIX}sessions"),
        ],
        [
            InlineKeyboardButton("Campaigns", callback_data=f"{PRIVATE_MENU_PREFIX}campaigns"),
            InlineKeyboardButton("Admin Tools", callback_data=f"{PRIVATE_MENU_PREFIX}admin"),
        ],
    ]
    return text, InlineKeyboardMarkup(common_rows)


# Commands that are user-accessible but can be toggled per-chat by admins
USER_COMMANDS = {"mystatus", "leaderboard", "campaignstatus", "mycampaignstats", "stats"}


# ─── ADMIN CHECK ──────────────────────────────────────────────────────────────

async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        member = await context.bot.get_chat_member(
            update.effective_chat.id, update.effective_user.id
        )
        return member.status in ("administrator", "creator")
    except Exception:
        return False


def is_cmd_enabled(conn, chat_id: int, thread_id: int, command: str) -> bool:
    """Return True if the user command is enabled for this specific topic thread."""
    row = conn.execute(
        "SELECT enabled FROM cmd_permissions WHERE chat_id=? AND thread_id=? AND command=?",
        (chat_id, thread_id, command),
    ).fetchone()
    return bool(row and row["enabled"])


async def deny_user_cmd(update: Update, command: str):
    """
    Quote the user's command message, reply that it's not available in this topic,
    then delete their original message.
    """
    msg = update.message
    await msg.reply_text(
        f"⚠️ `/{command}` is not available in this topic.",
        parse_mode="Markdown",
    )
    try:
        await msg.delete()
    except Exception:
        pass


def parse_mention(update: Update) -> str | None:
    """Extract @username from entities in the message."""
    entities = update.message.entities or []
    for e in entities:
        if e.type == "mention":
            return update.message.text[e.offset + 1 : e.offset + e.length]
    # Also check text_mention (users with no username)
    return None



# ─── ESCALATION HELPER ────────────────────────────────────────────────────────
# warn 1-2 = notice only
# warn 3   = 24-hour mute
# warn 4   = 72-hour mute
# warn 5   = permanent ban

MUTE_DURATIONS = {3: 86400, 4: 259200}  # seconds: 24h, 72h

async def apply_escalation(context, conn, chat_id: int, user_id: int,
                            handle: str, warnings: int, extra: str = ""):
    """
    Apply the correct penalty for the current warning count and notify the group.
    conn must already have the updated warning count committed.
    """
    from datetime import timezone
    from telegram import ChatPermissions

    if warnings >= 5:
        # Permanent ban
        try:
            await context.bot.ban_chat_member(chat_id, user_id)
            conn.execute(
                "UPDATE users SET banned=1 WHERE user_id=? AND chat_id=?",
                (user_id, chat_id),
            )
            conn.commit()
            await context.bot.send_message(
                chat_id,
                f"⛔ {handle} has been banned after 5 warnings.",
            )
        except Exception as e:
            logger.warning(f"Ban failed for {handle}: {e}")
            await context.bot.send_message(
                chat_id,
                f"⛔ {handle} reached 5 warnings. Manual ban required — bot lacks permission.",
            )

    elif warnings in MUTE_DURATIONS:
        # Timed mute
        seconds = MUTE_DURATIONS[warnings]
        hours = seconds // 3600
        until = datetime.now(tz=timezone.utc).timestamp() + seconds
        try:
            await context.bot.restrict_chat_member(
                chat_id,
                user_id,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=int(until),
            )
            prefix = f"{extra}\n" if extra else ""
            await context.bot.send_message(
                chat_id,
                f"{prefix}⚠️ {handle} — warning {warnings}/5. "
                f"Muted for {hours} hours.",
            )
        except Exception as e:
            logger.warning(f"Mute failed for {handle}: {e}")
            await context.bot.send_message(
                chat_id,
                f"{extra}\n⚠️ {handle} — warning {warnings}/5. (Mute failed: {e})",
            )

    else:
        # Notice only (warnings 1-2)
        prefix = f"{extra}\n" if extra else ""
        await context.bot.send_message(
            chat_id,
            f"{prefix}⚠️ {handle} — warning {warnings}/5.",
        )


# ─── MESSAGE HANDLER ──────────────────────────────────────────────────────────

async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    if msg.chat.type not in ("group", "supergroup"):
        return

    text = (msg.text or msg.caption or "").strip()
    chat_id = msg.chat_id
    thread_id = msg.message_thread_id or 0
    tg_user = msg.from_user
    thread_kwargs = {"message_thread_id": msg.message_thread_id} if msg.message_thread_id else {}

    with db() as conn:
        settings = fetch_settings(conn, chat_id, thread_id)

        if not settings["session_active"]:
            return  # Session not running — ignore

        upsert_user(conn, tg_user.id, chat_id, tg_user.username or "", tg_user.full_name)
        user = fetch_user(conn, tg_user.id, chat_id)
        handle = f"@{tg_user.username}" if tg_user.username else tg_user.full_name

        twitter_matches = list(TWITTER_RE.finditer(text))
        valid_drop_message = (
            len(twitter_matches) == 1
            and twitter_matches[0].span() == (0, len(text))
        )

        # ── Only a single bare Twitter/X link is allowed during session ───
        if not valid_drop_message:
            try:
                await msg.delete()
            except Exception:
                pass
            await context.bot.send_message(
                chat_id,
                f"❌ {handle} — only a single Twitter/X link is allowed during link-drop sessions. "
                "Your message was removed.",
                **thread_kwargs,
            )
            return

        # ── Duplicate link check (applies to everyone, including whitelisted) ─
        candidate_link = normalize_twitter_link(twitter_matches[0].group(0))
        existing = find_existing_link(conn, chat_id, thread_id, candidate_link)

        if existing:
            try:
                await msg.delete()
            except Exception:
                pass
            original_poster = f"@{existing['username']}" if existing["username"] else "someone"
            await context.bot.send_message(
                chat_id,
                f"❌ {handle} — that link was already submitted by {original_poster}. "
                "Post a different link.",
                **thread_kwargs,
            )
            return

        # ── Whitelisted user ────────────────────────────────────────────────
        if user and user["whitelisted"]:
            conn.execute(
                "INSERT INTO link_queue (chat_id, thread_id, user_id, username, link) VALUES (?,?,?,?,?)",
                (chat_id, thread_id, tg_user.id, tg_user.username or "", candidate_link),
            )
            conn.execute(
                "UPDATE users SET total_links=total_links+1, points=points+? WHERE user_id=? AND chat_id=?",
                (settings["points_per_link"], tg_user.id, chat_id),
            )
            _maybe_record_campaign(conn, chat_id, thread_id, tg_user, candidate_link)
            conn.commit()
            await context.bot.send_message(
                chat_id,
                format_drop_announcement(tg_user, candidate_link),
                parse_mode="HTML",
                disable_web_page_preview=True,
                **thread_kwargs,
            )
            try:
                await msg.delete()
            except Exception:
                pass
            return

        # ── Queue check ─────────────────────────────────────────────────────
        queue_size = settings["queue_size"]
        count_after, can_post = queue_progress(conn, chat_id, thread_id, tg_user.id, queue_size)

        if not can_post:
            remaining = queue_size - count_after
            try:
                await msg.delete()
            except Exception:
                pass

            await context.bot.send_message(
                chat_id,
                f"⏳ {handle} posted too early.\n"
                f"Progress: {count_after}/{queue_size}\n"
                f"Still waiting for {remaining} more people before your next link.",
                **thread_kwargs,
            )
            return

        # ── Valid post ──────────────────────────────────────────────────────
        conn.execute(
            "INSERT INTO link_queue (chat_id, thread_id, user_id, username, link) VALUES (?,?,?,?,?)",
            (chat_id, thread_id, tg_user.id, tg_user.username or "", candidate_link),
        )
        conn.execute(
            "UPDATE users SET total_links=total_links+1, points=points+? WHERE user_id=? AND chat_id=?",
            (settings["points_per_link"], tg_user.id, chat_id),
        )
        _maybe_record_campaign(conn, chat_id, thread_id, tg_user, candidate_link)
        conn.commit()

        await context.bot.send_message(
            chat_id,
            format_drop_announcement(tg_user, candidate_link, queue_size),
            parse_mode="HTML",
            disable_web_page_preview=True,
            **thread_kwargs,
        )
        try:
            await msg.delete()
        except Exception:
            pass


def _maybe_record_campaign(conn, chat_id: int, thread_id: int, tg_user, link: str):
    """If there's an active campaign, log this link as a campaign submission."""
    camp = active_campaign(conn, chat_id, thread_id)
    if not camp:
        return
    try:
        conn.execute(
            """INSERT OR IGNORE INTO campaign_submissions
               (campaign_id, user_id, username, link)
               VALUES (?,?,?,?)""",
            (camp["id"], tg_user.id, tg_user.username or "", link),
        )
    except Exception:
        pass  # Duplicate link — already counted


# ─── USER COMMANDS ────────────────────────────────────────────────────────────

async def cmd_mystatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        if not is_cmd_enabled(conn, update.effective_chat.id, thread_id, "mystatus"):
            await deny_user_cmd(update, "mystatus")
            return

    msg = update.message
    chat_id = msg.chat_id
    tg_user = msg.from_user

    with db() as conn:
        settings = fetch_settings(conn, chat_id, thread_id)
        upsert_user(conn, tg_user.id, chat_id, tg_user.username or "", tg_user.full_name)
        user = fetch_user(conn, tg_user.id, chat_id)
        queue_size = settings["queue_size"]
        count_after, can_post = queue_progress(conn, chat_id, thread_id, tg_user.id, queue_size)

    if can_post:
        status_line = " You can drop your link right now!"
    else:
        remaining = queue_size - count_after
        status_line = f"⏳ Waiting for {remaining} more people.\n📊 Progress: {count_after}/{queue_size}"

    warnings = user["warnings"] if user else 0
    total = user["total_links"] if user else 0
    points = user["points"] if user else 0

    await msg.reply_text(
        f" *Your Status*\n\n"
        f"{status_line}\n\n"
        f"🔗 Total links posted: {total}\n"
        f" Points: {points}\n"
        f"⚠️ Warnings: {warnings}/5",
        parse_mode="Markdown",
    )


async def cmd_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        if not is_cmd_enabled(conn, chat_id, thread_id, "leaderboard"):
            await deny_user_cmd(update, "leaderboard")
            return
        rows = conn.execute(
            """SELECT username, full_name, total_links, points
               FROM users WHERE chat_id=? AND total_links > 0
               ORDER BY total_links DESC LIMIT 10""",
            (chat_id,),
        ).fetchall()

    if not rows:
        await update.message.reply_text("No links dropped yet. Be the first!")
        return

    medals = ["🥇", "🥈", "🥉"] + [f"{i + 1}." for i in range(3, 10)]
    lines = ["🏆 *All-Time Leaderboard*\n"]
    for i, row in enumerate(rows):
        name = f"@{row['username']}" if row["username"] else row["full_name"]
        lines.append(f"{medals[i]} {name} — {row['total_links']} links · {row['points']} pts")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_campaignstatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        if not is_cmd_enabled(conn, chat_id, thread_id, "campaignstatus"):
            await deny_user_cmd(update, "campaignstatus")
            return

        camp = active_campaign(conn, chat_id, thread_id)
        if not camp:
            await update.message.reply_text("No active campaign right now.")
            return

        total = conn.execute(
            "SELECT COUNT(*) FROM campaign_submissions WHERE campaign_id=?",
            (camp["id"],),
        ).fetchone()[0]

        top = conn.execute(
            """SELECT username, COUNT(*) as cnt
               FROM campaign_submissions WHERE campaign_id=?
               GROUP BY user_id ORDER BY cnt DESC LIMIT 5""",
            (camp["id"],),
        ).fetchall()

    filled = min(int((total / max(camp["target"], 1)) * 10), 10)
    bar = "█" * filled + "░" * (10 - filled)
    pct = int((total / max(camp["target"], 1)) * 100)

    medals = ["🥇", "🥈", "🥉", "4.", "5."]
    top_lines = []
    for i, row in enumerate(top):
        name = f"@{row['username']}" if row["username"] else "Unknown"
        top_lines.append(f"  {medals[i]} {name} — {row['cnt']} links")

    top_block = "\n".join(top_lines) if top_lines else "  No submissions yet"

    await update.message.reply_text(
        f" *{camp['name']}*\n"
        f"_{camp['description']}_\n\n"
        f"[{bar}] {total}/{camp['target']} ({pct}%)\n\n"
        f" Reward: {camp['reward']}\n"
        f"📅 Deadline: {camp['deadline']}\n\n"
        f"🏆 Top Contributors:\n{top_block}",
        parse_mode="Markdown",
    )


async def cmd_mycampaignstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    tg_user = update.effective_user
    thread_id = update.message.message_thread_id or 0

    with db() as conn:
        if not is_cmd_enabled(conn, chat_id, thread_id, "mycampaignstats"):
            await deny_user_cmd(update, "mycampaignstats")
            return

        camp = active_campaign(conn, chat_id, thread_id)
        if not camp:
            await update.message.reply_text("No active campaign.")
            return

        count = conn.execute(
            "SELECT COUNT(*) FROM campaign_submissions WHERE campaign_id=? AND user_id=?",
            (camp["id"], tg_user.id),
        ).fetchone()[0]

        rank_row = conn.execute(
            """SELECT COUNT(DISTINCT user_id) + 1 as rank FROM (
               SELECT user_id, COUNT(*) as cnt FROM campaign_submissions
               WHERE campaign_id=? GROUP BY user_id
               HAVING cnt > (
                   SELECT COUNT(*) FROM campaign_submissions
                   WHERE campaign_id=? AND user_id=?
               )
            )""",
            (camp["id"], camp["id"], tg_user.id),
        ).fetchone()

        rank = rank_row["rank"] if rank_row else "N/A"

    handle = f"@{tg_user.username}" if tg_user.username else tg_user.full_name
    await update.message.reply_text(
        f"📊 *{handle}'s Campaign Stats*\n\n"
        f"Campaign: {camp['name']}\n"
        f"🔗 Your submissions: {count}\n"
        f"🏅 Your rank: #{rank}",
        parse_mode="Markdown",
    )


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        if not is_cmd_enabled(conn, chat_id, thread_id, "stats"):
            await deny_user_cmd(update, "stats")
            return

        settings = fetch_settings(conn, chat_id, thread_id)
        total_users = conn.execute(
            "SELECT COUNT(*) FROM users WHERE chat_id=?", (chat_id,)
        ).fetchone()[0]
        total_links = conn.execute(
            "SELECT COALESCE(SUM(total_links),0) FROM users WHERE chat_id=?", (chat_id,)
        ).fetchone()[0]
        total_campaigns = conn.execute(
            "SELECT COUNT(*) FROM campaigns WHERE chat_id=? AND thread_id=?",
            (chat_id, thread_id),
        ).fetchone()[0]
        banned = conn.execute(
            "SELECT COUNT(*) FROM users WHERE chat_id=? AND banned=1", (chat_id,)
        ).fetchone()[0]
        warned = conn.execute(
            "SELECT COUNT(*) FROM users WHERE chat_id=? AND warnings > 0", (chat_id,)
        ).fetchone()[0]

    session_status = "🟢 Active" if settings["session_active"] else "🔴 Inactive"

    await update.message.reply_text(
        f"📊 *Group Stats*\n\n"
        f"👥 Users tracked: {total_users}\n"
        f"🔗 Total links posted: {total_links}\n"
        f" Campaigns run: {total_campaigns}\n"
        f"⚠️ Users with warnings: {warned}\n"
        f"⛔ Banned users: {banned}\n\n"
        f"⚙️ Queue size: {settings['queue_size']}\n"
        f" Points per link: {settings['points_per_link']}\n"
        f"Session: {session_status}",
        parse_mode="Markdown",
    )


# ─── ADMIN — SESSION CONTROL ──────────────────────────────────────────────────

async def start_session(update: Update, queue_size: int):
    """Start a link-drop session with a fixed queue size."""
    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        update_chat_settings(conn, chat_id, thread_id, queue_size=queue_size, session_active=1)
        conn.execute("DELETE FROM link_queue WHERE chat_id=? AND thread_id=?", (chat_id, thread_id))
        conn.commit()

    await update.message.reply_text(
        f"🚀 *{queue_size}-link session started!*\n"
        "Queue has been cleared. Only Twitter/X links accepted.\n"
        f"Users must wait for {queue_size} unique posts before posting again.\n"
        "Use /stopsession to end it.",
        parse_mode="Markdown",
    )


async def cmd_startsession(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    await start_session(update, 15)


async def cmd_startsession15(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    await start_session(update, 15)


async def cmd_startsession28(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    await start_session(update, 28)


async def cmd_stopsession(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        update_chat_settings(conn, chat_id, thread_id, session_active=0)
        conn.commit()

    await update.message.reply_text("🛑 Link-drop session stopped.")


async def cmd_setqueue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /setqueue [number]")
        return

    try:
        size = int(context.args[0])
        if size < 1:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Provide a valid positive integer.")
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        update_chat_settings(conn, chat_id, thread_id, queue_size=size)
        conn.commit()

    await update.message.reply_text(f" Queue size updated to {size}.")


async def cmd_setpoints(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return

    if not context.args:
        await update.message.reply_text("Usage: /setpoints [number]")
        return

    try:
        pts = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Provide a valid integer.")
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        update_chat_settings(conn, chat_id, thread_id, points_per_link=pts)
        conn.commit()

    await update.message.reply_text(f" Points per link set to {pts}.")


# ─── ADMIN — USER ACTIONS ─────────────────────────────────────────────────────

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /reset @username")
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return
        conn.execute(
            "UPDATE users SET warnings=0, total_links=0, points=0 WHERE user_id=? AND chat_id=?",
            (target["user_id"], chat_id),
        )
        conn.execute(
            "DELETE FROM link_queue WHERE user_id=? AND chat_id=?",
            (target["user_id"], chat_id),
        )
        conn.commit()

    await update.message.reply_text(
        f" @{username}'s stats (warnings, links, points, queue position) have been reset."
    )


async def cmd_whitelist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /whitelist @username")
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(
                f"@{username} has no record yet. They need to post at least once."
            )
            return
        is_wl = target["whitelisted"]
        new_val = 0 if is_wl else 1
        conn.execute(
            "UPDATE users SET whitelisted=? WHERE user_id=? AND chat_id=?",
            (new_val, target["user_id"], chat_id),
        )
        conn.commit()

    state = "removed from whitelist" if is_wl else "whitelisted (queue-exempt)"
    await update.message.reply_text(f" @{username} has been {state}.")


async def cmd_warn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /warn @username")
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not found in records.")
            return

        new_warnings = target["warnings"] + 1
        conn.execute(
            "UPDATE users SET warnings=? WHERE user_id=? AND chat_id=?",
            (new_warnings, target["user_id"], chat_id),
        )
        conn.commit()

        await apply_escalation(
            context, conn, chat_id, target["user_id"],
            f"@{username}", new_warnings,
        )


async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /ban @username")
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return
        try:
            await context.bot.ban_chat_member(chat_id, target["user_id"])
            conn.execute(
                "UPDATE users SET banned=1 WHERE user_id=? AND chat_id=?",
                (target["user_id"], chat_id),
            )
            conn.commit()
            await update.message.reply_text(f"⛔ @{username} has been banned.")
        except Exception as e:
            await update.message.reply_text(f"❌ Ban failed: {e}")


async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /unban @username")
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return
        try:
            await context.bot.unban_chat_member(chat_id, target["user_id"])
            conn.execute(
                "UPDATE users SET banned=0, warnings=0 WHERE user_id=? AND chat_id=?",
                (target["user_id"], chat_id),
            )
            conn.commit()
            await update.message.reply_text(
                f" @{username} has been unbanned and warnings cleared."
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Unban failed: {e}")


async def cmd_unmute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /unmute @username")
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return
        try:
            await context.bot.restrict_chat_member(
                chat_id,
                target["user_id"],
                permissions=ChatPermissions(
                    can_send_messages=True,
                    can_send_other_messages=True,
                    can_add_web_page_previews=True,
                ),
            )
            await update.message.reply_text(f"@{username} has been unmuted.")
        except Exception as e:
            await update.message.reply_text(f"❌ Unmute failed: {e}")


# ─── ADMIN — CAMPAIGN MANAGEMENT ─────────────────────────────────────────────

async def cmd_newcampaign(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /newcampaign Name | Description | Target | Reward | Deadline
    Example: /newcampaign Alpha Drop | Post your X link | 50 | 0.1 SOL | 2025-05-01
    """
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    raw = update.message.text.partition(" ")[2].strip()
    parts = [p.strip() for p in raw.split("|")]

    if not raw or not parts[0]:
        await update.message.reply_text(
            "Usage:\n/newcampaign Name | Description | Target | Reward | Deadline\n\n"
            "Example:\n/newcampaign Alpha Drop | Drop your X link for the alpha | 50 | 0.1 SOL each | 2025-05-01"
        )
        return

    name = parts[0]
    description = parts[1] if len(parts) > 1 else ""
    target = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 100
    reward = parts[3] if len(parts) > 3 else "TBA"
    deadline = parts[4] if len(parts) > 4 else "Open-ended"

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    user_id = update.effective_user.id

    with db() as conn:
        # Deactivate existing campaigns
        conn.execute(
            "UPDATE campaigns SET active=0 WHERE chat_id=? AND thread_id=? AND active=1",
            (chat_id, thread_id),
        )
        conn.execute(
            """INSERT INTO campaigns (
                   chat_id, thread_id, name, description, target, reward, deadline, created_by
               ) VALUES (?,?,?,?,?,?,?,?)""",
            (chat_id, thread_id, name, description, target, reward, deadline, user_id),
        )
        # Auto-enable session
        update_chat_settings(conn, chat_id, thread_id, session_active=1)
        conn.execute("DELETE FROM link_queue WHERE chat_id=? AND thread_id=?", (chat_id, thread_id))
        conn.commit()

    await update.message.reply_text(
        f" *Campaign Launched: {name}*\n\n"
        f"_{description}_\n\n"
        f"📌 Target: {target} submissions\n"
        f" Reward: {reward}\n"
        f"📅 Deadline: {deadline}\n\n"
        f"Link-drop session is now live. Drop your Twitter/X links below!\n"
        f"Use /campaignstatus to track progress.",
        parse_mode="Markdown",
    )


async def cmd_endcampaign(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0

    with db() as conn:
        camp = active_campaign(conn, chat_id, thread_id)
        if not camp:
            await update.message.reply_text("No active campaign to end.")
            return

        total = conn.execute(
            "SELECT COUNT(*) FROM campaign_submissions WHERE campaign_id=?",
            (camp["id"],),
        ).fetchone()[0]

        top = conn.execute(
            """SELECT username, COUNT(*) as cnt FROM campaign_submissions
               WHERE campaign_id=? GROUP BY user_id ORDER BY cnt DESC LIMIT 10""",
            (camp["id"],),
        ).fetchall()

        conn.execute("UPDATE campaigns SET active=0 WHERE id=?", (camp["id"],))
        update_chat_settings(conn, chat_id, thread_id, session_active=0)
        conn.commit()

    medals = ["🥇", "🥈", "🥉"] + [f"{i+1}." for i in range(3, 10)]
    top_lines = [
        f"  {medals[i]} @{row['username'] or 'Unknown'} — {row['cnt']} links"
        for i, row in enumerate(top)
    ]

    await update.message.reply_text(
        f"🏁 *Campaign Ended: {camp['name']}*\n\n"
        f"Total submissions: {total}\n"
        f"Target was: {camp['target']}\n\n"
        f"🏆 Final Leaderboard:\n" + "\n".join(top_lines),
        parse_mode="Markdown",
    )


async def cmd_exportlinks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Export all links from the most recent campaign as a .txt file."""
    if not await is_admin(update, context):
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0

    with db() as conn:
        camp = latest_campaign(conn, chat_id, thread_id)

        if not camp:
            await update.message.reply_text("No campaigns found.")
            return

        rows = conn.execute(
            """SELECT username, link, submitted_at, verified FROM campaign_submissions
               WHERE campaign_id=? ORDER BY submitted_at""",
            (camp["id"],),
        ).fetchall()

    if not rows:
        await update.message.reply_text("No submissions to export.")
        return

    lines = [
        f"Campaign: {camp['name']}",
        f"Status: {'Active' if camp['active'] else 'Ended'}",
        f"Exported: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}",
        f"Total: {len(rows)} submissions",
        "",
        "Username | Link | Submitted At | Verified",
        "-" * 70,
    ]
    for row in rows:
        verified = "YES" if row["verified"] else "NO"
        lines.append(
            f"@{row['username'] or 'unknown'} | {row['link']} | {row['submitted_at']} | {verified}"
        )

    content = "\n".join(lines).encode("utf-8")
    filename = f"campaign_{camp['id']}_{camp['name'].replace(' ', '_')}.txt"

    await update.message.reply_document(
        document=BytesIO(content),
        filename=filename,
        caption=f"📁 *{camp['name']}* — {len(rows)} submissions exported.",
        parse_mode="Markdown",
    )


async def cmd_verifysub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Mark a submission as verified.
    /verifysub @username [link_or_index]
    """
    if not await is_admin(update, context):
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /verifysub @username")
        return

    with db() as conn:
        camp = active_campaign(conn, chat_id, thread_id)
        if not camp:
            camp = latest_campaign(conn, chat_id, thread_id)
        if not camp:
            await update.message.reply_text("No campaign found.")
            return

        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return

        result = conn.execute(
            """UPDATE campaign_submissions SET verified=1
               WHERE campaign_id=? AND user_id=? AND verified=0""",
            (camp["id"], target["user_id"]),
        )
        conn.commit()
        count = result.rowcount

    if count:
        await update.message.reply_text(
            f" {count} submission(s) from @{username} marked as verified."
        )
    else:
        await update.message.reply_text(
            f"No unverified submissions found for @{username}."
        )


async def cmd_removesub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Remove a specific submission from the active (or most recent) campaign
    without touching the user's other stats.

    Usage:
      /removesub @username              — removes their most recent submission
      /removesub @username [partial url] — removes the matching submission
    """
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /removesub @username [optional partial link]")
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    # Optional partial link filter — everything after the @mention token
    raw = update.message.text.partition(" ")[2].strip()
    parts = raw.split(None, 1)
    link_filter = parts[1].strip() if len(parts) > 1 else None

    with db() as conn:
        # Use active campaign first, fall back to most recent
        camp = active_campaign(conn, chat_id, thread_id)
        if not camp:
            camp = latest_campaign(conn, chat_id, thread_id)
        if not camp:
            await update.message.reply_text("No campaign found.")
            return

        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return

        if link_filter:
            row = conn.execute(
                """SELECT id, link FROM campaign_submissions
                   WHERE campaign_id=? AND user_id=? AND link LIKE ?
                   ORDER BY id DESC LIMIT 1""",
                (camp["id"], target["user_id"], f"%{link_filter}%"),
            ).fetchone()
        else:
            row = conn.execute(
                """SELECT id, link FROM campaign_submissions
                   WHERE campaign_id=? AND user_id=?
                   ORDER BY id DESC LIMIT 1""",
                (camp["id"], target["user_id"]),
            ).fetchone()

        if not row:
            await update.message.reply_text(
                f"No matching submission found for @{username}."
            )
            return

        conn.execute("DELETE FROM campaign_submissions WHERE id=?", (row["id"],))
        # Also remove from the session queue so they can repost
        conn.execute(
            "DELETE FROM link_queue WHERE chat_id=? AND user_id=? AND link=?",
            (chat_id, target["user_id"], row["link"]),
        )
        # Decrement their total_links count
        conn.execute(
            "UPDATE users SET total_links=MAX(0, total_links-1) WHERE user_id=? AND chat_id=?",
            (target["user_id"], chat_id),
        )
        conn.commit()

    await update.message.reply_text(
        f"Submission removed for @{username}:\n{row['link']}\n\n"
        "Their queue position has been cleared — they can repost."
    )


async def cmd_logpayout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Log a reward payout.
    /logpayout @username 0.5 SOL — for Alpha Drop campaign
    """
    if not await is_admin(update, context):
        return

    chat_id = update.effective_chat.id
    username = parse_mention(update)
    if not username or not context.args:
        await update.message.reply_text(
            "Usage: /logpayout @username [amount] [reason]\n"
            "Example: /logpayout @john 0.1 SOL Alpha Drop reward"
        )
        return

    # Remove the @mention token from args — it was part of the text
    all_text = update.message.text.partition(" ")[2]
    parts = all_text.split(None, 2)  # mention, amount, reason
    amount = parts[1] if len(parts) > 1 else "?"
    reason = parts[2] if len(parts) > 2 else ""

    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return

        conn.execute(
            """INSERT INTO rewards (chat_id, user_id, username, amount, reason)
               VALUES (?,?,?,?,?)""",
            (chat_id, target["user_id"], username, amount, reason),
        )
        conn.commit()

    await update.message.reply_text(
        f" Payout logged: @{username} — {amount}\nReason: {reason or 'N/A'}"
    )


async def cmd_payouts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List recent payouts."""
    if not await is_admin(update, context):
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        rows = conn.execute(
            """SELECT username, amount, reason, paid_at FROM rewards
               WHERE chat_id=? ORDER BY id DESC LIMIT 20""",
            (chat_id,),
        ).fetchall()

    if not rows:
        await update.message.reply_text("No payouts logged yet.")
        return

    lines = [" *Recent Payouts*\n"]
    for row in rows:
        lines.append(
            f"@{row['username']} — {row['amount']} ({row['reason'] or 'N/A'}) | {row['paid_at'][:10]}"
        )

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_tagall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Mention all tracked, non-banned users in the current chat.
    Usage: /tagall [optional intro]
    """
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    chat_id = update.effective_chat.id
    intro = update.message.text.partition(" ")[2].strip()
    thread_kwargs = (
        {"message_thread_id": update.message.message_thread_id}
        if update.message.message_thread_id
        else {}
    )

    with db() as conn:
        rows = conn.execute(
            """
            SELECT user_id, username, full_name
            FROM users
            WHERE chat_id=? AND banned=0
            ORDER BY COALESCE(NULLIF(username, ''), full_name, CAST(user_id AS TEXT)) COLLATE NOCASE
            """,
            (chat_id,),
        ).fetchall()

    if not rows:
        await update.message.reply_text("No tracked members found for this chat.")
        return

    messages = chunk_tagall_messages(build_tagall_mentions(rows), intro=intro)

    for message in messages:
        await context.bot.send_message(
            chat_id,
            message,
            parse_mode="HTML",
            disable_web_page_preview=True,
            **thread_kwargs,
        )


# ─── HELP ─────────────────────────────────────────────────────────────────────

# ─── ADMIN — COMMAND PERMISSIONS ─────────────────────────────────────────────

async def cmd_enablecmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /enablecmd [command]
    Enable a user command in the current topic thread.
    Valid: mystatus, leaderboard, campaignstatus, mycampaignstats, stats
    """
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    if not context.args:
        await update.message.reply_text(
            "Usage: /enablecmd [command]\n"
            f"Valid commands: {', '.join(sorted(USER_COMMANDS))}"
        )
        return

    cmd = context.args[0].lower().lstrip("/")
    if cmd not in USER_COMMANDS:
        await update.message.reply_text(
            f"❌ `{cmd}` is not a toggleable user command.\n"
            f"Valid: {', '.join(sorted(USER_COMMANDS))}",
            parse_mode="Markdown",
        )
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        conn.execute(
            """INSERT INTO cmd_permissions (chat_id, thread_id, command, enabled)
               VALUES (?,?,?,1)
               ON CONFLICT(chat_id, thread_id, command) DO UPDATE SET enabled=1""",
            (chat_id, thread_id, cmd),
        )
        conn.commit()

    topic_label = f"topic {thread_id}" if thread_id else "this group (no topic)"
    await update.message.reply_text(
        f"`/{cmd}` enabled for users in {topic_label}.",
        parse_mode="Markdown",
    )


async def cmd_disablecmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /disablecmd [command]
    Disable a user command in the current topic thread.
    """
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    if not context.args:
        await update.message.reply_text(
            "Usage: /disablecmd [command]\n"
            f"Valid commands: {', '.join(sorted(USER_COMMANDS))}"
        )
        return

    cmd = context.args[0].lower().lstrip("/")
    if cmd not in USER_COMMANDS:
        await update.message.reply_text(
            f"❌ `{cmd}` is not a toggleable user command.\n"
            f"Valid: {', '.join(sorted(USER_COMMANDS))}",
            parse_mode="Markdown",
        )
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        conn.execute(
            """INSERT INTO cmd_permissions (chat_id, thread_id, command, enabled)
               VALUES (?,?,?,0)
               ON CONFLICT(chat_id, thread_id, command) DO UPDATE SET enabled=0""",
            (chat_id, thread_id, cmd),
        )
        conn.commit()

    topic_label = f"topic {thread_id}" if thread_id else "this group (no topic)"
    await update.message.reply_text(
        f"🔒 `/{cmd}` disabled for users in {topic_label}.",
        parse_mode="Markdown",
    )


async def cmd_cmdstatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show which user commands are enabled or disabled in the current topic."""
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0

    with db() as conn:
        rows = {
            row["command"]: bool(row["enabled"])
            for row in conn.execute(
                "SELECT command, enabled FROM cmd_permissions WHERE chat_id=? AND thread_id=?",
                (chat_id, thread_id),
            ).fetchall()
        }

    topic_label = f"Topic {thread_id}" if thread_id else "Group (no topic)"
    lines = [f"*Command Permissions — {topic_label}*\n"]
    for cmd in sorted(USER_COMMANDS):
        enabled = rows.get(cmd, False)
        icon = "🟢" if enabled else "🔴"
        lines.append(f"{icon} `/{cmd}`")

    lines.append("\nUse /enablecmd or /disablecmd to toggle.")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        text, markup = build_private_menu("home")
        await update.message.reply_text(text, reply_markup=markup)
        return

    await update.message.reply_text(
        " *KOL Campaign Manager*\n\n"
        "*── User Commands ──*\n"
        "/mystatus — Your queue position & stats\n"
        "/leaderboard — All-time top posters\n"
        "/campaignstatus — Current campaign progress in this topic\n"
        "/mycampaignstats — Your campaign submissions in this topic\n"
        "/stats — Group-wide stats\n\n"
        "*── Admin: Sessions ──*\n"
        "/startsession — Start 15-link session\n"
        "/startsession15 — Start 15-link session\n"
        "/startsession28 — Start 28-link session\n"
        "/stopsession — Stop the session\n"
        "/setqueue [n] — Change queue size manually\n"
        "/setpoints [n] — Points awarded per valid link\n\n"
        "*── Admin: User Control ──*\n"
        "/reset @user — Wipe user stats & queue\n"
        "/whitelist @user — Toggle queue exemption\n"
        "/warn @user — Issue warning (3=mute 24h, 4=mute 72h, 5=ban)\n"
        "/ban @user — Ban from group\n"
        "/unban @user — Unban user\n"
        "/unmute @user — Lift a mute early\n"
        "/tagall [message] — Mention all tracked members\n\n"
        "*── Admin: Command Permissions ──*\n"
        "/enablecmd [command] — Allow users to run a command\n"
        "/disablecmd [command] — Block users from a command\n"
        "/cmdstatus — See which user commands are on/off\n\n"
        "*── Admin: Campaigns ──*\n"
        "/newcampaign Name | Desc | Target | Reward | Deadline\n"
        "/endcampaign — Close active campaign\n"
        "/exportlinks — Download all submitted links from this topic campaign as a text file, useful for checking work or sending the list to the payout team\n"
        "/verifysub @user — Mark submissions verified\n"
        "/removesub @user [partial link] — Remove a submission\n"
        "/logpayout @user [amount] [reason] — Save a payment record for a user, including amount and why they were paid\n"
        "/payouts — Show recent payment records so you can confirm who has already been paid\n",
        parse_mode="Markdown",
    )


async def on_private_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    await query.answer()
    if query.message.chat.type != "private":
        return

    section = query.data.removeprefix(PRIVATE_MENU_PREFIX) if query.data else "home"
    text, markup = build_private_menu(section)
    await query.edit_message_text(text, reply_markup=markup, parse_mode="Markdown")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CallbackQueryHandler(on_private_menu, pattern=f"^{PRIVATE_MENU_PREFIX}"))

    # User commands
    for cmd in ["start", "help"]:
        app.add_handler(CommandHandler(cmd, cmd_help))

    app.add_handler(CommandHandler("mystatus", cmd_mystatus))
    app.add_handler(CommandHandler("leaderboard", cmd_leaderboard))
    app.add_handler(CommandHandler("campaignstatus", cmd_campaignstatus))
    app.add_handler(CommandHandler("mycampaignstats", cmd_mycampaignstats))
    app.add_handler(CommandHandler("stats", cmd_stats))

    # Admin — session
    app.add_handler(CommandHandler("startsession", cmd_startsession))
    app.add_handler(CommandHandler("startsession15", cmd_startsession15))
    app.add_handler(CommandHandler("startsession28", cmd_startsession28))
    app.add_handler(CommandHandler("stopsession", cmd_stopsession))
    app.add_handler(CommandHandler("setqueue", cmd_setqueue))
    app.add_handler(CommandHandler("setpoints", cmd_setpoints))

    # Admin — users
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(CommandHandler("whitelist", cmd_whitelist))
    app.add_handler(CommandHandler("warn", cmd_warn))
    app.add_handler(CommandHandler("ban", cmd_ban))
    app.add_handler(CommandHandler("unban", cmd_unban))
    app.add_handler(CommandHandler("unmute", cmd_unmute))
    app.add_handler(CommandHandler("tagall", cmd_tagall))

    # Admin — command permissions
    app.add_handler(CommandHandler("enablecmd", cmd_enablecmd))
    app.add_handler(CommandHandler("disablecmd", cmd_disablecmd))
    app.add_handler(CommandHandler("cmdstatus", cmd_cmdstatus))

    # Admin — campaigns
    app.add_handler(CommandHandler("newcampaign", cmd_newcampaign))
    app.add_handler(CommandHandler("endcampaign", cmd_endcampaign))
    app.add_handler(CommandHandler("exportlinks", cmd_exportlinks))
    app.add_handler(CommandHandler("verifysub", cmd_verifysub))
    app.add_handler(CommandHandler("removesub", cmd_removesub))
    app.add_handler(CommandHandler("logpayout", cmd_logpayout))
    app.add_handler(CommandHandler("payouts", cmd_payouts))

    # Message handler — must be last
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    logger.info("KOL Campaign Manager Bot is running...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
