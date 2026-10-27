"""
Telegram VIP Membership Manager Bot
===================================
Stack : Python 3.10+, pyTelegramBotAPI (telebot), Flask (webhook), SQLite
Target: PythonAnywhere (WSGI webhook + external cron such as cron-job.org)

pip install pyTelegramBotAPI flask

Secrets are read from environment variables (never hardcode them):
    BOT_TOKEN   - token from @BotFather
    CRON_SECRET - any long random string (protects /cron-check and /set-webhook)

PythonAnywhere WSGI file (set the variables BEFORE importing the bot):
    import os
    os.environ["BOT_TOKEN"] = "<your bot token>"
    os.environ["CRON_SECRET"] = "<your random secret>"
    from vip_bot import app as application

Setup:
    1. Make the bot ADMIN in the group (Restrict members + Invite users).
    2. Open  https://<host>/set-webhook?secret=<CRON_SECRET>   once.
    3. cron-job.org -> GET https://<host>/cron-check?secret=<CRON_SECRET> every 15-60 min.
"""

import hmac
import html
import logging
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import telebot
from flask import Flask, request, abort, jsonify
from telebot import types

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
CRON_SECRET = os.environ.get("CRON_SECRET", "").strip()

if not BOT_TOKEN or not CRON_SECRET:
    raise RuntimeError(
        "Missing configuration: set the BOT_TOKEN and CRON_SECRET environment variables."
    )

WEBHOOK_BASE_URL = os.environ.get("WEBHOOK_BASE_URL", "").strip()
DB_PATH = os.environ.get(
    "DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "vip_manager.db")
)

DAY = 86400
MAX_DAYS = 3650
ALLOWED_UPDATES = [
    "message",
    "callback_query",
    "chat_join_request",
    "my_chat_member",
]

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("vip_bot")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML", threaded=False)
app = Flask(__name__)

_bot_info = {}


# --------------------------------------------------------------------------- #
# Time helpers (all stored values are UTC Unix epoch integers)
# --------------------------------------------------------------------------- #
def now_ts() -> int:
    # time.time() is always the true UTC epoch. (datetime.utcnow().timestamp()
    # is off by the server's UTC offset on non-UTC hosts.)
    return int(time.time())


def fmt_ts(ts) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fmt_remaining(seconds: int) -> str:
    if seconds <= 0:
        return "expired"
    d, rem = divmod(seconds, DAY)
    h = rem // 3600
    if d:
        return f"{d}d {h}h"
    m = (rem % 3600) // 60
    return f"{h}h {m}m"


# --------------------------------------------------------------------------- #
# Database (thread-safe, auto-migrating)
# --------------------------------------------------------------------------- #
_db_lock = threading.RLock()


@contextmanager
def db():
    with _db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


GROUPS_COLUMNS = {
    "group_id": "INTEGER PRIMARY KEY",
    "title": "TEXT",
    "active": "INTEGER DEFAULT 1",
}

MEMBERS_COLUMNS = {
    "group_id": "INTEGER",
    "user_id": "INTEGER",
    "full_name": "TEXT",
    "username": "TEXT",
    "join_date": "INTEGER",
    "expiry_date": "INTEGER",
    "status": "TEXT DEFAULT 'pending'",
    "reminded_3d": "INTEGER DEFAULT 0",
    "reminded_last": "INTEGER DEFAULT 0",
    "approved_by": "TEXT",
    "card_message_id": "INTEGER",
}


def _migrate_columns(conn, table, columns):
    existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
    for col, decl in columns.items():
        if col not in existing:
            # PRIMARY KEY can't be added via ALTER; strip it for migrations.
            decl = decl.replace("PRIMARY KEY", "").strip()
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
            log.info("Migration: added %s.%s", table, col)


def init_db():
    with db() as c:
        c.execute(
            """CREATE TABLE IF NOT EXISTS groups (
                group_id INTEGER PRIMARY KEY,
                title TEXT,
                active INTEGER DEFAULT 1
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS members (
                group_id INTEGER,
                user_id INTEGER,
                full_name TEXT,
                username TEXT,
                join_date INTEGER,
                expiry_date INTEGER,
                status TEXT DEFAULT 'pending',
                reminded_3d INTEGER DEFAULT 0,
                reminded_last INTEGER DEFAULT 0,
                approved_by TEXT,
                card_message_id INTEGER,
                PRIMARY KEY (group_id, user_id)
            )"""
        )
        _migrate_columns(c, "groups", GROUPS_COLUMNS)
        _migrate_columns(c, "members", MEMBERS_COLUMNS)
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_members_status_exp ON members(status, expiry_date)"
        )


init_db()


def upsert_group(group_id, title, active=1):
    with db() as c:
        c.execute(
            """INSERT INTO groups (group_id, title, active) VALUES (?,?,?)
               ON CONFLICT(group_id) DO UPDATE SET title=excluded.title, active=excluded.active""",
            (group_id, title, active),
        )


def get_member(group_id, user_id):
    with db() as c:
        return c.execute(
            "SELECT * FROM members WHERE group_id=? AND user_id=?", (group_id, user_id)
        ).fetchone()


def upsert_member_info(group_id, user_id, full_name, username):
    """Create a pending row if missing; otherwise only refresh name/username."""
    with db() as c:
        c.execute(
            """INSERT INTO members (group_id, user_id, full_name, username, join_date, status)
               VALUES (?,?,?,?,?, 'pending')
               ON CONFLICT(group_id, user_id) DO UPDATE SET
                   full_name=excluded.full_name, username=excluded.username""",
            (group_id, user_id, full_name, username, now_ts()),
        )


def reset_to_pending(group_id, user_id, full_name, username):
    """A fresh join/request: wipe the previous cycle and wait for approval."""
    with db() as c:
        c.execute(
            """INSERT INTO members (group_id, user_id, full_name, username, join_date, status)
               VALUES (?,?,?,?,?, 'pending')
               ON CONFLICT(group_id, user_id) DO UPDATE SET
                   full_name=excluded.full_name, username=excluded.username,
                   join_date=excluded.join_date, expiry_date=NULL, status='pending',
                   reminded_3d=0, reminded_last=0, approved_by=NULL, card_message_id=NULL""",
            (group_id, user_id, full_name, username, now_ts()),
        )


def set_card_message(group_id, user_id, message_id):
    with db() as c:
        c.execute(
            "UPDATE members SET card_message_id=? WHERE group_id=? AND user_id=?",
            (message_id, group_id, user_id),
        )


def grant_membership(group_id, user_id, days, admin_label):
    """Set/extend expiry. Extends from the current expiry if still active."""
    now = now_ts()
    with db() as c:
        row = c.execute(
            "SELECT status, expiry_date FROM members WHERE group_id=? AND user_id=?",
            (group_id, user_id),
        ).fetchone()
        base = now
        if row and row["status"] == "active" and row["expiry_date"] and row["expiry_date"] > now:
            base = row["expiry_date"]
        new_expiry = base + int(days) * DAY
        c.execute(
            """UPDATE members SET status='active', expiry_date=?, reminded_3d=0,
                   reminded_last=0, approved_by=?, join_date=COALESCE(join_date, ?)
               WHERE group_id=? AND user_id=?""",
            (new_expiry, admin_label, now, group_id, user_id),
        )
    return new_expiry


# --------------------------------------------------------------------------- #
# Telegram helpers
# --------------------------------------------------------------------------- #
def bot_username():
    if "username" not in _bot_info:
        me = bot.get_me()
        _bot_info["username"] = me.username
        _bot_info["id"] = me.id
    return _bot_info["username"]


def bot_id():
    bot_username()
    return _bot_info["id"]


def full_name_of(u) -> str:
    return " ".join(p for p in [u.first_name, getattr(u, "last_name", None)] if p) or str(u.id)


def mention(uid, name) -> str:
    return f'<a href="tg://user?id={uid}">{html.escape(name or str(uid))}</a>'


def admin_label(u) -> str:
    return f"@{u.username}" if getattr(u, "username", None) else full_name_of(u)


def is_admin(chat_id, user_id) -> bool:
    try:
        return bot.get_chat_member(chat_id, user_id).status in ("creator", "administrator")
    except Exception as e:
        log.warning("is_admin check failed: %s", e)
        return False


def sender_is_admin(m) -> bool:
    # Anonymous admins post as the group itself.
    if m.sender_chat and m.sender_chat.id == m.chat.id:
        return True
    return is_admin(m.chat.id, m.from_user.id)


def perms(allowed: bool) -> types.ChatPermissions:
    return types.ChatPermissions(
        can_send_messages=allowed,
        can_send_audios=allowed,
        can_send_documents=allowed,
        can_send_photos=allowed,
        can_send_videos=allowed,
        can_send_video_notes=allowed,
        can_send_voice_notes=allowed,
        can_send_polls=allowed,
        can_send_other_messages=allowed,
        can_add_web_page_previews=allowed,
        can_invite_users=allowed,
    )


def restrict_user(chat_id, user_id):
    try:
        bot.restrict_chat_member(chat_id, user_id, permissions=perms(False))
        return True
    except Exception as e:
        log.warning("restrict failed g=%s u=%s: %s", chat_id, user_id, e)
        return False


def unrestrict_user(chat_id, user_id):
    try:
        bot.restrict_chat_member(chat_id, user_id, permissions=perms(True))
        return True
    except Exception as e:
        log.warning("unrestrict failed g=%s u=%s: %s", chat_id, user_id, e)
        return False


def kick_user(chat_id, user_id):
    """Ban + unban (user can rejoin later). Raises on real failure."""
    try:
        bot.ban_chat_member(chat_id, user_id)
    except Exception as e:
        if "USER_NOT_PARTICIPANT" not in str(e).upper():
            raise
    try:
        bot.unban_chat_member(chat_id, user_id, only_if_banned=True)
    except Exception as e:
        log.warning("unban after kick failed g=%s u=%s: %s", chat_id, user_id, e)


# --------------------------------------------------------------------------- #
# Approval card
# --------------------------------------------------------------------------- #
def card_text(user_id, full_name, username, group_title, footer):
    uname = f"@{html.escape(username)}" if username else "—"
    return (
        "🆕 <b>New Member Request</b>\n\n"
        f"👤 <b>Name:</b> {mention(user_id, full_name)}\n"
        f"🔗 <b>Username:</b> {uname}\n"
        f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
        f"💬 <b>Group:</b> {html.escape(group_title or '')}\n\n"
        f"{footer}"
    )


def card_keyboard(user_id):
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("7 Days", callback_data=f"ap:{user_id}:7"),
        types.InlineKeyboardButton("15 Days", callback_data=f"ap:{user_id}:15"),
        types.InlineKeyboardButton("30 Days", callback_data=f"ap:{user_id}:30"),
    )
    kb.row(
        types.InlineKeyboardButton("⚙️ Custom Days", callback_data=f"cd:{user_id}"),
        types.InlineKeyboardButton("❌ Reject & Kick", callback_data=f"rj:{user_id}"),
    )
    return kb


def post_approval_card(chat, user_id, full_name, username):
    try:
        msg = bot.send_message(
            chat.id,
            card_text(user_id, full_name, username, chat.title, "⏳ <i>Waiting for admin approval…</i>"),
            reply_markup=card_keyboard(user_id),
        )
        set_card_message(chat.id, user_id, msg.message_id)
    except Exception as e:
        log.error("Could not post approval card g=%s u=%s: %s", chat.id, user_id, e)


# --------------------------------------------------------------------------- #
# Join handling
# --------------------------------------------------------------------------- #
@bot.my_chat_member_handler()
def on_my_chat_member(upd):
    chat = upd.chat
    if chat.type not in ("group", "supergroup"):
        return
    status = upd.new_chat_member.status
    upsert_group(chat.id, chat.title, 1 if status in ("member", "administrator") else 0)


@bot.chat_join_request_handler()
def on_join_request(req):
    chat, user = req.chat, req.from_user
    upsert_group(chat.id, chat.title, 1)
    existing = get_member(chat.id, user.id)

    # Pre-paid member (e.g. added via /add_days) -> auto-approve.
    if existing and existing["status"] == "active" and (existing["expiry_date"] or 0) > now_ts():
        try:
            bot.approve_chat_join_request(chat.id, user.id)
        except Exception as e:
            log.warning("auto-approve failed: %s", e)
        return

    # Pending join already has a card: don't spam another.
    if existing and existing["status"] == "pending" and existing["card_message_id"]:
        return

    name = full_name_of(user)
    reset_to_pending(chat.id, user.id, name, user.username)
    post_approval_card(chat, user.id, name, user.username)


@bot.message_handler(content_types=["new_chat_members"])
def on_new_members(m):
    chat = m.chat
    if chat.type not in ("group", "supergroup"):
        return
    for user in m.new_chat_members:
        if user.id == bot_id():
            upsert_group(chat.id, chat.title, 1)
            try:
                bot.send_message(
                    chat.id,
                    "✅ VIP Manager is active. Make me an admin with <b>Restrict members</b> "
                    "and <b>Invite users</b> rights so I can manage memberships.",
                )
            except Exception:
                pass
            continue
        if user.is_bot:
            continue

        upsert_group(chat.id, chat.title, 1)
        existing = get_member(chat.id, user.id)

        # Already paid and valid (approved via join request, or pre-granted): leave alone.
        if existing and existing["status"] == "active" and (existing["expiry_date"] or 0) > now_ts():
            continue

        restrict_user(chat.id, user.id)

        # Card already posted from the join request: just keep them restricted.
        if existing and existing["status"] == "pending" and existing["card_message_id"]:
            continue

        name = full_name_of(user)
        reset_to_pending(chat.id, user.id, name, user.username)
        post_approval_card(chat, user.id, name, user.username)


# --------------------------------------------------------------------------- #
# Approval callbacks
# --------------------------------------------------------------------------- #
def finalize_approval(chat, user_id, days, admin_user):
    """Grant, lift restrictions, return new expiry."""
    label = admin_label(admin_user)
    new_expiry = grant_membership(chat.id, user_id, days, label)
    # If this was a join request, approving it lets them in; harmless otherwise.
    try:
        bot.approve_chat_join_request(chat.id, user_id)
    except Exception:
        pass
    unrestrict_user(chat.id, user_id)
    return new_expiry, label


def edit_card(chat_id, message_id, text):
    try:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=None)
    except Exception as e:
        log.warning("edit card failed: %s", e)


@bot.callback_query_handler(func=lambda c: c.data and c.data.split(":")[0] in ("ap", "cd", "rj"))
def on_card_action(call):
    parts = call.data.split(":")
    action = parts[0]
    msg = call.message
    chat = msg.chat

    if chat.type not in ("group", "supergroup"):
        return bot.answer_callback_query(call.id)

    if not is_admin(chat.id, call.from_user.id):
        return bot.answer_callback_query(call.id, "Only Group Admins can approve!", show_alert=True)

    try:
        user_id = int(parts[1])
    except (IndexError, ValueError):
        return bot.answer_callback_query(call.id, "Invalid button.", show_alert=True)

    member = get_member(chat.id, user_id)
    if member and member["status"] == "active" and (member["expiry_date"] or 0) > now_ts():
        return bot.answer_callback_query(
            call.id, "Already approved. Use /add_days to extend.", show_alert=True
        )

    name = member["full_name"] if member else str(user_id)
    uname = member["username"] if member else None

    if action == "ap":
        days = int(parts[2])
        if not member:
            upsert_member_info(chat.id, user_id, name, uname)
        expiry, label = finalize_approval(chat, user_id, days, call.from_user)
        edit_card(
            chat.id,
            msg.message_id,
            card_text(
                user_id, name, uname, chat.title,
                f"✅ <b>Approved for {days} days</b> by {html.escape(label)}\n"
                f"📅 Expires: <b>{fmt_ts(expiry)}</b>",
            ),
        )
        bot.answer_callback_query(call.id, f"Approved for {days} days ✅")

    elif action == "cd":
        try:
            bot.send_message(
                chat.id,
                f"⚙️ <b>Custom days</b> for {mention(user_id, name)}\n"
                f"Reply to this message with a number of days (1-{MAX_DAYS}).\n"
                f"<code>[ref:{user_id}]</code>",
                reply_markup=types.ForceReply(selective=False),
            )
        except Exception as e:
            log.warning("custom prompt failed: %s", e)
        bot.answer_callback_query(call.id, "Reply to the prompt with the number of days.")

    elif action == "rj":
        try:
            bot.decline_chat_join_request(chat.id, user_id)
        except Exception:
            pass
        try:
            kick_user(chat.id, user_id)
            result = "❌ <b>Rejected & removed</b>"
            status = "rejected"
        except Exception as e:
            log.warning("reject kick failed: %s", e)
            result = "❌ <b>Rejected</b> (could not remove — check my admin rights)"
            status = "rejected"
        label = admin_label(call.from_user)
        with db() as c:
            c.execute(
                "UPDATE members SET status=?, approved_by=? WHERE group_id=? AND user_id=?",
                (status, label, chat.id, user_id),
            )
        edit_card(
            chat.id, msg.message_id,
            card_text(user_id, name, uname, chat.title, f"{result} by {html.escape(label)}"),
        )
        bot.answer_callback_query(call.id, "Rejected")


_REF_RE = re.compile(r"\[ref:(\d+)\]")


def _is_custom_days_reply(m):
    r = m.reply_to_message
    return bool(
        m.text
        and not m.text.startswith("/")
        and r
        and r.from_user
        and r.from_user.id == bot_id()
        and r.text
        and _REF_RE.search(r.text)
    )


@bot.message_handler(func=_is_custom_days_reply, content_types=["text"])
def on_custom_days_reply(m):
    if m.chat.type not in ("group", "supergroup"):
        return
    if not sender_is_admin(m):
        return bot.reply_to(m, "Only Group Admins can approve!")
    try:
        days = int(m.text.strip())
        if not 1 <= days <= MAX_DAYS:
            raise ValueError
    except ValueError:
        return bot.reply_to(m, f"Please send a whole number between 1 and {MAX_DAYS}.")

    user_id = int(_REF_RE.search(m.reply_to_message.text).group(1))
    member = get_member(m.chat.id, user_id)
    name = member["full_name"] if member else str(user_id)
    uname = member["username"] if member else None
    if not member:
        upsert_member_info(m.chat.id, user_id, name, uname)

    admin_user = m.from_user
    expiry, label = finalize_approval(m.chat, user_id, days, admin_user)

    # Update the original card if we still know it.
    card_id = member["card_message_id"] if member else None
    summary = (
        f"✅ <b>Approved for {days} days</b> by {html.escape(label)}\n"
        f"📅 Expires: <b>{fmt_ts(expiry)}</b>"
    )
    if card_id:
        edit_card(m.chat.id, card_id, card_text(user_id, name, uname, m.chat.title, summary))
    bot.reply_to(m, f"✅ {mention(user_id, name)} approved for {days} days.\n📅 Expires: {fmt_ts(expiry)}")


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #
HELP_TEXT = (
    "📖 <b>Commands</b>\n"
    "/status <code>&lt;user_id&gt;</code> — membership status &amp; expiry\n"
    "/add_days <code>&lt;user_id&gt; &lt;days&gt;</code> — grant/extend membership (in group)\n"
    "/help — show this message\n\n"
    "⚙️ <b>Quick setup</b>\n"
    "1. Add me to your group.\n"
    "2. Promote me to admin (Restrict members + Invite users).\n"
    "3. New members are restricted and an approval card appears in the group.\n"
    "4. Admins tap a duration. I handle reminders and auto-removal."
)


@bot.message_handler(commands=["start", "help"])
def cmd_start(m):
    if m.chat.type != "private":
        return bot.reply_to(m, HELP_TEXT)
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(
            "➕ Add Bot to Group",
            url=f"https://t.me/{bot_username()}?startgroup=true&admin=restrict_members+invite_users",
        )
    )
    kb.row(types.InlineKeyboardButton("📂 My Groups", callback_data="mg"))
    bot.send_message(
        m.chat.id,
        f"👋 <b>Welcome to VIP Membership Manager</b>\n\n"
        f"I manage paid/VIP access to your Telegram groups: approval cards, "
        f"expiry tracking, reminders and automatic removal.\n\n{HELP_TEXT}",
        reply_markup=kb,
    )


@bot.callback_query_handler(func=lambda c: c.data == "mg")
def on_my_groups(call):
    with db() as c:
        groups = c.execute("SELECT group_id, title FROM groups WHERE active=1").fetchall()
    mine = [g for g in groups if is_admin(g["group_id"], call.from_user.id)]
    bot.answer_callback_query(call.id)
    if not mine:
        return bot.send_message(
            call.message.chat.id, "You don't administer any group where I'm active yet."
        )
    lines = []
    with db() as c:
        for g in mine:
            n = c.execute(
                "SELECT COUNT(*) FROM members WHERE group_id=? AND status='active'", (g["group_id"],)
            ).fetchone()[0]
            lines.append(f"• <b>{html.escape(g['title'] or str(g['group_id']))}</b> — {n} active VIP")
    bot.send_message(call.message.chat.id, "📂 <b>Your groups</b>\n\n" + "\n".join(lines))


def _status_block(row, group_title):
    now = now_ts()
    exp = row["expiry_date"]
    remaining = f" ({fmt_remaining(exp - now)} left)" if exp and row["status"] == "active" else ""
    uname = f"@{html.escape(row['username'])}" if row["username"] else "—"
    return (
        f"👤 {mention(row['user_id'], row['full_name'])} ({uname})\n"
        f"💬 Group: {html.escape(group_title or str(row['group_id']))}\n"
        f"📌 Status: <b>{row['status']}</b>\n"
        f"📅 Expires: <b>{fmt_ts(exp)}</b>{remaining}\n"
        f"🛡 Approved by: {html.escape(row['approved_by'] or '—')}"
    )


@bot.message_handler(commands=["status"])
def cmd_status(m):
    args = m.text.split()[1:]
    if len(args) != 1 or not args[0].lstrip("-").isdigit():
        return bot.reply_to(m, "Usage: /status <code>&lt;user_id&gt;</code>")
    uid = int(args[0])

    if m.chat.type in ("group", "supergroup"):
        if not sender_is_admin(m):
            return bot.reply_to(m, "Only Group Admins can use this command.")
        row = get_member(m.chat.id, uid)
        if not row:
            return bot.reply_to(m, "No record found for that user in this group.")
        return bot.reply_to(m, _status_block(row, m.chat.title))

    # Private chat: show records from groups the caller administers.
    with db() as c:
        rows = c.execute(
            """SELECT m.*, g.title FROM members m
               LEFT JOIN groups g ON g.group_id = m.group_id WHERE m.user_id=?""",
            (uid,),
        ).fetchall()
    rows = [r for r in rows if is_admin(r["group_id"], m.from_user.id)]
    if not rows:
        return bot.reply_to(m, "No records found in groups you administer.")
    bot.reply_to(m, "\n\n".join(_status_block(r, r["title"]) for r in rows))


@bot.message_handler(commands=["add_days"])
def cmd_add_days(m):
    if m.chat.type not in ("group", "supergroup"):
        return bot.reply_to(m, "Use /add_days inside the group.")
    if not sender_is_admin(m):
        return bot.reply_to(m, "Only Group Admins can use this command.")
    args = m.text.split()[1:]
    if len(args) != 2 or not args[0].isdigit() or not args[1].isdigit():
        return bot.reply_to(m, "Usage: /add_days <code>&lt;user_id&gt; &lt;days&gt;</code>")
    uid, days = int(args[0]), int(args[1])
    if not 1 <= days <= MAX_DAYS:
        return bot.reply_to(m, f"Days must be between 1 and {MAX_DAYS}.")

    upsert_group(m.chat.id, m.chat.title, 1)
    member = get_member(m.chat.id, uid)
    name = member["full_name"] if member else str(uid)
    uname = member["username"] if member else None
    try:
        u = bot.get_chat_member(m.chat.id, uid).user
        name, uname = full_name_of(u), u.username
    except Exception:
        pass  # not in the group yet: pre-grant, they'll be auto-approved on join
    upsert_member_info(m.chat.id, uid, name, uname)

    expiry, label = finalize_approval(m.chat, uid, days, m.from_user)
    bot.reply_to(
        m,
        f"✅ {mention(uid, name)} granted +{days} days by {html.escape(label)}\n"
        f"📅 New expiry: <b>{fmt_ts(expiry)}</b>",
    )


# --------------------------------------------------------------------------- #
# Cron: reminders + auto-kick (with offline catch-up)
# --------------------------------------------------------------------------- #
_cron_lock = threading.Lock()


def run_cron():
    stats = {"kicked": 0, "kick_failed": 0, "reminded_3d": 0, "reminded_last": 0}
    now = now_ts()

    # 1) EXPIRED: catch-up uses <= so nothing is ever skipped after downtime.
    with db() as c:
        due = c.execute(
            """SELECT * FROM members
               WHERE status IN ('active','kick_failed')
                 AND expiry_date IS NOT NULL AND expiry_date <= ?""",
            (now,),
        ).fetchall()

    for r in due:
        g, u = r["group_id"], r["user_id"]
        try:
            kick_user(g, u)
        except Exception as e:
            log.error("Kick failed g=%s u=%s: %s", g, u, e)
            with db() as c:
                c.execute(
                    "UPDATE members SET status='kick_failed' WHERE group_id=? AND user_id=?", (g, u)
                )
            stats["kick_failed"] += 1
            continue
        with db() as c:
            c.execute(
                "UPDATE members SET status='expired' WHERE group_id=? AND user_id=?", (g, u)
            )
        stats["kicked"] += 1
        try:
            bot.send_message(
                g,
                f"⛔ {mention(u, r['full_name'])}'s VIP membership expired on "
                f"<b>{fmt_ts(r['expiry_date'])}</b> and was removed from the group.",
            )
        except Exception as e:
            log.warning("kick announcement failed g=%s: %s", g, e)
        time.sleep(0.05)

    # 2) LAST DAY (<= 24h left). Also marks the 3-day flag so it can't fire late.
    with db() as c:
        last = c.execute(
            """SELECT * FROM members
               WHERE status='active' AND expiry_date > ? AND reminded_last=0
                 AND expiry_date - ? <= ?""",
            (now, now, DAY),
        ).fetchall()

    for r in last:
        g, u = r["group_id"], r["user_id"]
        with db() as c:  # claim first -> at-most-once even with overlapping crons
            cur = c.execute(
                """UPDATE members SET reminded_last=1, reminded_3d=1
                   WHERE group_id=? AND user_id=? AND reminded_last=0""",
                (g, u),
            )
            if cur.rowcount != 1:
                continue
        try:
            bot.send_message(
                g,
                f"🚨 {mention(u, r['full_name'])}, your VIP membership expires in "
                f"<b>{fmt_remaining(r['expiry_date'] - now_ts())}</b> "
                f"({fmt_ts(r['expiry_date'])}). Contact an admin now to renew!",
            )
            stats["reminded_last"] += 1
        except Exception as e:
            log.warning("last-day reminder failed g=%s u=%s: %s", g, u, e)
            with db() as c:  # release claim so the next run retries
                c.execute(
                    "UPDATE members SET reminded_last=0, reminded_3d=? WHERE group_id=? AND user_id=?",
                    (r["reminded_3d"], g, u),
                )
        time.sleep(0.05)

    # 3) 3 DAYS BEFORE (between 24h and 72h left)
    with db() as c:
        soon = c.execute(
            """SELECT * FROM members
               WHERE status='active' AND reminded_3d=0
                 AND expiry_date - ? <= ? AND expiry_date - ? > ?""",
            (now, 3 * DAY, now, DAY),
        ).fetchall()

    for r in soon:
        g, u = r["group_id"], r["user_id"]
        with db() as c:
            cur = c.execute(
                "UPDATE members SET reminded_3d=1 WHERE group_id=? AND user_id=? AND reminded_3d=0",
                (g, u),
            )
            if cur.rowcount != 1:
                continue
        try:
            bot.send_message(
                g,
                f"⏰ {mention(u, r['full_name'])}, your VIP membership expires in about "
                f"<b>{fmt_remaining(r['expiry_date'] - now_ts())}</b> "
                f"({fmt_ts(r['expiry_date'])}). Please renew to keep access.",
            )
            stats["reminded_3d"] += 1
        except Exception as e:
            log.warning("3-day reminder failed g=%s u=%s: %s", g, u, e)
            with db() as c:
                c.execute(
                    "UPDATE members SET reminded_3d=0 WHERE group_id=? AND user_id=?", (g, u)
                )
        time.sleep(0.05)

    return stats


# --------------------------------------------------------------------------- #
# Flask routes
# --------------------------------------------------------------------------- #
def _secret_ok() -> bool:
    return hmac.compare_digest(request.args.get("secret", ""), CRON_SECRET)


@app.route("/", methods=["GET"])
def health():
    return "VIP Telegram Manager Bot is active", 200


@app.route("/" + BOT_TOKEN, methods=["POST"])
def webhook():
    if request.headers.get("content-type", "").startswith("application/json"):
        try:
            update = telebot.types.Update.de_json(request.get_data().decode("utf-8"))
            bot.process_new_updates([update])
        except Exception:
            # Always 200 so Telegram doesn't hammer us with retries.
            log.exception("Error processing update")
        return "", 200
    abort(403)


@app.route("/cron-check", methods=["GET"])
def cron_check():
    if not _secret_ok():
        abort(403)
    if not _cron_lock.acquire(blocking=False):
        return jsonify(ok=True, skipped="already running")
    try:
        return jsonify(ok=True, **run_cron())
    except Exception as e:
        log.exception("cron failed")
        return jsonify(ok=False, error=str(e)), 500
    finally:
        _cron_lock.release()


@app.route("/set-webhook", methods=["GET"])
def set_webhook():
    if not _secret_ok():
        abort(403)
    base = WEBHOOK_BASE_URL or request.url_root.replace("http://", "https://")
    url = base.rstrip("/") + "/" + BOT_TOKEN
    bot.remove_webhook()
    ok = bot.set_webhook(url=url, allowed_updates=ALLOWED_UPDATES)
    return jsonify(ok=bool(ok), allowed_updates=ALLOWED_UPDATES)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
