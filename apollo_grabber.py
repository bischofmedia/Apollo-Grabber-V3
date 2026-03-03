"""
RTC Apollo-Grabber V2
Block A: Imports, Configuration, State Management, Flask Health Server
"""

import asyncio
import json
import logging
import math
import os
import random
import re
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
from flask import Flask

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ApolloGrabber")

BERLIN = ZoneInfo("Europe/Berlin")

# ─────────────────────────────────────────────
# Constants / Paths
# ─────────────────────────────────────────────
STATE_FILE       = Path("state.json")
EVENT_LOG_FILE   = Path("event_log.txt")
DISCORD_LOG_FILE = Path("discord_log.txt")
ANMELDUNGEN_FILE = Path("anmeldungen.txt")

DISCORD_API = "https://discord.com/api/v10"

# ─────────────────────────────────────────────
# Environment variable helpers
# ─────────────────────────────────────────────

def _env(key: str, default=None) -> str:
    return os.environ.get(key, default)

def _env_msg(key: str, default: str = "") -> str:
    """Like _env() but converts literal \\n sequences to real newlines (for message templates)."""
    val = os.environ.get(key, default)
    return val.replace("\\n", "\n") if val else default

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default

# ─────────────────────────────────────────────
# Static (non-var_) environment variables
# ─────────────────────────────────────────────
DISCORD_TOKEN_APOLLOGRABBER    = _env("DISCORD_TOKEN_APOLLOGRABBER", "")
DISCORD_TOKEN_LOBBYCODEGRABBER = _env("DISCORD_TOKEN_LOBBYCODEGRABBER", "")
USER_ID_ORGA      = [u.strip() for u in _env("USER_ID_ORGA", "").split(";") if u.strip()]
DISCORD_ID_APOLLO = _env("DISCORD_ID_APOLLO", "")
DISCORD_GUILD_ID  = _env("DISCORD_GUILD_ID", "")

CHAN_APOLLO = _env("CHAN_APOLLO", "")
CHAN_LOG    = _env("CHAN_LOG", "")
CHAN_NEWS   = _env("CHAN_NEWS", "")
CHAN_CODES  = _env("CHAN_CODES", "")
# FIX #5: resolved after CHAN_LOG is set, never evaluated before it
CHAN_ORDERS = _env("CHAN_ORDERS") or _env("CHAN_LOG", "")

DRIVERS_PER_GRID          = _env_int("DRIVERS_PER_GRID", 15)
MAX_GRIDS                 = _env_int("MAX_GRIDS", 4)
GOOGLE_SHEETS_ID          = _env("GOOGLE_SHEETS_ID", "")
GOOGLE_CREDENTIALS_FILE   = _env("GOOGLE_CREDENTIALS_FILE", "credentials.json")
CMD_SCAN_INTERVAL_SECONDS = _env_int("CMD_SCAN_INTERVAL_SECONDS", 10)
ENABLE_MULTILANGUAGE      = _env_int("ENABLE_MULTILANGUAGE", 0)

# Message templates – loaded once from env at import time, never from state
MSG_HILFETEXT          = _env_msg("MSG_HILFETEXT", "Kein Hilfetext konfiguriert.")
MSG_LOBBYCODES         = _env_msg("MSG_LOBBYCODES", "Lobby-Codes")
MSG_EXTRA_GRID_TEXT    = _env_msg("MSG_EXTRA_GRID_TEXT", "")
MSG_EXTRA_GRID_TEXT_EN = _env_msg("MSG_EXTRA_GRID_TEXT_EN", "")
MSG_GRID_FULL_TEXT     = _env_msg("MSG_GRID_FULL_TEXT", "")
MSG_GRID_FULL_TEXT_EN  = _env_msg("MSG_GRID_FULL_TEXT_EN", "")
MSG_MOVED_UP_SINGLE    = _env_msg("MSG_MOVED_UP_SINGLE", "")
MSG_MOVED_UP_SINGLE_EN = _env_msg("MSG_MOVED_UP_SINGLE_EN", "")
MSG_MOVED_UP_MULTI     = _env_msg("MSG_MOVED_UP_MULTI", "")
MSG_MOVED_UP_MULTI_EN  = _env_msg("MSG_MOVED_UP_MULTI_EN", "")
MSG_SUNDAY_TEXT        = _env_msg("MSG_SUNDAY_TEXT", "")
MSG_SUNDAY_TEXT_EN     = _env_msg("MSG_SUNDAY_TEXT_EN", "")
MSG_WAITLIST_SINGLE    = _env_msg("MSG_WAITLIST_SINGLE", "")
MSG_WAITLIST_SINGLE_EN = _env_msg("MSG_WAITLIST_SINGLE_EN", "")
MSG_WAITLIST_MULTI     = _env_msg("MSG_WAITLIST_MULTI", "")
MSG_WAITLIST_MULTI_EN  = _env_msg("MSG_WAITLIST_MULTI_EN", "")
MSG_NEW_EVENT          = _env_msg("MSG_NEW_EVENT", "")
MSG_NEW_EVENT_EN       = _env_msg("MSG_NEW_EVENT_EN", "")

# ─────────────────────────────────────────────
# var_* keys – persisted in state.json, changeable via !set
# FIX #9: var_SET_MSG_MOVED_UP_TEXT is initialised from env on first run
# ─────────────────────────────────────────────
VAR_KEYS = [
    "var_ENABLE_EXTRA_GRID",
    "var_ENABLE_MOVED_UP_MSG",
    "var_ENABLE_NEWS_CLEANUP",
    "var_ENABLE_SUNDAY_MSG",
    "var_ENABLE_WAITLIST_MSG",
    "var_EXTRA_GRID_THRESHOLD",
    "var_POLL_INTERVAL_SECONDS",
    "var_REGISTRATION_END_TIME",
    "var_ENABLE_DELETE_OLD_EVENT",
    "var_SET_MIN_GRIDS_MSG",
    "var_ENABLE_EXTRA_GRID_MSG",
    "var_ENABLE_GRID_FULL_MSG",
    "var_SET_MSG_MOVED_UP_TEXT",
    "var_SET_NEW_EVENT_MSG",
]

VAR_ENV_MAP = {k: k[4:] for k in VAR_KEYS}  # "var_ENABLE_X" -> "ENABLE_X"

# ─────────────────────────────────────────────
# Default state structure
# ─────────────────────────────────────────────
DEFAULT_STATE: dict = {
    # Event identity
    "event_id": "0",
    "event_title": "",
    "event_datetime": "",
    "new_event": 0,
    # FIX #8: stores the specific Monday that is the registration deadline
    "registration_end_monday": "",  # "YYYY-MM-DD"
    # Driver tracking
    "drivers": [],
    "driver_status": {},
    # Grid logic
    "manual_grids": None,
    "man_lock": False,
    "sunday_lock": False,
    "last_grid_count": 0,
    "grid_lock_override": False,
    # Discord
    "log_id": "",
    "last_sync_sheets": "",
    # Triggers
    "sunday_msg_sent": False,
    "registration_end_logged": False,
    "ignored_drivers": [],
    # var_* settings (defaults; overridden by env vars on first run)
    "var_ENABLE_EXTRA_GRID": 0,
    "var_ENABLE_MOVED_UP_MSG": 1,
    "var_ENABLE_NEWS_CLEANUP": 1,
    "var_ENABLE_SUNDAY_MSG": 1,
    "var_ENABLE_WAITLIST_MSG": 1,
    "var_EXTRA_GRID_THRESHOLD": 3,
    "var_POLL_INTERVAL_SECONDS": 60,
    "var_REGISTRATION_END_TIME": "23:59",
    "var_ENABLE_DELETE_OLD_EVENT": 0,
    "var_SET_MIN_GRIDS_MSG": 2,
    "var_ENABLE_EXTRA_GRID_MSG": 1,
    "var_ENABLE_GRID_FULL_MSG": 1,
    "var_SET_MSG_MOVED_UP_TEXT": 1,
    "var_SET_NEW_EVENT_MSG": 1,
}

# ─────────────────────────────────────────────
# Runtime state – single shared dict
# ─────────────────────────────────────────────
state: dict = {}


def _coerce_var(var_key: str, raw: str):
    """Convert a raw string value to the correct Python type for a var_ key."""
    int_keys = {
        "var_ENABLE_EXTRA_GRID", "var_ENABLE_MOVED_UP_MSG",
        "var_ENABLE_NEWS_CLEANUP", "var_ENABLE_SUNDAY_MSG",
        "var_ENABLE_WAITLIST_MSG", "var_EXTRA_GRID_THRESHOLD",
        "var_POLL_INTERVAL_SECONDS", "var_ENABLE_DELETE_OLD_EVENT",
        "var_SET_MIN_GRIDS_MSG", "var_ENABLE_EXTRA_GRID_MSG",
        "var_ENABLE_GRID_FULL_MSG", "var_SET_MSG_MOVED_UP_TEXT",
        "var_SET_NEW_EVENT_MSG",
    }
    if var_key in int_keys:
        try:
            return int(raw)
        except (ValueError, TypeError):
            return 0
    return str(raw)  # e.g. var_REGISTRATION_END_TIME stays "hh:mm"


def load_state() -> None:
    """
    Load state.json into the global state dict.
    On first run (no file): initialise defaults, populate var_* from env, save.
    FIX #10: called ONLY from __main__, never from worker().
    """
    global state
    if STATE_FILE.exists():
        try:
            with STATE_FILE.open("r", encoding="utf-8") as f:
                loaded = json.load(f)
            state = {**DEFAULT_STATE, **loaded}
            log.info("state.json geladen.")
            return
        except Exception as e:
            log.warning(f"state.json fehlerhaft, neu initialisieren: {e}")
    # Fresh init – populate var_* from environment variables
    state = dict(DEFAULT_STATE)
    for var_key, env_key in VAR_ENV_MAP.items():
        val = os.environ.get(env_key)
        if val is not None:
            state[var_key] = _coerce_var(var_key, val)
    save_state()


def save_state() -> None:
    """Atomically persist current state to state.json."""
    tmp = STATE_FILE.with_suffix(".tmp")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        tmp.replace(STATE_FILE)
    except Exception as e:
        log.error(f"Fehler beim Speichern der state.json: {e}")


# ─────────────────────────────────────────────
# Convenience accessor for var_* settings
# ─────────────────────────────────────────────
def cfg(key: str):
    """Return the current runtime value of var_<key>."""
    return state.get(f"var_{key}", DEFAULT_STATE.get(f"var_{key}"))


# ─────────────────────────────────────────────
# File helpers
# ─────────────────────────────────────────────
def ensure_files() -> None:
    """Create all required data files if they do not exist."""
    for fp in (EVENT_LOG_FILE, DISCORD_LOG_FILE, ANMELDUNGEN_FILE):
        if not fp.exists():
            fp.write_text("", encoding="utf-8")


def clear_file(fp: Path) -> None:
    fp.write_text("", encoding="utf-8")


def append_event_log(line: str) -> None:
    """Append a single line to event_log.txt."""
    with EVENT_LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def read_event_log() -> str:
    if EVENT_LOG_FILE.exists():
        return EVENT_LOG_FILE.read_text(encoding="utf-8")
    return ""


def read_discord_log() -> str:
    if DISCORD_LOG_FILE.exists():
        return DISCORD_LOG_FILE.read_text(encoding="utf-8")
    return ""


def write_discord_log(content: str) -> None:
    DISCORD_LOG_FILE.write_text(content, encoding="utf-8")


def write_anmeldungen(drivers: list) -> None:
    """
    FIX #1: Write current driver list to anmeldungen.txt.
    One name per line. >>> already stripped by parse_apollo_embed();
    all other characters (incl. backslashes) are preserved.
    Called whenever the embed content changes (trigger_make=1).
    """
    ANMELDUNGEN_FILE.write_text("\n".join(drivers), encoding="utf-8")


# ─────────────────────────────────────────────
# Time helpers
# ─────────────────────────────────────────────
def now_berlin() -> datetime:
    return datetime.now(BERLIN)


def ts_str() -> str:
    """'WD hh:mm' in Berlin time, e.g. 'Mo 19:45'."""
    n = now_berlin()
    days = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]
    return f"{days[n.weekday()]} {n.strftime('%H:%M')}"


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _next_monday_date(reference: datetime) -> str:
    """
    Return the ISO date string ("YYYY-MM-DD") of the next Monday
    strictly after `reference`. If reference is itself a Monday,
    the FOLLOWING Monday is returned (event week runs Tue–Mon).
    """
    days_ahead = (7 - reference.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    target = (reference + timedelta(days=days_ahead)).date()
    return target.isoformat()


def set_registration_end_monday() -> None:
    """
    FIX #8: Compute and store the target Monday the first time a new event
    is seen. REGISTRATION_END_TIME always refers to this specific date.
    Called once per event cycle, idempotent if already set.
    """
    if state.get("registration_end_monday"):
        return
    monday_str = _next_monday_date(now_berlin())
    state["registration_end_monday"] = monday_str
    log.info(f"Anmeldeschluss-Montag gesetzt: {monday_str}")


def registration_end_passed() -> bool:
    """
    FIX #8: True only when today matches registration_end_monday
    AND Berlin time >= var_REGISTRATION_END_TIME (hh:mm).
    """
    ref_str = state.get("registration_end_monday", "")
    if not ref_str:
        return False
    n = now_berlin()
    try:
        ref_date = datetime.strptime(ref_str, "%Y-%m-%d").date()
    except ValueError:
        return False
    if n.date() != ref_date:
        return False
    end_str = str(state.get("var_REGISTRATION_END_TIME", "23:59"))
    try:
        h, m = map(int, end_str.split(":"))
    except Exception:
        return False
    return n.hour > h or (n.hour == h and n.minute >= m)


def is_event_deletion_window() -> bool:
    """True only on Tuesday between 09:45 and 10:15 Berlin time."""
    n = now_berlin()
    if n.weekday() != 1:  # 1 = Dienstag
        return False
    return (n.hour == 9 and n.minute >= 45) or (n.hour == 10 and n.minute < 15)



    """True on Sunday >= 18:00 Berlin, or any time on Monday."""
    n = now_berlin()
    if n.weekday() == 6 and n.hour >= 18:
        return True
    if n.weekday() == 0:
        return True
    return False


# ─────────────────────────────────────────────
# Flask Health Server
# ─────────────────────────────────────────────
flask_app = Flask("ApolloGrabberHealth")


@flask_app.route("/")
@flask_app.route("/dashboard")
def dashboard():
    grids = state.get("last_grid_count", 0)
    return build_html_dashboard(grids), 200


@flask_app.route("/health")
def health():
    return "Apollo Grabber running", 200


def start_flask() -> None:
    port = int(os.environ.get("PORT", 8080))
    flask_app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)


def launch_flask_thread() -> None:
    t = threading.Thread(target=start_flask, daemon=True, name="FlaskHealth")
    t.start()
    log.info("Flask Health Server gestartet.")


import math
import re
import aiohttp
from datetime import datetime


# ─────────────────────────────────────────────
# Low-level Discord REST helpers
# ─────────────────────────────────────────────

def _auth(token: str) -> dict:
    return {"Authorization": f"Bot {token}", "Content-Type": "application/json"}


async def discord_get(session: aiohttp.ClientSession, path: str, token: str) -> dict | list | None:
    url = f"{DISCORD_API}{path}"
    try:
        async with session.get(url, headers=_auth(token)) as r:
            if r.status == 200:
                return await r.json()
            log.warning(f"GET {path} -> {r.status}")
            return None
    except Exception as e:
        log.error(f"discord_get {path}: {e}")
        return None


async def discord_post(session: aiohttp.ClientSession, path: str, token: str, payload: dict) -> dict | None:
    url = f"{DISCORD_API}{path}"
    try:
        async with session.post(url, headers=_auth(token), json=payload) as r:
            if r.status in (200, 201):
                return await r.json()
            text = await r.text()
            log.warning(f"POST {path} -> {r.status}: {text[:200]}")
            return None
    except Exception as e:
        log.error(f"discord_post {path}: {e}")
        return None


async def discord_patch(session: aiohttp.ClientSession, path: str, token: str, payload: dict) -> dict | None:
    url = f"{DISCORD_API}{path}"
    try:
        async with session.patch(url, headers=_auth(token), json=payload) as r:
            if r.status == 200:
                return await r.json()
            text = await r.text()
            log.warning(f"PATCH {path} -> {r.status}: {text[:200]}")
            return None
    except Exception as e:
        log.error(f"discord_patch {path}: {e}")
        return None


async def discord_delete(session: aiohttp.ClientSession, path: str, token: str) -> bool:
    url = f"{DISCORD_API}{path}"
    try:
        async with session.delete(url, headers=_auth(token)) as r:
            return r.status in (200, 204)
    except Exception as e:
        log.error(f"discord_delete {path}: {e}")
        return False


async def get_channel_messages(
    session: aiohttp.ClientSession,
    channel_id: str,
    token: str,
    limit: int = 200,
) -> list:
    """
    Fetch up to `limit` messages from a channel via pagination.
    FIX #11: limit parameter is now enforced; pagination stops at `limit` messages.
    """
    messages = []
    last_id = None
    while len(messages) < limit:
        batch_size = min(100, limit - len(messages))
        path = f"/channels/{channel_id}/messages?limit={batch_size}"
        if last_id:
            path += f"&before={last_id}"
        batch = await discord_get(session, path, token)
        if not batch:
            break
        messages.extend(batch)
        if len(batch) < batch_size:
            break
        last_id = batch[-1]["id"]
    return messages


async def delete_all_bot_messages(
    session: aiohttp.ClientSession,
    channel_id: str,
    token: str,
    bot_user_id: str | None = None,
) -> None:
    """Delete all messages in the channel posted by this bot."""
    messages = await get_channel_messages(session, channel_id, token)
    for msg in messages:
        author_id = msg.get("author", {}).get("id", "")
        if bot_user_id is None or author_id == bot_user_id:
            await discord_delete(session, f"/channels/{channel_id}/messages/{msg['id']}", token)


async def delete_all_messages(
    session: aiohttp.ClientSession,
    channel_id: str,
    token: str,
) -> None:
    """Delete ALL messages in a channel (Ausnahme: CHAN_CODES Bereinigung)."""
    messages = await get_channel_messages(session, channel_id, token)
    for msg in messages:
        await discord_delete(session, f"/channels/{channel_id}/messages/{msg['id']}", token)


async def get_bot_user_id(session: aiohttp.ClientSession, token: str) -> str | None:
    data = await discord_get(session, "/users/@me", token)
    return data.get("id") if data else None


async def get_display_name(
    session: aiohttp.ClientSession,
    user_id: str,
    fallback: str,
) -> str:
    """Gibt den Server-Nickname zurück, falls vorhanden, sonst den globalen Username."""
    if not DISCORD_GUILD_ID:
        return fallback
    data = await discord_get(
        session,
        f"/guilds/{DISCORD_GUILD_ID}/members/{user_id}",
        DISCORD_TOKEN_APOLLOGRABBER,
    )
    if data:
        return data.get("nick") or data.get("user", {}).get("global_name") or fallback
    return fallback


async def message_exists(
    session: aiohttp.ClientSession,
    channel_id: str,
    message_id: str,
    token: str,
) -> bool:
    """
    Check whether a message still exists in a channel.
    Uses channel history search instead of the direct message endpoint,
    because Discord's GET /messages/{id} can return 404 for recently posted
    messages due to propagation delay, even though the message is visible.
    """
    messages = await get_channel_messages(session, channel_id, token, limit=50)
    return any(m["id"] == message_id for m in messages)


# ─────────────────────────────────────────────
# Apollo Embed Parsing
# ─────────────────────────────────────────────

def _clean_name(name: str) -> str:
    """Remove >>> from a name; all other characters (incl. backslashes) stay intact."""
    return name.replace(">>>", "").strip()


def parse_apollo_embed(message: dict) -> dict:
    """
    Extract title, event_datetime, and driver list from an Apollo embed message.
    Returns: {"title": str, "event_datetime": str, "drivers": [str, ...]}
    drivers list contains raw names (>>> stripped, backslashes preserved).
    """
    result = {"title": "", "event_datetime": "", "drivers": []}
    embeds = message.get("embeds", [])
    if not embeds:
        return result

    embed = embeds[0]
    result["title"] = embed.get("title", "") or embed.get("description", "")

    # Apollo places the event datetime in the description as a Discord timestamp
    description = embed.get("description", "")
    ts_match = re.search(r"<t:(\d+)(?::[A-Za-z])?>", description)
    if ts_match:
        epoch = int(ts_match.group(1))
        from zoneinfo import ZoneInfo
        dt = datetime.fromtimestamp(epoch, tz=ZoneInfo("Europe/Berlin"))
        result["event_datetime"] = dt.strftime("%d.%m.%Y %H:%M")
    else:
        result["event_datetime"] = description[:100]

    # Driver list: Fields[1..n] (Fields[0] is the header / "Accepted" field)
    fields = embed.get("fields", [])
    drivers = []
    for field in fields[1:]:
        for line in field.get("value", "").splitlines():
            name = _clean_name(line)
            if name:
                drivers.append(name)
    result["drivers"] = drivers
    return result


async def find_apollo_message(session: aiohttp.ClientSession, token: str) -> dict | None:
    """
    Return the newest Apollo-bot message in CHAN_APOLLO that carries an event embed.

    Criteria:
    - Author is DISCORD_ID_APOLLO
    - Message has at least one embed (the event embed)
    - Message is NOT a thread-starter post (Apollo may open reminder threads;
      those have a 'thread' key on the message object and must be ignored)

    'Newest' = largest snowflake ID = most recently created message.
    This ensures that when a new event is posted alongside a still-existing old
    event, the current event is always picked up correctly.
    """
    messages = await get_channel_messages(session, CHAN_APOLLO, token)
    apollo_msgs = [
        m for m in messages
        if m.get("author", {}).get("id") == DISCORD_ID_APOLLO
        and m.get("embeds")          # must have at least one embed
        and not m.get("thread")      # must not be a thread-starter post
    ]
    if not apollo_msgs:
        return None
    return max(apollo_msgs, key=lambda m: int(m["id"]))


# ─────────────────────────────────────────────
# Grid Calculation  (Block 8)
# ─────────────────────────────────────────────

def calculate_grids(driver_count: int) -> int:
    """Compute grid count from driver count, capped at MAX_GRIDS."""
    if driver_count <= 0:
        return 0
    return min(math.ceil(driver_count / DRIVERS_PER_GRID), MAX_GRIDS)


def grid_capacity(grids: int) -> int:
    return grids * DRIVERS_PER_GRID


def recalculate_grids(driver_count: int) -> int:
    """
    Compute the new grid count respecting sunday_lock / man_lock / grid_lock_override.
    Updates state["last_grid_count"] and resets grid_lock_override after use.
    FIX #11: After resetting grid_lock_override, sunday_lock/man_lock resume control.
    """
    override = state.get("grid_lock_override", False)
    sunday_lock = state.get("sunday_lock", False)
    man_lock = state.get("man_lock", False)

    if (sunday_lock or man_lock) and not override:
        # Locked – return current count unchanged
        return int(state.get("last_grid_count", 0))

    new_count = calculate_grids(driver_count)
    state["last_grid_count"] = new_count
    if override:
        # Single use: reset override so locks take effect again next cycle
        state["grid_lock_override"] = False
    return new_count


def check_extra_grid(driver_count: int, current_grids: int) -> bool:
    """
    Block 8.3: If ENABLE_EXTRA_GRID=1 and waitlist count >= EXTRA_GRID_THRESHOLD,
    set grid_lock_override=True and return True so the pipeline triggers a recalc.
    The grid lock variables themselves are NOT changed.
    """
    if not int(cfg("ENABLE_EXTRA_GRID")):
        return False
    capacity = grid_capacity(current_grids)
    waitlist_count = max(0, driver_count - capacity)
    threshold = int(cfg("EXTRA_GRID_THRESHOLD"))
    if waitlist_count >= threshold and current_grids < MAX_GRIDS:
        state["grid_lock_override"] = True
        return True
    return False


# ─────────────────────────────────────────────
# Driver status classification
# ─────────────────────────────────────────────

def classify_drivers(drivers: list, grids: int) -> dict:
    """Assign 'grid' or 'waitlist' to each driver based on position and capacity."""
    capacity = grid_capacity(grids)
    return {name: ("grid" if i < capacity else "waitlist") for i, name in enumerate(drivers)}


# ─────────────────────────────────────────────
# Event log delta processing  (Block 2)
# ─────────────────────────────────────────────

def process_driver_changes(
    new_drivers: list,
    old_drivers: list,
    new_grids: int,
    reg_end: bool,
) -> dict:
    """
    Compare new_drivers against old_drivers. Append transition lines to event_log.txt.

    Log format:
      🟢 Name              new sign-up into grid
      🟡 Name              new sign-up onto waitlist
      🔴 Name              cancellation
      🔴🔴 Name          post-deadline sign-up (ignored)
      🟢 -> 🟡 Name     grid demoted to waitlist
      🟡 -> 🟢 Name     waitlist promoted to grid (move-up)
      🔴 -> 🟢 Name     re-registration into grid
      🔴 -> 🟡 Name     re-registration onto waitlist

    Move-ups ARE written to event_log.txt so !clean log can include them.
    """
    capacity  = grid_capacity(new_grids)
    old_set   = set(old_drivers)
    new_set   = set(new_drivers)
    old_status = state.get("driver_status", {})

    added      = []
    removed    = []
    moved_up   = []
    waitlisted = []
    ignored    = []

    # Removals
    for name in old_drivers:
        if name not in new_set:
            removed.append(name)
            prev = old_status.get(name)
            if prev == "grid":
                append_event_log(f"{ts_str()} 🟢 -> 🔴 {name}")
            elif prev == "waitlist":
                append_event_log(f"{ts_str()} 🟡 -> 🔴 {name}")
            else:
                append_event_log(f"{ts_str()} 🔴 {name}")

    # Additions and status changes
    for i, name in enumerate(new_drivers):
        cur = "grid" if i < capacity else "waitlist"
        prev = old_status.get(name)

        if name not in old_set:
            # New driver
            if reg_end:
                # Only log once – if already in state["ignored_drivers"], skip
                already_ignored = name in state.get("ignored_drivers", [])
                ignored.append(name)
                if not already_ignored:
                    append_event_log(f"{ts_str()} 🔴🔴 {name}")
                continue
            if cur == "grid":
                added.append(name)
                if prev is not None:
                    append_event_log(f"{ts_str()} 🔴 -> 🟢 {name}")
                else:
                    append_event_log(f"{ts_str()} 🟢 {name}")
            else:
                waitlisted.append(name)
                if prev is not None:
                    append_event_log(f"{ts_str()} 🔴 -> 🟡 {name}")
                else:
                    append_event_log(f"{ts_str()} 🟡 {name}")
        else:
            # Existing driver – check for status change
            if prev == "waitlist" and cur == "grid":
                moved_up.append(name)
                append_event_log(f"{ts_str()} 🟡 -> 🟢 {name}")
            elif prev == "grid" and cur == "waitlist":
                waitlisted.append(name)
                append_event_log(f"{ts_str()} 🟢 -> 🟡 {name}")
            # unchanged: no entry

    return {
        "added":      added,
        "removed":    removed,
        "moved_up":   moved_up,
        "waitlisted": waitlisted,
        "ignored":    ignored,
    }


# ─────────────────────────────────────────────
# discord_log.txt generation  (Block 6)
# ─────────────────────────────────────────────

def build_discord_log(grids: int) -> str:
    """
    Rebuild discord_log.txt from event_log.txt.
    All status transitions are already written in arrow format by
    process_driver_changes(), so this function just strips backslashes
    and passes lines through. System lines (⚙️) pass through unchanged.
    """
    raw_lines = read_event_log().splitlines()
    out_lines = [line.replace("\\", "") for line in raw_lines]
    return "\n".join(out_lines)


def build_clean_log(grids: int, username: str = "") -> str:
    """
    Build a compacted log for !clean log:
    - All system lines are removed
    - For each driver only their CURRENT status is kept (from state["driver_status"])
    - Each driver appears exactly once with the timestamp of their last status line
    - A single system line ⚙️ Clean Log is prepended with current timestamp
    """
    current_status = state.get("driver_status", {})

    CIRCLE_EMOJIS = ["🟢", "🟡", "🔴"]

    def _extract_name_and_ts(line: str):
        """Return (timestamp_str, name) from a status line, or (None, None)."""
        parts = line.split()
        if len(parts) < 3:
            return None, None
        try:
            last_emoji_idx = max(
                i for i, p in enumerate(parts)
                if any(e in p for e in CIRCLE_EMOJIS)
            )
            name = " ".join(parts[last_emoji_idx + 1:]).strip()
            # Timestamp is first two tokens: "Mo 21:05"
            ts = f"{parts[0]} {parts[1]}" if len(parts) >= 2 else ""
            return ts, name
        except (ValueError, IndexError):
            return None, None

    # Walk event_log: track first occurrence order and last known timestamp per driver
    seen: list = []
    seen_set: set = set()
    last_ts: dict = {}  # driver -> most recent timestamp string

    for line in read_event_log().splitlines():
        if not any(e in line for e in CIRCLE_EMOJIS):
            continue
        ts_val, name = _extract_name_and_ts(line)
        if not name:
            continue
        if name not in seen_set:
            seen.append(name)
            seen_set.add(name)
        if ts_val:
            last_ts[name] = ts_val

    fallback_ts = ts_str()
    by = f" by {username}" if username else ""
    lines = [f"{fallback_ts} ⚙️ Clean Log{by}"]
    for name in seen:
        driver_ts = last_ts.get(name, fallback_ts)
        status = current_status.get(name)
        if status == "grid":
            lines.append(f"{driver_ts} 🟢 {name}")
        elif status == "waitlist":
            lines.append(f"{driver_ts} 🟡 {name}")
        else:
            lines.append(f"{driver_ts} 🔴 {name}")
    return "\n".join(lines)


# ─────────────────────────────────────────────
# Discord log post builder  (Block 4)
# ─────────────────────────────────────────────

def _registration_status(grids: int) -> tuple[str, str]:
    """
    Returns (emoji, label) for the current registration status.

    🟢 Anmeldung offen       – next sign-up goes directly into a grid slot
    🟡 Anmeldung auf Warteliste – all grids full/locked, deadline not yet reached
    🔴 Anmeldung gesperrt    – final registration deadline passed
    """
    if registration_end_passed():
        return "🔴", "Anmeldung gesperrt"
    capacity     = grid_capacity(grids)
    driver_count = len(state.get("drivers", []))
    locked       = state.get("sunday_lock") or state.get("man_lock")
    max_capacity = grid_capacity(MAX_GRIDS)
    # Open: free slots still available and not locked beyond max
    if driver_count < capacity and not locked:
        return "🟢", "Anmeldung offen"
    if driver_count < max_capacity and not locked:
        return "🟢", "Anmeldung offen"
    return "🟡", "Anmeldung auf Warteliste"


def _status_emoji(grids: int) -> str:
    """Legacy single-emoji accessor (used elsewhere)."""
    emoji, _ = _registration_status(grids)
    return emoji


def _berlin_ts(iso_str: str) -> str:
    """Convert a UTC ISO string to 'WD hh:mm' in Berlin time."""
    if not iso_str:
        return "–"
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        dt = datetime.fromisoformat(iso_str).astimezone(ZoneInfo("Europe/Berlin"))
        days = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]
        return f"{days[dt.weekday()]} {dt.strftime('%H:%M')}"
    except Exception:
        return iso_str


def build_log_payload(grids: int) -> dict:
    """
    Build the full Discord message payload for CHAN_LOG.

    Structure:
      content    : 4-line header + status
      embeds[0]  : log list as codeblock
      embeds[1]  : Stand / Sync footer

    Header lines:
      1. Registration status (emoji + label)
      2. Event title (bold)
      3. Event datetime
      4. Fahrer | Grids | Lock symbol
    """
    status_emoji, status_label = _registration_status(grids)
    title        = state.get("event_title", "–")
    ev_dt        = state.get("event_datetime", "–")
    driver_count = len(state.get("drivers", []))
    stand        = ts_str()
    last_sync    = _berlin_ts(state.get("last_sync_sheets", ""))

    locked      = state.get("sunday_lock") or state.get("man_lock")
    lock_symbol = " 🔒" if locked else ""

    content = (
        f"{status_emoji} {status_label}\n"
        f"**{title}**\n"
        f"{ev_dt}\n"
        f"Fahrer: {driver_count} | Grids: {grids}{lock_symbol}"
    )

    # Log embed – codeblock, line-based truncation to 4096 chars
    raw_log = read_discord_log()
    wrapper_overhead = len("```\n") + len("\n```")
    max_desc = 4096 - wrapper_overhead
    if len(raw_log) > max_desc:
        lines = raw_log.splitlines()
        while lines and len("\n".join(lines)) > max_desc - len("[...]\n"):
            lines.pop(0)
        raw_log = "[...]\n" + "\n".join(lines)

    log_embed = {
        "description": f"```\n{raw_log or '–'}\n```",
    }

    footer_embed = {
        "description": f"Stand: {stand} | Sync: {last_sync}",
    }

    return {"content": content, "embeds": [log_embed, footer_embed]}


async def post_or_update_log(session: aiohttp.ClientSession, payload: dict) -> None:
    """
    Update the log in CHAN_LOG:
    1. Fetch channel history, find the oldest message whose first embed has
       title "Stand" – this is our log post identifier.
    2. PATCH it with the new payload.
    3. If not found (or PATCH fails): POST fresh.
    Never deletes – cleanup is handled exclusively by clear_chan_log().
    """
    messages = await get_channel_messages(session, CHAN_LOG, DISCORD_TOKEN_APOLLOGRABBER)

    # Identify log post: bot message with exactly 2 embeds (log + footer).
    # Command response messages have 0 or 1 embed and are never overwritten.
    log_msgs = [
        m for m in messages
        if m.get("author", {}).get("bot") is True
        and len(m.get("embeds", [])) == 2
    ]

    if log_msgs:
        # If multiple log posts exist (shouldn't happen), delete the extras first.
        if len(log_msgs) > 1:
            for extra in sorted(log_msgs, key=lambda m: int(m["id"]))[1:]:
                await discord_delete(
                    session,
                    f"/channels/{CHAN_LOG}/messages/{extra['id']}",
                    DISCORD_TOKEN_APOLLOGRABBER,
                )
                log.info(f"Doppelter Log-Post gelöscht: {extra['id']}")
        target = min(log_msgs, key=lambda m: int(m["id"]))
        log.debug(f"PATCH Log-Post {target['id']}")
        result = await discord_patch(
            session,
            f"/channels/{CHAN_LOG}/messages/{target['id']}",
            DISCORD_TOKEN_APOLLOGRABBER,
            payload,
        )
        if result:
            state["log_id"] = target["id"]
            save_state()
            return
        log.debug("PATCH fehlgeschlagen – poste neu (Fallback).")

    log.info("Kein Log-Post gefunden – poste neu.")
    msg = await discord_post(
        session,
        f"/channels/{CHAN_LOG}/messages",
        DISCORD_TOKEN_APOLLOGRABBER,
        payload,
    )
    if msg:
        state["log_id"] = msg["id"]
        save_state()
        log.info(f"Log-Post erstellt: {msg['id']}")


# ─────────────────────────────────────────────
# Google Sheets Sync  (Block 3)
# Ersetzt make.com – schreibt direkt via gspread
#
# Spreadsheet-Layout (Sheet "Apollo-Grabber"):
#   Zeile 1, Spalte B  → Letzte Änderung (Timestamp)
#   Zeile 1, Spalte D  → Anzahl Grids
#   Zeile 1, Spalte Q  → Fahrerliste (newline-getrennt)
#   Zeile 1, Spalte F  → Grid Override (wird nach Update auf "0" zurückgesetzt)
#
# Bei event_type == "cleancodes":
#   Sheet "LobbyCodes", Range A2:C500 → wird geleert
# ─────────────────────────────────────────────

def _get_gspread_client():
    """Initialisiert einen gspread-Client mit dem Service-Account aus GOOGLE_CREDENTIALS_FILE."""
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)


async def sync_to_sheets(session: aiohttp.ClientSession, event_type: str) -> None:
    """
    Schreibt Fahrerdaten direkt in das Google Sheet.
    Läuft in einem ThreadPoolExecutor, um den asyncio-Loop nicht zu blockieren.
    """
    if not GOOGLE_SHEETS_ID:
        log.warning("GOOGLE_SHEETS_ID nicht gesetzt – Sheet-Sync übersprungen.")
        return

    grids = int(state.get("last_grid_count", 0))
    drivers = list(state.get("drivers", []))
    now_str = datetime.now(BERLIN).strftime("%d.%m.%Y %H:%M")

    def _do_sync():
        gc = _get_gspread_client()
        sh = gc.open_by_key(GOOGLE_SHEETS_ID)

        # ── Sheet "Apollo-Grabber", Zeile 1 aktualisieren ──────────────────
        ws = sh.worksheet("Apollo-Grabber")

        # Spalte B: Timestamp, Spalte D: Grids, Spalte Q: Fahrerliste
        ws.update(
            range_name="B1",
            values=[[f"Letzte Änderung:\n{now_str} Uhr"]],
            value_input_option="USER_ENTERED",
        )
        ws.update(
            range_name="D1",
            values=[[grids]],
            value_input_option="USER_ENTERED",
        )
        ws.update(
            range_name="Q1",
            values=[["\n".join(drivers)]],
            value_input_option="USER_ENTERED",
        )
        # Grid Override (Spalte F) auf 0 zurücksetzen
        ws.update(
            range_name="F1",
            values=[[0]],
            value_input_option="USER_ENTERED",
        )
        log.info(f"Google Sheets aktualisiert (Apollo-Grabber, type={event_type}).")

        # ── Bei neuem Event: LobbyCodes leeren ─────────────────────────────
        if event_type == "cleancodes":
            lc = sh.worksheet("LobbyCodes")
            lc.batch_clear(["A2:C500"])
            log.info("Google Sheets: LobbyCodes A2:C500 geleert.")

    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, _do_sync)
        state["last_sync_sheets"] = iso_now()
    except Exception as e:
        log.error(f"Google Sheets Sync Fehler: {e}")


# ─────────────────────────────────────────────
# Lobby Code Cleanup  (Block 10)
# ─────────────────────────────────────────────

async def clean_lobby_codes(session: aiohttp.ClientSession) -> None:
    """Delete ALL messages in CHAN_CODES and post MSG_LOBBYCODES. Sync to Make."""
    log.info("Lobby-Code Bereinigung gestartet.")
    await delete_all_messages(session, CHAN_CODES, DISCORD_TOKEN_LOBBYCODEGRABBER)
    await discord_post(
        session,
        f"/channels/{CHAN_CODES}/messages",
        DISCORD_TOKEN_LOBBYCODEGRABBER,
        {"content": MSG_LOBBYCODES},
    )
    log.info("Lobby-Code Bereinigung abgeschlossen.")


# ─────────────────────────────────────────────
# CHAN_LOG cleanup (Bootstrap & new event)
# ─────────────────────────────────────────────

async def clear_chan_log(session: aiohttp.ClientSession, bot_user_id: str) -> None:
    """Delete all own messages in CHAN_LOG. Called on bootstrap and new event."""
    await delete_all_bot_messages(session, CHAN_LOG, DISCORD_TOKEN_APOLLOGRABBER, bot_user_id)
    state["log_id"] = ""
    save_state()
    log.info("CHAN_LOG bereinigt.")


# ─────────────────────────────────────────────
# HTML Dashboard  (Block 5)
# ─────────────────────────────────────────────

def build_html_dashboard(grids: int) -> str:
    title        = state.get("event_title", "–")
    ev_dt        = state.get("event_datetime", "–")
    driver_count = len(state.get("drivers", []))
    locked       = state.get("sunday_lock") or state.get("man_lock")
    lock_symbol  = " 🔒" if locked else ""
    actual_grids = int(state.get("last_grid_count", grids))

    status_emoji, status_label = _registration_status(actual_grids)

    discord_log = (
        read_discord_log()
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    return f"""<!DOCTYPE html>
<html lang="de">
<head><meta charset="UTF-8"><title>RTC Apollo Grabber</title>
<style>
  body {{ background: #111; color: #eee; font-family: Arial, sans-serif; padding: 24px; max-width: 860px; margin: 0 auto; }}
  h1 {{ color: #7ec8e3; margin-bottom: 4px; }}
  .status {{ font-size: 1.1em; margin-bottom: 6px; }}
  .event-title {{ font-size: 1.3em; font-weight: bold; margin-bottom: 2px; }}
  .event-dt {{ color: #aaa; margin-bottom: 12px; }}
  .info {{ color: #ccc; margin-bottom: 16px; }}
  .log {{ background: #000; color: #0f0; font-family: monospace; padding: 15px; white-space: pre-wrap; border-radius: 6px; }}
  a {{ color: #7ec8e3; }}
</style>
</head>
<body>
<h1>RTC Apollo Grabber</h1>
<div class="status">{status_emoji} {status_label}</div>
<div class="event-title">{title}</div>
<div class="event-dt">{ev_dt}</div>
<div class="info">Fahrer: {driver_count} &nbsp;|&nbsp; Grids: {actual_grids}{lock_symbol}</div>
<div class="info"><a href="https://cutt.ly/RTC-infos" target="_blank">https://cutt.ly/RTC-infos</a></div>
<div class="log">{discord_log}</div>
</body>
</html>"""


import asyncio
import random
import re
import aiohttp

# ─────────────────────────────────────────────
# Pipeline mutex – guarantees single execution
# ─────────────────────────────────────────────
_pipeline_lock = asyncio.Lock()


# ─────────────────────────────────────────────
# Bilingual message helpers  (Block 9.1)
# ─────────────────────────────────────────────

def _pick_bilingual(de_var: str, en_var: str) -> tuple[str, str]:
    """
    Split de_var and en_var by ';', pick the same random index for both,
    return (de_text, en_text).
    """
    de_opts = [s.strip() for s in de_var.split(";") if s.strip()] if de_var else [""]
    en_opts = [s.strip() for s in en_var.split(";") if s.strip()] if en_var else [""]
    idx = random.randint(0, len(de_opts) - 1) if de_opts else 0
    en_idx = min(idx, len(en_opts) - 1) if en_opts else 0
    return (de_opts[idx] if de_opts else ""), (en_opts[en_idx] if en_opts else "")


def _format_bilingual(de_text: str, en_text: str) -> str:
    parts = []
    if de_text:
        if ENABLE_MULTILANGUAGE:
            parts.append(f"🇩🇪 {de_text}")
        else:
            parts.append(de_text)
    if en_text and ENABLE_MULTILANGUAGE:
        parts.append(f"🇬🇧 {en_text}")
    return "\n".join(parts)


# ─────────────────────────────────────────────
# News message senders  (Block 9)
# ─────────────────────────────────────────────

def _format_names(names: list, conjunction: str = "und") -> str:
    """
    Join a list of driver names with commas, replacing the last comma with conjunction.
    ['A', 'B', 'C'] -> 'A, B und C'
    ['A'] -> 'A'
    """
    clean = [n.replace("\\", "") for n in names]
    if len(clean) <= 1:
        return clean[0] if clean else ""
    return ", ".join(clean[:-1]) + f" {conjunction} " + clean[-1]


# ─────────────────────────────────────────────
# News message senders  (Block 9)
# ─────────────────────────────────────────────

async def send_sunday_msg(session: aiohttp.ClientSession) -> None:
    """Block 9.2 – post Sunday notification once per event."""
    if not int(cfg("ENABLE_SUNDAY_MSG")):
        return
    if state.get("sunday_msg_sent"):
        return
    grids        = int(state.get("last_grid_count", 0))
    driver_count = len(state.get("drivers", []))
    capacity     = grid_capacity(grids)
    free_slots   = max(0, capacity - driver_count)
    de, en = _pick_bilingual(MSG_SUNDAY_TEXT, MSG_SUNDAY_TEXT_EN)
    for tmpl, lang in [(de, "de"), (en, "en")]:
        if lang == "de":
            de = tmpl.replace("{driver_count}", str(driver_count)) \
                     .replace("{grids}", str(grids)) \
                     .replace("{free_slots}", str(free_slots))
        else:
            en = tmpl.replace("{driver_count}", str(driver_count)) \
                     .replace("{grids}", str(grids)) \
                     .replace("{free_slots}", str(free_slots))
    text = _format_bilingual(de, en)
    if text:
        await discord_post(session, f"/channels/{CHAN_NEWS}/messages",
                           DISCORD_TOKEN_APOLLOGRABBER, {"content": text})
    state["sunday_msg_sent"] = True
    save_state()
    log.info("Sonntags-Nachricht gesendet.")


async def send_waitlist_msg(session: aiohttp.ClientSession, names: list) -> None:
    """Block 9.3 – notify when driver(s) land on the waitlist."""
    if not int(cfg("ENABLE_WAITLIST_MSG")) or not names:
        return
    if len(names) == 1:
        de, en = _pick_bilingual(MSG_WAITLIST_SINGLE, MSG_WAITLIST_SINGLE_EN)
        name_de = _format_names(names, "und")
        name_en = _format_names(names, "and")
    else:
        de, en = _pick_bilingual(MSG_WAITLIST_MULTI, MSG_WAITLIST_MULTI_EN)
        name_de = _format_names(names, "und")
        name_en = _format_names(names, "and")
    de = de.replace("{driver_names}", name_de)
    en = en.replace("{driver_names}", name_en)
    text = _format_bilingual(de, en)
    if text:
        await discord_post(session, f"/channels/{CHAN_NEWS}/messages",
                           DISCORD_TOKEN_APOLLOGRABBER, {"content": text})


async def send_moved_up_msg(session: aiohttp.ClientSession, names: list) -> None:
    """Block 9.4 – notify when driver(s) move up from waitlist to grid."""
    if not int(cfg("ENABLE_MOVED_UP_MSG")) or not names:
        return
    if len(names) == 1:
        de, en = _pick_bilingual(MSG_MOVED_UP_SINGLE, MSG_MOVED_UP_SINGLE_EN)
        name_de = _format_names(names, "und")
        name_en = _format_names(names, "and")
    else:
        de, en = _pick_bilingual(MSG_MOVED_UP_MULTI, MSG_MOVED_UP_MULTI_EN)
        name_de = _format_names(names, "und")
        name_en = _format_names(names, "and")
    de = de.replace("{driver_names}", name_de)
    en = en.replace("{driver_names}", name_en)
    text = _format_bilingual(de, en)
    if text:
        await discord_post(session, f"/channels/{CHAN_NEWS}/messages",
                           DISCORD_TOKEN_APOLLOGRABBER, {"content": text})


async def send_grid_full_msg(session: aiohttp.ClientSession, new_grids: int) -> None:
    """Block 9.5 – notify when a new grid is filled (threshold check)."""
    if not int(cfg("ENABLE_GRID_FULL_MSG")):
        return
    if new_grids < int(cfg("SET_MIN_GRIDS_MSG")):
        return
    de, en = _pick_bilingual(MSG_GRID_FULL_TEXT, MSG_GRID_FULL_TEXT_EN)
    de = de.replace("{full_grids}", str(new_grids))
    en = en.replace("{full_grids}", str(new_grids))
    text = _format_bilingual(de, en)
    if text:
        await discord_post(session, f"/channels/{CHAN_NEWS}/messages",
                           DISCORD_TOKEN_APOLLOGRABBER, {"content": text})


async def send_extra_grid_msg(session: aiohttp.ClientSession) -> None:
    """Block 9.6 – notify when an extra grid opens despite a lock."""
    if not int(cfg("ENABLE_EXTRA_GRID_MSG")) or not int(cfg("ENABLE_EXTRA_GRID")):
        return
    grids = int(state.get("last_grid_count", 0))
    de, en = _pick_bilingual(MSG_EXTRA_GRID_TEXT, MSG_EXTRA_GRID_TEXT_EN)
    de = de.replace("{grid}", str(grids))
    en = en.replace("{grid}", str(grids))
    text = _format_bilingual(de, en)
    if text:
        await discord_post(session, f"/channels/{CHAN_NEWS}/messages",
                           DISCORD_TOKEN_APOLLOGRABBER, {"content": text})


async def send_new_event_msg(session: aiohttp.ClientSession) -> None:
    """
    Post a new-event notification to CHAN_NEWS when a genuinely new Apollo event
    is detected. NOT sent on restart or fresh deploy (event_id was "0").
    Controlled by var_SET_NEW_EVENT_MSG (1 = on, 0 = off).
    """
    if not int(cfg("SET_NEW_EVENT_MSG")):
        return
    de, en = _pick_bilingual(MSG_NEW_EVENT, MSG_NEW_EVENT_EN)
    text = _format_bilingual(de, en)
    if text:
        await discord_post(session, f"/channels/{CHAN_NEWS}/messages",
                           DISCORD_TOKEN_APOLLOGRABBER, {"content": text})


# ─────────────────────────────────────────────
# Internal log helpers
# ─────────────────────────────────────────────

def _rebuild_discord_log(grids: int) -> None:
    """Regenerate discord_log.txt from event_log.txt."""
    write_discord_log(build_discord_log(grids))


async def _refresh_chan_log(session: aiohttp.ClientSession) -> None:
    """Push the current log post to CHAN_LOG (patch oldest own post, or new post)."""
    grids = int(state.get("last_grid_count", 0))
    payload = build_log_payload(grids)
    await post_or_update_log(session, payload)


# ─────────────────────────────────────────────
# Command parameter validation  (Block 11)
# ─────────────────────────────────────────────

def _validate_var(param: str, val: str) -> str | None:
    """Return an error string if val is invalid for param, else None."""
    binary = {
        "ENABLE_EXTRA_GRID", "ENABLE_EXTRA_GRID_MSG", "ENABLE_MOVED_UP_MSG",
        "ENABLE_NEWS_CLEANUP", "ENABLE_SUNDAY_MSG", "ENABLE_WAITLIST_MSG",
        "ENABLE_DELETE_OLD_EVENT", "ENABLE_GRID_FULL_MSG", "SET_MSG_MOVED_UP_TEXT",
        "SET_NEW_EVENT_MSG",
    }
    if param in binary:
        if val not in ("0", "1"):
            return f"{param} muss 0 oder 1 sein."
        return None
    if param == "EXTRA_GRID_THRESHOLD":
        try:
            v = int(val)
            if v <= 0 or v > DRIVERS_PER_GRID:
                return f"EXTRA_GRID_THRESHOLD muss zwischen 1 und {DRIVERS_PER_GRID} liegen."
        except ValueError:
            return "EXTRA_GRID_THRESHOLD muss eine ganze Zahl sein."
        return None
    if param == "REGISTRATION_END_TIME":
        if not re.match(r"^\d{2}:\d{2}$", val):
            return "REGISTRATION_END_TIME muss im Format hh:mm sein."
        return None
    if param == "POLL_INTERVAL_SECONDS":
        try:
            v = int(val)
            if not (10 <= v <= 120):
                return "POLL_INTERVAL_SECONDS muss zwischen 10 und 120 liegen."
        except ValueError:
            return "POLL_INTERVAL_SECONDS muss eine ganze Zahl sein."
        return None
    if param == "SET_MIN_GRIDS_MSG":
        try:
            v = int(val)
            if not (1 <= v <= MAX_GRIDS):
                return f"SET_MIN_GRIDS_MSG muss zwischen 1 und {MAX_GRIDS} liegen."
        except ValueError:
            return "SET_MIN_GRIDS_MSG muss eine ganze Zahl sein."
        return None
    return None  # Unknown params are silently ignored


# ─────────────────────────────────────────────
# Command handler  (Block 11)
# ─────────────────────────────────────────────

KNOWN_COMMANDS = (
    "!help",
    "!clean",
    "!set",
    "!grids=",
)


def _is_command(content: str) -> bool:
    """Return True if the message content matches a known command."""
    lower = content.lower().strip()
    return any(lower == cmd or lower.startswith(cmd) for cmd in KNOWN_COMMANDS)


async def handle_commands(session: aiohttp.ClientSession, bot_user_id: str) -> None:
    """Scan CHAN_ORDERS for commands. Only delete messages that match a known command."""
    messages = await get_channel_messages(session, CHAN_ORDERS, DISCORD_TOKEN_APOLLOGRABBER, limit=20)

    for msg in messages:
        author_id = msg.get("author", {}).get("id", "")
        content   = msg.get("content", "").strip()
        msg_id    = msg.get("id", "")
        username  = await get_display_name(session, author_id, msg.get("author", {}).get("username", "Unknown"))

        # Only process messages that match a known command pattern.
        # This protects log posts and any other non-command content in the channel.
        if not _is_command(content):
            continue

        # Delete the command message
        await discord_delete(
            session,
            f"/channels/{CHAN_ORDERS}/messages/{msg_id}",
            DISCORD_TOKEN_APOLLOGRABBER,
        )

        if author_id not in USER_ID_ORGA:
            continue  # Ignore unauthorised users silently

        content_lower = content.lower()
        ts = ts_str()
        grids = int(state.get("last_grid_count", 0))

        # ── !help ─────────────────────────────────────────────────────────
        if content_lower == "!help":
            await discord_post(
                session, f"/channels/{CHAN_ORDERS}/messages",
                DISCORD_TOKEN_APOLLOGRABBER, {"content": MSG_HILFETEXT},
            )
            continue

        # ── !clean (ohne Parameter) ───────────────────────────────────────
        if content_lower == "!clean":
            await discord_post(
                session, f"/channels/{CHAN_ORDERS}/messages",
                DISCORD_TOKEN_APOLLOGRABBER,
                {"content": (
                    "**!clean** – Verfügbare Optionen:\n"
                    "`!clean codes` – Lobby-Code-Kanal leeren und Platzhalter posten\n"
                    "`!clean log` – Log neu aufbauen und aktualisieren\n"
                    "`!clean news` – Alle Bot-Nachrichten im News-Kanal löschen"
                )},
            )
            continue

        # ── !clean codes ──────────────────────────────────────────────────
        if content_lower == "!clean codes":
            await clean_lobby_codes(session)
            await sync_to_sheets(session, "cleancodes")
            log_line = f"{ts} ⚙️ Lobby-Bereinigung durch {username}"
            append_event_log(log_line)
            _rebuild_discord_log(grids)
            await _refresh_chan_log(session)
            continue

        # ── !clean log ────────────────────────────────────────────────────
        if content_lower == "!clean log":
            clean_content = build_clean_log(grids, username)
            EVENT_LOG_FILE.write_text(clean_content, encoding="utf-8")
            _rebuild_discord_log(grids)
            await _refresh_chan_log(session)
            continue

        # ── !clean news ───────────────────────────────────────────────────
        if content_lower == "!clean news":
            await delete_all_bot_messages(
                session, CHAN_NEWS, DISCORD_TOKEN_APOLLOGRABBER, bot_user_id,
            )
            log_line = f"{ts} ⚙️ News-Bereinigung durch {username}"
            append_event_log(log_line)
            _rebuild_discord_log(grids)
            await _refresh_chan_log(session)
            continue

        # ── !set ──────────────────────────────────────────────────────────
        if content_lower.startswith("!set"):
            parts = content.split(maxsplit=2)
            if len(parts) == 1:
                # !set ohne Parameter: alle Einstellungen als Embed auflisten
                lines = []
                for vk in VAR_KEYS:
                    val = state.get(vk)
                    if val is None:
                        val = DEFAULT_STATE.get(vk, '–')
                    lines.append(f"`{vk[4:]}` = `{val}`")
                await discord_post(
                    session, f"/channels/{CHAN_ORDERS}/messages",
                    DISCORD_TOKEN_APOLLOGRABBER,
                    {"content": "**Aktuelle Einstellungen:**", "embeds": [{"description": "\n".join(lines)}]},
                )
            elif len(parts) == 2:
                # !set PARAM ohne Wert
                await discord_post(
                    session, f"/channels/{CHAN_ORDERS}/messages",
                    DISCORD_TOKEN_APOLLOGRABBER,
                    {"content": f"❌ Kein Wert angegeben. Verwendung: `!set {parts[1].upper()} <Wert>`"},
                )
            else:
                param   = parts[1].upper()
                val_raw = parts[2].strip()
                var_key = f"var_{param}"
                if var_key not in VAR_KEYS:
                    await discord_post(
                        session, f"/channels/{CHAN_ORDERS}/messages",
                        DISCORD_TOKEN_APOLLOGRABBER,
                        {"content": f"❌ Unbekannter Parameter: `{param}`"},
                    )
                    continue
                err = _validate_var(param, val_raw)
                if err:
                    await discord_post(
                        session, f"/channels/{CHAN_ORDERS}/messages",
                        DISCORD_TOKEN_APOLLOGRABBER,
                        {"content": f"❌ Ungültiger Wert: {err}"},
                    )
                else:
                    state[var_key] = _coerce_var(var_key, val_raw)
                    save_state()
                    log_line = f"{ts} ⚠️ {param} geändert durch {username}: {val_raw}"
                    append_event_log(log_line)
                    _rebuild_discord_log(grids)
                    await _refresh_chan_log(session)
                    await discord_post(
                        session, f"/channels/{CHAN_ORDERS}/messages",
                        DISCORD_TOKEN_APOLLOGRABBER,
                        {"content": f"✅ `{param}` gesetzt auf `{val_raw}`"},
                    )
            continue

        # ── !grids=x ──────────────────────────────────────────────────────
        m = re.match(r"!grids=(\d+)", content_lower)
        if m:
            x = int(m.group(1))
            if x == 0:
                state["man_lock"]         = False
                state["manual_grids"]     = None
                state["grid_lock_override"] = True
                new_g = recalculate_grids(len(state.get("drivers", [])))
                state["last_grid_count"]  = new_g
            else:
                new_g = min(x, MAX_GRIDS)
                state["man_lock"]         = True
                state["manual_grids"]     = new_g
                state["last_grid_count"]  = new_g

            # Recalculate driver status and detect waitlist/moveup changes
            current_drivers = list(state.get("drivers", []))
            old_status      = dict(state.get("driver_status", {}))
            new_status      = classify_drivers(current_drivers, new_g)
            state["driver_status"] = new_status

            newly_waitlisted = [
                n for n, s in new_status.items()
                if s == "waitlist" and old_status.get(n) == "grid"
            ]
            newly_moved_up = [
                n for n, s in new_status.items()
                if s == "grid" and old_status.get(n) == "waitlist"
            ]

            # 1. Command entry FIRST
            if x == 0:
                append_event_log(f"{ts} 🔓 Grid-Automatik reaktiviert durch {username}")
            else:
                append_event_log(f"{ts} 🔒 Grids auf {new_g} gesetzt durch {username}")

            # 2. Status changes AFTER the command entry
            for n in newly_moved_up:
                append_event_log(f"{ts} 🟡 -> 🟢 {n}")
            for n in newly_waitlisted:
                append_event_log(f"{ts} 🟢 -> 🟡 {n}")

            save_state()
            _rebuild_discord_log(new_g)
            await _refresh_chan_log(session)

            if newly_waitlisted:
                await send_waitlist_msg(session, newly_waitlisted)
            if newly_moved_up:
                await send_moved_up_msg(session, newly_moved_up)
            continue


# ─────────────────────────────────────────────
# Bootstrap  (Block 1 – first-run initialisation)
# FIX #11: bootstrap() is called BEFORE the worker loop, inside the
#          aiohttp session, with state already loaded in __main__.
# ─────────────────────────────────────────────

async def bootstrap(session: aiohttp.ClientSession) -> None:
    """
    Run once when no state.json existed at startup (Erstinstallation / frische VM).
    Does NOT touch CHAN_LOG or CHAN_CODES — those channels are only cleaned
    when a genuinely new event is detected (had_previous_event=True).
    """
    log.info("Bootstrap: Erstinitialisierung …")

    # Clear / create local data files only
    for fp in (EVENT_LOG_FILE, DISCORD_LOG_FILE, ANMELDUNGEN_FILE):
        fp.write_text("", encoding="utf-8")

    # Reset event sentinel so first pipeline run detects the current event
    state["event_id"] = "0"
    state["log_id"]   = ""
    state["registration_end_monday"] = ""

    # If we're deploying during sunday-lock time, pre-set the lock and mark
    # sunday_msg_sent=True so the notification is not sent again.
    # Logic: sunday lock applies Sun >= 18:00 or all day Monday.
    # A new event starts on Tuesday, so if it's currently lock time we know
    # the sunday message was already sent (or is irrelevant for this event).
    if is_sunday_lock_time():
        state["sunday_lock"]     = True
        state["sunday_msg_sent"] = True
        log.info("Bootstrap: Sonntags-Sperre vorgesetzt (kein erneuter Text).")

    save_state()
    append_event_log(f"{ts_str()} ⚙️ Systemupdate")
    log.info("Bootstrap abgeschlossen.")


# ─────────────────────────────────────────────
# Core pipeline  (Block 2 – Operative Ablauf-Reihenfolge)
# ─────────────────────────────────────────────

async def run_pipeline(session: aiohttp.ClientSession, bot_user_id: str) -> None:
    """Execute one complete pipeline iteration."""
    trigger_sheets   = False
    roster_changed = False
    sheets_type      = "update"

    # ── 0. Sunday-lock check – BEFORE any grid calculation ────────────────
    # Must run first so that the lock is active when recalculate_grids() is
    # called later in this same iteration.
    if is_sunday_lock_time() and not state.get("sunday_lock"):
        state["sunday_lock"] = True
        log.info("Sonntags-Sperre aktiviert.")
        save_state()

    # ── 1. Find Apollo event message ───────────────────────────────────────
    apollo_msg = await find_apollo_message(session, DISCORD_TOKEN_APOLLOGRABBER)

    if not apollo_msg:
        log.info("Kein Apollo-Event gefunden – nur Commands werden verarbeitet.")
        await handle_commands(session, bot_user_id)
        return

    new_id   = apollo_msg["id"]
    event_id = state.get("event_id", "0")

    # ── 2. New event detection ─────────────────────────────────────────────
    if new_id != event_id:
        log.info(f"Neues Event erkannt: {new_id} (alt: {event_id})")

        # Remember whether a real previous event existed (not a fresh deploy sentinel)
        had_previous_event = event_id != "0"

        # Delete old Apollo event post and its reminder thread if enabled
        if int(cfg("ENABLE_DELETE_OLD_EVENT")) and event_id and event_id != "0" and is_event_deletion_window():
            # Delete the event message itself
            await discord_delete(
                session,
                f"/channels/{CHAN_APOLLO}/messages/{event_id}",
                DISCORD_TOKEN_APOLLOGRABBER,
            )
            # Delete any remaining Apollo messages in CHAN_APOLLO that are not
            # the new event – this covers reminder thread-starter posts which
            # Apollo creates as separate messages with a 'thread' object.
            # After deleting the event embed, any leftover Apollo post in the
            # channel is either a thread or a stale post – both should go.
            leftover_msgs = await get_channel_messages(
                session, CHAN_APOLLO, DISCORD_TOKEN_APOLLOGRABBER
            )
            for lm in leftover_msgs:
                if (lm.get("author", {}).get("id") == DISCORD_ID_APOLLO
                        and lm["id"] != new_id):
                    await discord_delete(
                        session,
                        f"/channels/{CHAN_APOLLO}/messages/{lm['id']}",
                        DISCORD_TOKEN_APOLLOGRABBER,
                    )

        # Reset event-level state
        state["event_id"]   = new_id
        state["new_event"]  = 1
        state["sunday_lock"]  = False
        state["man_lock"]     = False
        state["manual_grids"] = None
        state["registration_end_logged"] = False
        state["ignored_drivers"]   = []
        state["driver_status"]     = {}
        state["drivers"]           = []
        # sunday_msg_sent: only reset for real new events, not on fresh deploy.
        # On deploy, bootstrap may have pre-set it to True (sunday lock time).
        if had_previous_event:
            state["sunday_msg_sent"] = False
        elif not is_sunday_lock_time():
            state["sunday_msg_sent"] = False
        state["last_grid_count"]   = 0
        state["registration_end_monday"] = ""   # will be set when embed is first parsed

        trigger_sheets = True
        sheets_type    = "cleancodes" if had_previous_event else "update"

        # Clear all log files
        for fp in (EVENT_LOG_FILE, DISCORD_LOG_FILE, ANMELDUNGEN_FILE):
            fp.write_text("", encoding="utf-8")

        # CHAN_CODES and CHAN_LOG cleanup only when a real previous event existed.
        # On a fresh deploy (had_previous_event=False) these channels are left
        # untouched — the existing log post stays, CHAN_CODES is not wiped.
        if had_previous_event:
            await clean_lobby_codes(session)
            await clear_chan_log(session, bot_user_id)
            # Brief pause so Discord propagates the deletions before we post again.
            await asyncio.sleep(2)
            if int(cfg("ENABLE_NEWS_CLEANUP")):
                await delete_all_bot_messages(
                    session, CHAN_NEWS, DISCORD_TOKEN_APOLLOGRABBER, bot_user_id,
                )

        append_event_log(f"{ts_str()} ⚙️ New Event")
        save_state()

        # New-event notification – only when a real previous event existed,
        # not on a fresh deploy/restart where event_id was the "0" sentinel.
        if had_previous_event:
            await send_new_event_msg(session)

    # ── 3. Parse embed, detect changes ────────────────────────────────────
    embed_data  = parse_apollo_embed(apollo_msg)
    new_title   = embed_data["title"]
    new_ev_dt   = embed_data["event_datetime"]
    new_drivers = embed_data["drivers"]

    old_drivers = list(state.get("drivers", []))
    old_title   = state.get("event_title", "")

    # FIX #7: title change also triggers roster_changed / trigger_make
    content_changed = (
        new_drivers != old_drivers
        or new_title != old_title
        or state.get("new_event") == 1
    )

    if content_changed:
        trigger_sheets   = True
        roster_changed = True

    state["event_title"]    = new_title
    state["event_datetime"] = new_ev_dt

    # FIX #8: set registration deadline Monday on first parse of this event
    set_registration_end_monday()

    # ── 4. Driver delta & grid calculation ────────────────────────────────
    if roster_changed:
        reg_end   = registration_end_passed()
        old_grids = int(state.get("last_grid_count", 0))

        # Write deadline log entry exactly once per event cycle
        if reg_end and not state.get("registration_end_logged"):
            append_event_log(f"{ts_str()} ⚙️ Anmeldeschluss – keine weiteren Anmeldungen möglich")
            state["registration_end_logged"] = True
            save_state()

        # Grid count: recalculate_grids() respects locks and override internally
        new_grids = recalculate_grids(len(new_drivers))

        # Check for extra-grid condition (Block 8.3)
        extra_grid_triggered = False
        if check_extra_grid(len(new_drivers), new_grids):
            new_grids = recalculate_grids(len(new_drivers))
            extra_grid_triggered = True

        state["last_grid_count"] = new_grids

        # Delta detection and event_log.txt updates
        changes = process_driver_changes(new_drivers, old_drivers, new_grids, reg_end)

        # Accepted drivers = embed list minus any ignored (post-deadline sign-ups).
        # Ignored drivers are visible in Apollo but not part of our state.
        # Persist ignored list so next cycle won't re-log them with 🔴🔴.
        new_ignored = changes.get("ignored", [])
        existing_ignored = state.get("ignored_drivers", [])
        all_ignored = list(dict.fromkeys(existing_ignored + new_ignored))  # deduplicated, order preserved
        state["ignored_drivers"] = all_ignored
        ignored_set = set(all_ignored)
        accepted_drivers = [d for d in new_drivers if d not in ignored_set]

        # Update state with accepted drivers only
        state["drivers"]       = accepted_drivers
        state["driver_status"] = classify_drivers(accepted_drivers, new_grids)

        # FIX #1: anmeldungen.txt reflects accepted drivers only
        write_anmeldungen(accepted_drivers)

        # Rebuild discord_log.txt
        _rebuild_discord_log(new_grids)

        # ── News notifications ────────────────────────────────────────────
        if changes["waitlisted"]:
            await send_waitlist_msg(session, changes["waitlisted"])

        if changes["moved_up"]:
            await send_moved_up_msg(session, changes["moved_up"])

        # Grid-full message: only when grids increase organically (no lock active)
        if (new_grids > old_grids
                and old_grids > 0
                and not (state.get("sunday_lock") or state.get("man_lock"))):
            await send_grid_full_msg(session, new_grids)

        if extra_grid_triggered:
            await send_extra_grid_msg(session)

        if sheets_type != "event_reset":
            sheets_type = "update"

    # ── 5. Make.com sync ──────────────────────────────────────────────────
    if trigger_sheets:
        await sync_to_sheets(session, sheets_type)
        save_state()

    # ── 6. Discord CHAN_LOG update ────────────────────────────────────────
    await _refresh_chan_log(session)

    # ── 7. Command scan ───────────────────────────────────────────────────
    await handle_commands(session, bot_user_id)

    # ── 8. Sunday-lock message ───────────────────────────────────────────
    # Lock state itself is set at the top of run_pipeline (step 0).
    if is_sunday_lock_time():
        await send_sunday_msg(session)

    # ── 9. End-of-cycle variable reset ───────────────────────────────────
    state["new_event"] = 0
    save_state()


# ─────────────────────────────────────────────
# Worker loop  (Single-Execution Polling Engine)
# ─────────────────────────────────────────────

async def worker(fresh_install: bool) -> None:
    """
    Async entry point. Runs the full pipeline every POLL_INTERVAL_SECONDS.
    Between pipeline runs, CHAN_ORDERS is scanned every CMD_SCAN_INTERVAL_SECONDS
    so commands are picked up quickly without triggering a full pipeline cycle.
    """
    log.info("Worker gestartet.")
    ensure_files()

    async with aiohttp.ClientSession() as session:
        bot_user_id = await get_bot_user_id(session, DISCORD_TOKEN_APOLLOGRABBER)

        if fresh_install:
            await bootstrap(session)

        while True:
            # ── Full pipeline run ─────────────────────────────────────────
            async with _pipeline_lock:
                try:
                    await run_pipeline(session, bot_user_id)
                except Exception as e:
                    log.error(f"Pipeline-Fehler: {e}", exc_info=True)
                    save_state()

            # ── Inter-pipeline command scanning ───────────────────────────
            # Sleep in CMD_SCAN_INTERVAL_SECONDS steps until POLL_INTERVAL_SECONDS
            # has elapsed. Each step scans CHAN_ORDERS so commands are executed
            # within CMD_SCAN_INTERVAL_SECONDS, not POLL_INTERVAL_SECONDS.
            poll_interval = int(state.get("var_POLL_INTERVAL_SECONDS", 60))
            cmd_interval  = max(1, CMD_SCAN_INTERVAL_SECONDS)
            elapsed       = 0

            while elapsed < poll_interval:
                sleep_for = min(cmd_interval, poll_interval - elapsed)
                await asyncio.sleep(sleep_for)
                elapsed += sleep_for

                if elapsed < poll_interval:
                    # Mid-interval command scan – lightweight, no pipeline mutex needed
                    try:
                        await handle_commands(session, bot_user_id)
                    except Exception as e:
                        log.warning(f"Command-Scan Fehler: {e}")


# ─────────────────────────────────────────────
# Main entry point  (single __main__ block)
# ─────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Determine fresh-install BEFORE load_state() creates the file
    fresh_install = not STATE_FILE.exists()

    # 2. Load / initialise state (only here – never in worker())
    load_state()
    ensure_files()

    # 3. Start Flask health server in a daemon thread
    launch_flask_thread()

    # 4. Hand off to the async worker in the main event loop
    try:
        asyncio.run(worker(fresh_install))
    except KeyboardInterrupt:
        log.info("Shutdown durch KeyboardInterrupt.")
        save_state()
