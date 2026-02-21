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

CHAN_APOLLO = _env("CHAN_APOLLO", "")
CHAN_LOG    = _env("CHAN_LOG", "")
CHAN_NEWS   = _env("CHAN_NEWS", "")
CHAN_CODES  = _env("CHAN_CODES", "")
# FIX #5: resolved after CHAN_LOG is set, never evaluated before it
CHAN_ORDERS = _env("CHAN_ORDERS") or _env("CHAN_LOG", "")

DRIVERS_PER_GRID       = _env_int("DRIVERS_PER_GRID", 15)
MAX_GRIDS              = _env_int("MAX_GRIDS", 4)
MAKE_WEBHOOK_URL       = _env("MAKE_WEBHOOK_URL", "")
CMD_SCAN_INTERVAL_SECONDS = _env_int("CMD_SCAN_INTERVAL_SECONDS", 10)

# Message templates – loaded once from env at import time, never from state
MSG_HILFETEXT          = _env("MSG_HILFETEXT", "Kein Hilfetext konfiguriert.")
MSG_LOBBYCODES         = _env("MSG_LOBBYCODES", "Lobby-Codes")
MSG_EXTRA_GRID_TEXT    = _env("MSG_EXTRA_GRID_TEXT", "")
MSG_EXTRA_GRID_TEXT_EN = _env("MSG_EXTRA_GRID_TEXT_EN", "")
MSG_GRID_FULL_TEXT     = _env("MSG_GRID_FULL_TEXT", "")
MSG_GRID_FULL_TEXT_EN  = _env("MSG_GRID_FULL_TEXT_EN", "")
MSG_MOVED_UP_SINGLE    = _env("MSG_MOVED_UP_SINGLE", "")
MSG_MOVED_UP_SINGLE_EN = _env("MSG_MOVED_UP_SINGLE_EN", "")
MSG_MOVED_UP_MULTI     = _env("MSG_MOVED_UP_MULTI", "")
MSG_MOVED_UP_MULTI_EN  = _env("MSG_MOVED_UP_MULTI_EN", "")
MSG_SUNDAY_TEXT        = _env("MSG_SUNDAY_TEXT", "")
MSG_SUNDAY_TEXT_EN     = _env("MSG_SUNDAY_TEXT_EN", "")
MSG_WAITLIST_SINGLE    = _env("MSG_WAITLIST_SINGLE", "")
MSG_WAITLIST_SINGLE_EN = _env("MSG_WAITLIST_SINGLE_EN", "")
MSG_WAITLIST_MULTI     = _env("MSG_WAITLIST_MULTI", "")
MSG_WAITLIST_MULTI_EN  = _env("MSG_WAITLIST_MULTI_EN", "")
MSG_NEW_EVENT          = _env("MSG_NEW_EVENT", "")
MSG_NEW_EVENT_EN       = _env("MSG_NEW_EVENT_EN", "")

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
    "last_sync_make": "",
    # Triggers
    "sunday_msg_sent": False,
    "registration_end_logged": False,
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


def is_sunday_lock_time() -> bool:
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
    from apollo_grabber_b import build_html_dashboard
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
