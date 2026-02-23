"""
RTC Apollo-Grabber V2
Block B: Discord helpers, Apollo embed parsing, Grid logic, Make.com webhook
"""

from apollo_grabber_a import (
    BERLIN, DISCORD_API, DISCORD_ID_APOLLO, DISCORD_TOKEN_APOLLOGRABBER,
    DISCORD_TOKEN_LOBBYCODEGRABBER, DRIVERS_PER_GRID, MAX_GRIDS,
    MAKE_WEBHOOK_URL, MSG_LOBBYCODES,
    CHAN_APOLLO, CHAN_CODES, CHAN_LOG, CHAN_NEWS,
    EVENT_LOG_FILE, DISCORD_LOG_FILE, ANMELDUNGEN_FILE,
    append_event_log, clear_file, iso_now, log, now_berlin, read_event_log,
    read_discord_log, write_discord_log, write_anmeldungen,
    registration_end_passed, save_state, state, ts_str, cfg,
)

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
                ignored.append(name)
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
    last_sync    = _berlin_ts(state.get("last_sync_make", ""))

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
# Make.com Webhook  (Block 3)
# ─────────────────────────────────────────────

async def send_to_make(session: aiohttp.ClientSession, event_type: str) -> None:
    """
    POST the full payload to MAKE_WEBHOOK_URL.
    drivers list = raw names from state (>>> stripped, backslashes preserved).
    """
    if not MAKE_WEBHOOK_URL:
        log.warning("MAKE_WEBHOOK_URL nicht gesetzt – übersprungen.")
        return
    grids = int(state.get("last_grid_count", 0))
    sunday_lock = state.get("sunday_lock", False)
    man_lock = state.get("man_lock", False)
    grid_status = "locked" if (sunday_lock or man_lock) else "open"

    payload = {
        "type": event_type,
        "driver_count": len(state.get("drivers", [])),
        "drivers": list(state.get("drivers", [])),
        "grids": grids,
        "grid_status": grid_status,
        "history": read_event_log(),
        "timestamp": iso_now(),
    }
    try:
        async with session.post(MAKE_WEBHOOK_URL, json=payload) as r:
            if r.status in (200, 201, 204):
                state["last_sync_make"] = iso_now()
                log.info(f"Make.com Webhook ({event_type}) gesendet.")
            else:
                text = await r.text()
                log.warning(f"Make.com Webhook -> {r.status}: {text[:200]}")
    except Exception as e:
        log.error(f"Make.com Webhook Fehler: {e}")


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
    await send_to_make(session, "cleancodes")
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
