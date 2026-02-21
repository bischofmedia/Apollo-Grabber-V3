"""
RTC Apollo-Grabber V2
Block C: News messages, Command handling, Pipeline, Main entry point
"""

from apollo_grabber_a import (
    BERLIN, STATE_FILE, EVENT_LOG_FILE, DISCORD_LOG_FILE, ANMELDUNGEN_FILE,
    DISCORD_TOKEN_APOLLOGRABBER, DISCORD_TOKEN_LOBBYCODEGRABBER,
    DISCORD_ID_APOLLO, USER_ID_ORGA,
    CHAN_APOLLO, CHAN_LOG, CHAN_NEWS, CHAN_CODES, CHAN_ORDERS,
    DRIVERS_PER_GRID, MAX_GRIDS, MAKE_WEBHOOK_URL, CMD_SCAN_INTERVAL_SECONDS,
    MSG_HILFETEXT,
    MSG_EXTRA_GRID_TEXT, MSG_EXTRA_GRID_TEXT_EN,
    MSG_GRID_FULL_TEXT, MSG_GRID_FULL_TEXT_EN,
    MSG_MOVED_UP_SINGLE, MSG_MOVED_UP_SINGLE_EN,
    MSG_MOVED_UP_MULTI, MSG_MOVED_UP_MULTI_EN,
    MSG_NEW_EVENT, MSG_NEW_EVENT_EN,
    MSG_SUNDAY_TEXT, MSG_SUNDAY_TEXT_EN,
    MSG_WAITLIST_SINGLE, MSG_WAITLIST_SINGLE_EN,
    MSG_WAITLIST_MULTI, MSG_WAITLIST_MULTI_EN,
    VAR_KEYS, VAR_ENV_MAP, DEFAULT_STATE,
    append_event_log, clear_file, ensure_files, iso_now, is_sunday_lock_time,
    launch_flask_thread, load_state, log, now_berlin, read_event_log,
    registration_end_passed, save_state, set_registration_end_monday, state,
    ts_str, write_discord_log, write_anmeldungen, cfg, _coerce_var,
)
from apollo_grabber_b import (
    build_clean_log, build_discord_log, build_html_dashboard, build_log_payload,
    calculate_grids, check_extra_grid, clean_lobby_codes, clear_chan_log,
    classify_drivers, delete_all_bot_messages, delete_all_messages,
    discord_delete, discord_get, discord_patch, discord_post,
    find_apollo_message, get_bot_user_id, get_channel_messages, grid_capacity,
    message_exists, parse_apollo_embed, post_or_update_log,
    process_driver_changes, recalculate_grids, send_to_make,
)

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
        parts.append(f"🇩🇪 {de_text}")
    if en_text:
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
        username  = msg.get("author", {}).get("username", "Unknown")

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
            log_line = f"{ts} ⚙️ Lobby-Bereinigung durch {username}"
            append_event_log(log_line)
            _rebuild_discord_log(grids)
            await _refresh_chan_log(session)
            continue

        # ── !clean log ────────────────────────────────────────────────────
        if content_lower == "!clean log":
            # Rebuild event_log.txt to compacted form, then refresh Discord
            clean_content = build_clean_log(grids)
            # Replace event_log.txt with the clean version
            from apollo_grabber_a import EVENT_LOG_FILE
            EVENT_LOG_FILE.write_text(clean_content, encoding="utf-8")
            _rebuild_discord_log(grids)
            await _refresh_chan_log(session)
            continue

        # ── !clean news ───────────────────────────────────────────────────
        if content_lower == "!clean news":
            if int(cfg("ENABLE_NEWS_CLEANUP")):
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
    Run once when no state.json existed at startup (fresh Render deploy).
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

    save_state()
    append_event_log(f"{ts_str()} ⚙️ Systemneustart")
    log.info("Bootstrap abgeschlossen.")


# ─────────────────────────────────────────────
# Core pipeline  (Block 2 – Operative Ablauf-Reihenfolge)
# ─────────────────────────────────────────────

async def run_pipeline(session: aiohttp.ClientSession, bot_user_id: str) -> None:
    """Execute one complete pipeline iteration."""
    trigger_make   = False
    roster_changed = False
    make_type      = "update"

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
        if int(cfg("ENABLE_DELETE_OLD_EVENT")) and event_id and event_id != "0":
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
        state["sunday_msg_sent"]        = False
        state["registration_end_logged"] = False
        state["driver_status"]     = {}
        state["drivers"]           = []
        state["last_grid_count"]   = 0
        state["registration_end_monday"] = ""   # will be set when embed is first parsed

        trigger_make = True
        make_type    = "event_reset"

        # Clear all log files
        for fp in (EVENT_LOG_FILE, DISCORD_LOG_FILE, ANMELDUNGEN_FILE):
            fp.write_text("", encoding="utf-8")

        # CHAN_CODES and CHAN_LOG cleanup only when a real previous event existed.
        # On a fresh deploy (had_previous_event=False) these channels are left
        # untouched — the existing log post stays, CHAN_CODES is not wiped.
        if had_previous_event:
            await clean_lobby_codes(session)
            await clear_chan_log(session, bot_user_id)

        append_event_log(f"{ts_str()} ⚙️ Neues Event")
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
        trigger_make   = True
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
        ignored_set = set(changes.get("ignored", []))
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

        if make_type != "event_reset":
            make_type = "update"

    # ── 5. Make.com sync ──────────────────────────────────────────────────
    if trigger_make:
        await send_to_make(session, make_type)
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
