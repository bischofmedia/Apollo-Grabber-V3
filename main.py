import os
import json
import asyncio
import aiohttp
import signal
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from threading import Thread
from flask import Flask, Response

# =========================
# Konstante Zeitzone
# =========================
TZ = ZoneInfo("Europe/Berlin")

# =========================
# Environment Loader
# =========================
def load_env():
    return {
        "DISCORD_TOKEN_APOLLOGRABBER": os.getenv("DISCORD_TOKEN_APOLLOGRABBER"),
        "DISCORD_TOKEN_LOBBYCODEGRABBER": os.getenv("DISCORD_TOKEN_LOBBYCODEGRABBER"),
        "DISCORD_ID_APOLLO": os.getenv("DISCORD_ID_APOLLO"),
        "USER_ID_ORGA": os.getenv("USER_ID_ORGA", ""),
        "CHAN_APOLLO": os.getenv("CHAN_APOLLO"),
        "CHAN_LOG": os.getenv("CHAN_LOG"),
        "CHAN_NEWS": os.getenv("CHAN_NEWS"),
        "CHAN_CODES": os.getenv("CHAN_CODES"),
        "CHAN_ORDERS": os.getenv("CHAN_ORDERS"),
        "DRIVERS_PER_GRID": int(os.getenv("DRIVERS_PER_GRID", 15)),
        "MAX_GRIDS": int(os.getenv("MAX_GRIDS", 4)),
        "MAKE_WEBHOOK_URL": os.getenv("MAKE_WEBHOOK_URL"),
    }

ENV = load_env()

# =========================
# State Handling
# =========================
STATE_FILE = "state.json"
EVENT_LOG = "event_log.txt"
DISCORD_LOG = "discord_log.txt"
ANMELDUNGEN_FILE = "anmeldungen.txt"

DEFAULT_STATE = {
    "event_id": "0",
    "event_title": "",
    "event_datetime": "",
    "drivers": [],
    "driver_status": {},
    "manual_grids": None,
    "man_lock": 0,
    "sunday_lock": 0,
    "grid_lock_override": 0,
    "last_grid_count": 0,
    "log_id": None,
    "last_sync_make": "",
    "sunday_msg_sent": 0,
    "var_ENABLE_EXTRA_GRID": int(os.getenv("ENABLE_EXTRA_GRID", 0)),
    "var_ENABLE_MOVED_UP_MSG": int(os.getenv("ENABLE_MOVED_UP_MSG", 0)),
    "var_ENABLE_NEWS_CLEANUP": int(os.getenv("ENABLE_NEWS_CLEANUP", 0)),
    "var_ENABLE_SUNDAY_MSG": int(os.getenv("ENABLE_SUNDAY_MSG", 0)),
    "var_ENABLE_WAITLIST_MSG": int(os.getenv("ENABLE_WAITLIST_MSG", 0)),
    "var_ENABLE_DELETE_OLD_EVENT": int(os.getenv("ENABLE_DELETE_OLD_EVENT", 0)),
    "var_ENABLE_EXTRA_GRID_MSG": int(os.getenv("ENABLE_EXTRA_GRID_MSG", 0)),
    "var_ENABLE_GRID_FULL_MSG": int(os.getenv("ENABLE_GRID_FULL_MSG", 0)),
    "var_EXTRA_GRID_THRESHOLD": int(os.getenv("EXTRA_GRID_THRESHOLD", 1)),
    "var_POLL_INTERVAL_SECONDS": int(os.getenv("POLL_INTERVAL_SECONDS", 60)),
    "var_REGISTRATION_END_TIME": os.getenv("REGISTRATION_END_TIME", "18:00"),
    "var_SET_MIN_GRIDS_MSG": int(os.getenv("SET_MIN_GRIDS_MSG", 1)),
}

def load_state():
    if not os.path.exists(STATE_FILE):
        with open(STATE_FILE, "w") as f:
            json.dump(DEFAULT_STATE, f, indent=2)
        return DEFAULT_STATE.copy()
    with open(STATE_FILE, "r") as f:
        return json.load(f)

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

state = load_state()

# =========================
# Flask Health Server
# =========================
app = Flask(__name__)

@app.route("/health")
def health():
    return Response("Apollo Grabber running", status=200)

def start_flask():
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# =========================
# Global Mutex
# =========================
pipeline_lock = asyncio.Lock()
service_active = True

def handle_shutdown(sig, frame):
    global service_active
    service_active = False

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

# =========================
# Discord API Layer
# =========================

DISCORD_API = "https://discord.com/api/v10"

def discord_headers(token):
    return {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json"
    }

async def fetch_channel_messages(session, channel_id, limit=50):
    url = f"{DISCORD_API}/channels/{channel_id}/messages?limit={limit}"
    async with session.get(url, headers=discord_headers(ENV["DISCORD_TOKEN_APOLLOGRABBER"])) as resp:
        if resp.status != 200:
            return []
        return await resp.json()

async def fetch_message(session, channel_id, message_id):
    url = f"{DISCORD_API}/channels/{channel_id}/messages/{message_id}"
    async with session.get(url, headers=discord_headers(ENV["DISCORD_TOKEN_APOLLOGRABBER"])) as resp:
        if resp.status != 200:
            return None
        return await resp.json()

async def delete_message(session, channel_id, message_id, token):
    url = f"{DISCORD_API}/channels/{channel_id}/messages/{message_id}"
    async with session.delete(url, headers=discord_headers(token)) as resp:
        return resp.status in (200, 204)

async def post_message(session, channel_id, content, token):
    url = f"{DISCORD_API}/channels/{channel_id}/messages"
    payload = {"content": content}
    async with session.post(url, headers=discord_headers(token), json=payload) as resp:
        if resp.status == 200 or resp.status == 201:
            return await resp.json()
        return None

async def patch_message(session, channel_id, message_id, content):
    url = f"{DISCORD_API}/channels/{channel_id}/messages/{message_id}"
    payload = {"content": content}
    async with session.patch(url, headers=discord_headers(ENV["DISCORD_TOKEN_APOLLOGRABBER"]), json=payload) as resp:
        return resp.status in (200, 204)

# =========================
# Apollo Embed Parsing
# =========================

def clean_driver_name(name: str):
    return name.replace(">>>", "").strip()

def extract_apollo_data(message_json):
    if not message_json:
        return None

    embeds = message_json.get("embeds", [])
    if not embeds:
        return None

    embed = embeds[0]
    title = embed.get("title", "")
    fields = embed.get("fields", [])

    drivers = []

    # Fields[0] wird ignoriert (Header)
    for field in fields[1:]:
        value = field.get("value", "")
        lines = value.split("\n")
        for line in lines:
            name = clean_driver_name(line)
            if name:
                drivers.append(name)

    # Event-Zeit aus Embed-Description oder Footer extrahieren (Fallback)
    event_datetime = embed.get("description", "")
    if not event_datetime:
        footer = embed.get("footer", {})
        event_datetime = footer.get("text", "")

    return {
        "title": title,
        "datetime": event_datetime,
        "drivers": drivers
    }

# =========================
# Event Detection
# =========================

async def get_current_apollo_event(session):
    messages = await fetch_channel_messages(session, ENV["CHAN_APOLLO"], limit=50)

    apollo_messages = [
        m for m in messages
        if m.get("author", {}).get("id") == ENV["DISCORD_ID_APOLLO"]
    ]

    if not apollo_messages:
        return None

    # NEUESTE Nachricht (höchster Timestamp)
    apollo_messages.sort(key=lambda x: x["timestamp"], reverse=True)
    return apollo_messages[0]

async def detect_event_change(session):
    global state

    message = await get_current_apollo_event(session)
    if not message:
        return None, False

    new_id = message["id"]
    is_new = False

    if state["event_id"] != new_id:
        is_new = True
        state["event_id"] = new_id
        state["sunday_lock"] = 0
        state["man_lock"] = 0
        state["grid_lock_override"] = 0
        state["sunday_msg_sent"] = 0

    data = extract_apollo_data(message)

    if data:
        state["event_title"] = data["title"]
        state["event_datetime"] = data["datetime"]

    return data, is_new

# =========================
# Grid Calculation
# =========================

def calculate_required_grids(driver_count: int):
    per_grid = ENV["DRIVERS_PER_GRID"]
    max_grids = ENV["MAX_GRIDS"]

    if driver_count == 0:
        return 0

    grids = (driver_count + per_grid - 1) // per_grid
    return min(grids, max_grids)


def build_grids(drivers):
    per_grid = ENV["DRIVERS_PER_GRID"]
    grids = []

    for i in range(0, len(drivers), per_grid):
        grids.append(drivers[i:i + per_grid])

    return grids


# =========================
# Registration Deadline Logic
# =========================

def parse_event_datetime():
    raw = state.get("event_datetime", "")
    if not raw:
        return None

    raw = raw.strip()

    # 1️⃣ ISO-Format
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)
        return dt.astimezone(TZ)
    except:
        pass

    # 2️⃣ Discord Timestamp <t:1737849600:F>
    if raw.startswith("<t:") and ":" in raw:
        try:
            timestamp = int(raw.split(":")[1])
            return datetime.fromtimestamp(timestamp, TZ)
        except:
            pass

    # 3️⃣ Deutsches Format z.B. "20.01.2026 20:00"
    try:
        dt = datetime.strptime(raw, "%d.%m.%Y %H:%M")
        return dt.replace(tzinfo=TZ)
    except:
        pass

    # Falls alles fehlschlägt
    log_event(f"Could not parse event datetime: {raw}")
    return None



def registration_closed():
    event_dt = parse_event_datetime()
    if not event_dt:
        return False

    reg_end_str = state["var_REGISTRATION_END_TIME"]  # z.B. "18:00"
    hour, minute = map(int, reg_end_str.split(":"))

    reg_deadline = event_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)

    now = datetime.now(TZ)
    return now >= reg_deadline


# =========================
# Delta Analysis
# =========================

def compute_driver_delta(new_drivers):
    old_set = set(state.get("drivers", []))
    new_set = set(new_drivers)

    added = list(new_set - old_set)
    removed = list(old_set - new_set)

    return added, removed


# =========================
# Event Log Management
# =========================

def log_event(text):
    timestamp = datetime.now(TZ).isoformat()
    with open(EVENT_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {text}\n")


def update_anmeldungen_file(drivers):
    with open(ANMELDUNGEN_FILE, "w", encoding="utf-8") as f:
        for d in drivers:
            f.write(d + "\n")


# =========================
# Core State Update (Driver Sync)
# =========================

def update_driver_state(new_drivers):
    added, removed = compute_driver_delta(new_drivers)

    if added:
        log_event(f"Drivers added: {added}")

    if removed:
        log_event(f"Drivers removed: {removed}")

    state["drivers"] = new_drivers
    state["driver_status"] = {d: "active" for d in new_drivers}

    update_anmeldungen_file(new_drivers)

    return added, removed

# =========================
# Message Builder (Multi-Text System)
# =========================

def build_multi_grid_message(grids):
    lines = []
    lines.append(f"**{state['event_title']}**")
    lines.append("")

    for idx, grid in enumerate(grids, start=1):
        lines.append(f"**Startgruppe {idx}**")
        for driver in grid:
            lines.append(f"- {driver}")
        lines.append("")

    total = sum(len(g) for g in grids)
    lines.append(f"Gesamtstarter: {total}")
    return "\n".join(lines)


def build_waitlist_message(waitlist):
    if not waitlist:
        return None

    lines = []
    lines.append("**Warteliste**")
    for driver in waitlist:
        lines.append(f"- {driver}")

    return "\n".join(lines)


def build_grid_full_message():
    return "⚠️ Alle Startplätze sind vergeben."


def build_sunday_message():
    return "📌 Anmeldung geschlossen. Grid finalisiert."


# =========================
# Grid + Waitlist Logic
# =========================

def process_grid_logic(drivers):
    required_grids = calculate_required_grids(len(drivers))
    grids = build_grids(drivers)

    max_slots = ENV["MAX_GRIDS"] * ENV["DRIVERS_PER_GRID"]

    waitlist = []
    if len(drivers) > max_slots:
        waitlist = drivers[max_slots:]
        grids = build_grids(drivers[:max_slots])

    return grids, waitlist, required_grids


# =========================
# Sunday Lock Handling
# =========================

def handle_sunday_lock():
    if state.get("sunday_lock") == 1:
        return True

    if registration_closed():
        state["sunday_lock"] = 1
        return True

    return False


# =========================
# Extra Grid Detection
# =========================

def check_extra_grid_activation(driver_count):
    per_grid = ENV["DRIVERS_PER_GRID"]

    if driver_count % per_grid == 1:
        return True

    return False


# =========================
# Delete Old Event Message
# =========================

async def delete_previous_event_message(session):
    if not state.get("published_message_id"):
        return

    await delete_message(
        session,
        ENV["CHAN_ORDERS"],
        state["published_message_id"],
        ENV["DISCORD_TOKEN_APOLLOGRABBER"]
    )

    state["published_message_id"] = None

# =========================
# Make Webhook
# =========================

async def send_make_webhook(payload):
    if not ENV["MAKE_WEBHOOK_URL"]:
        return

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(ENV["MAKE_WEBHOOK_URL"], json=payload) as resp:
                if resp.status in (200, 201):
                    state["last_sync_make"] = datetime.now(TZ).isoformat()
        except Exception as e:
            log_event(f"Make webhook error: {e}")


# =========================
# Discord Log Channel
# =========================

async def log_to_discord(session, text):
    if not ENV["CHAN_LOG"]:
        return
    await post_message(
        session,
        ENV["CHAN_LOG"],
        text[:2000],
        ENV["DISCORD_TOKEN_APOLLOGRABBER"]
    )


# =========================
# Core Pipeline
# =========================

async def run_pipeline_once():
    global state

    async with pipeline_lock:

        async with aiohttp.ClientSession() as session:

            # 1. Event Detection
            data, is_new_event = await detect_event_change(session)
            if not data:
                return

            new_drivers = data["drivers"]

            # 2. Registration Deadline Check
            closed = registration_closed()

            # 3. Delta
            added, removed = compute_driver_delta(new_drivers)

            if not is_new_event and not added and not removed:
                return  # keine Änderung

            # 4. Event Reset Handling
            if is_new_event:
                log_event("New event detected")
                state["drivers"] = []
                state["driver_status"] = {}
                state["published_message_id"] = None

            # 5. Update Driver State
            if not closed:
                added, removed = update_driver_state(new_drivers)
            else:
                # nur Abmeldungen berücksichtigen
                filtered = [d for d in state["drivers"] if d in new_drivers]
                state["drivers"] = filtered

            # 6. Grid Processing
            drivers = state["drivers"]
            grids, waitlist, required_grids = process_grid_logic(drivers)

            # 7. Sunday Lock
            sunday_locked = handle_sunday_lock()

            # 8. Message Building
            content_main = build_multi_grid_message(grids)
            content_wait = build_waitlist_message(waitlist)

            if sunday_locked and state["var_ENABLE_SUNDAY_MSG"] == 1:
                content_main += "\n\n" + build_sunday_message()

            if waitlist and state["var_ENABLE_WAITLIST_MSG"] == 1:
                content_main += "\n\n" + content_wait

            # 9. Publish or Update
            if not state.get("published_message_id"):
                msg = await post_message(
                    session,
                    ENV["CHAN_ORDERS"],
                    content_main[:2000],
                    ENV["DISCORD_TOKEN_APOLLOGRABBER"]
                )
                if msg:
                    state["published_message_id"] = msg["id"]
            else:
                await patch_message(
                    session,
                    ENV["CHAN_ORDERS"],
                    state["published_message_id"],
                    content_main[:2000]
                )

            # 10. Make Webhook Sync
            payload = {
                "type": "update",
                "event_id": state["event_id"],
                "event_title": state["event_title"],
                "event_datetime": state["event_datetime"],
                "driver_count": len(drivers),
                "grid_count": required_grids,
                "waitlist_count": len(waitlist),
                "timestamp": datetime.now(TZ).isoformat()
            }

            await send_make_webhook(payload)

            # 11. Persist State
            save_state(state)

# =========================
# Worker Loop (Single Execution Engine)
# =========================

service_active = True


async def worker_loop():
    global service_active

    interval = var_POLL_INTERVAL_SECONDS

    while service_active:
        start_time = datetime.now(TZ)

        try:
            await run_pipeline_once()
        except Exception as e:
            log_event(f"Pipeline error: {e}")
        finally:
            # Persist state defensiv nach jeder Iteration
            save_state(state)

        # Intervall startet NACH Pipeline-Ende
        await asyncio.sleep(interval)


# =========================
# Flask Health Server
# =========================

@app.route("/")
def dashboard():

    # Basisdaten
    event_title = state.get("event_title", "—")
    drivers = state.get("drivers", [])
    driver_count = len(drivers)

    grids = calculate_required_grids(driver_count)
    event_datetime = state.get("event_datetime", "—")

    # Log-Datei lesen
    log_content = ""
    if os.path.exists(EVENT_LOG):
        with open(EVENT_LOG, "r", encoding="utf-8") as f:
            log_content = f.read()

    html = f"""
    <html>
    <head>
        <title>Apollo Grabber V2</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background-color: #f4f4f4;
                margin: 30px;
            }}
            .header {{
                margin-bottom: 30px;
            }}
            .stats {{
                margin-top: 10px;
                padding: 10px;
                background: #ffffff;
                border-radius: 6px;
            }}
            .log-container {{
                margin-top: 20px;
                background-color: #000000;
                color: #00ff00;
                font-family: monospace;
                padding: 15px;
                border-radius: 6px;
                white-space: pre-wrap;
            }}
        </style>
    </head>
    <body>

        <div class="header">
            <h1>Apollo Grabber V2</h1>
            <h2>{event_title}</h2>

            <div class="stats">
                <strong>Fahrer:</strong> {driver_count} |
                <strong>Grids:</strong> {grids} |
                <strong>Datum:</strong> {event_datetime}
            </div>
        </div>

        <div class="log-container">
{log_content}
        </div>

    </body>
    </html>
    """

    return html



def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)


# =========================
# Bootstrap Sequence
# =========================

def initialize_state():
    global state

    loaded = load_state()
    if loaded:
        state = loaded
    else:
        state = {
            "event_id": None,
            "event_title": None,
            "event_datetime": None,
            "drivers": [],
            "driver_status": {},
            "published_message_id": None,
            "sunday_lock": 0,
            "var_ENABLE_SUNDAY_MSG": 1,
            "var_ENABLE_WAITLIST_MSG": 1,
            "last_sync_make": None
        }
        save_state(state)


def start_worker_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(worker_loop())


if __name__ == "__main__":

    # 1. Zustand laden
    initialize_state()

    # 2. Worker-Thread vorbereiten (aber noch nicht starten)
    worker_thread = threading.Thread(
        target=start_worker_thread,
        daemon=True
    )

    # 3. Flask starten
    flask_thread = threading.Thread(
        target=run_flask,
        daemon=True
    )
    flask_thread.start()

    # 4. Worker aktivieren
    worker_thread.start()

    # Hauptthread blockieren
    worker_thread.join()
