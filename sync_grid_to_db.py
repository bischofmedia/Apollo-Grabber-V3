#!/usr/bin/env python3
"""
sync_grid_to_db.py
==================
Liest die aktuelle Grideinteilung aus dem Google Sheet (Tab "Grids")
und schreibt sie als Snapshot in die MySQL-Tabelle `grid_assignments`.
Legt Streamer in der `streamers`-Tabelle an bzw. aktualisiert sie.

Aufruf direkt:
    python3 sync_grid_to_db.py

Aufruf vom Bot (non-blocking):
    import subprocess
    subprocess.Popen(
        ["python3", "/home/ubuntu/RTC_ApolloGrabber/sync_grid_to_db.py"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

Google Sheets API:
    Nur EIN get_all_values()-Call, um das Rate-Limit (60 Req/Min) zu schonen.

Benötigte .env-Variablen:
    GOOGLE_CREDENTIALS_FILE   Pfad zur service-account JSON
    SPREADSHEET_ID            ID des Google Spreadsheets
    GRID_SHEET_NAME           Name des Tabs (Standard: Grids)
    DB_HOST                   MySQL-Host (Standard: localhost)
    DB_PORT                   MySQL-Port  (Standard: 3306)
    DB_USER                   MySQL-Benutzer
    DB_PASSWORD               MySQL-Passwort
    DB_NAME                   Datenbankname

SQL zum Hinzufügen von streamer_id in grid_assignments (einmalig):
    ALTER TABLE grid_assignments
        ADD COLUMN streamer_id INT NULL AFTER driver_id,
        ADD FOREIGN KEY (streamer_id) REFERENCES streamers(streamer_id)
            ON DELETE SET NULL;

Grid-Nummern in der DB:
    '1'   Grid 1
    '2'   Grid 2  (wenn Grid-2b leer)
    '2a'  Grid 2a (wenn Grid-2b Einträge hat)
    '2b'  Grid 2b
    '3'   Grid 3
    'WL'  Warteliste
"""

import logging
import os
import sys
from datetime import datetime, timezone

import gspread
import mysql.connector
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Konfiguration ─────────────────────────────────────────────────────────────
load_dotenv("/etc/RTC_ApolloGrabber-env")

CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")
SPREADSHEET_ID   = os.getenv("SPREADSHEET_ID")
GRID_SHEET_NAME  = os.getenv("GRID_SHEET_NAME", "Grids")

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "charset":  "utf8mb4",
}

# ── Spalten-Layout (0-basiert) ────────────────────────────────────────────────
#
# Datenzeilen ab Index 4 (= Zeile 5 im Sheet).
#
# Grid-Blöcke (col_pos, col_driver, col_dr, col_stream_url):
#   Grid-1:  B=1(#)  C=2(Driver)  D=3(DR)   Stream-URL in B=1  (Zeile 22)
#   Grid-2:  G=6(#)  H=7(Driver)  I=8(DR)   Stream-URL in G=6  (Zeile 22)
#   Grid-2b: L=11(#) M=12(Driver) N=13(DR)  kein Stream
#   Grid-3:  Q=16(#) R=17(Driver) S=18(DR)  Stream-URL in Q=16 (Zeile 22)
#   Warte:   V=21(#) W=22(Driver) X=23(DR)  kein Stream
#
# Ranking-Lookup für Host-Rating:
#   AF=31 (PSN-Name), AG=32 (Ranking)
#
# Stream-URL-Zeile: Index 21 (= Zeile 22 im Sheet)

GRID_BLOCKS = [
    # (fixed_grid_number, col_pos, col_driver, col_dr, col_stream_url)
    # fixed_grid_number = None bedeutet: '2' oder '2a' je nach 2b-Inhalt
    ("1",   1,  2,  3,  1),
    (None,  6,  7,  8,  6),   # wird zu '2' oder '2a'
    ("2b", 11, 12, 13, None),
    ("3",  16, 17, 18, 16),
]

WAITLIST_BLOCK     = ("WL", 21, 22, 23, None)
RANKING_COL_NAME   = 31
RANKING_COL_RATING = 32
DATA_START_ROW     = 4
STREAM_URL_ROW     = 21   # Index 21 = Zeile 22


# ── Hilfsfunktionen ───────────────────────────────────────────────────────────

def _cell(row: list, col: int) -> str | None:
    if col < len(row):
        val = str(row[col]).strip()
        return val if val else None
    return None


def _parse_rating(raw: str | None) -> float:
    """
    '101,27%' → 1.0127
    Gibt 0.0 zurück wenn Wert fehlt, leer oder nicht parsebar.
    """
    if not raw:
        return 0.0
    val = raw.replace("%", "").replace(",", ".").strip()
    try:
        parsed = float(val)
        if parsed > 10:
            return round(parsed / 100.0, 6)
        return round(parsed, 6)
    except ValueError:
        return 0.0


def _detect_platform(url: str | None) -> str:
    """Erkennt die Streaming-Platform aus der URL."""
    if not url:
        return "other"
    url_lower = url.lower()
    if "twitch.tv" in url_lower:
        return "twitch"
    if "youtube.com" in url_lower or "youtu.be" in url_lower:
        return "youtube"
    return "other"


def _has_2b_entries(rows: list[list]) -> bool:
    """Prüft ob Grid-2b (Spalte L=11) mindestens einen echten Fahrer hat."""
    for row in rows[DATA_START_ROW:]:
        pos_raw = _cell(row, 11)
        drv_raw = _cell(row, 12)
        if not pos_raw or not drv_raw:
            continue
        try:
            int(pos_raw)
            if not drv_raw.startswith("Grid-"):
                return True
        except ValueError:
            continue
    return False


def _build_ranking_lookup(rows: list[list]) -> dict[str, float]:
    """Erstellt {psn_name_lower: rating_float} aus den Ranking-Spalten AF/AG."""
    lookup: dict[str, float] = {}
    for row in rows[DATA_START_ROW:]:
        name = _cell(row, RANKING_COL_NAME)
        if not name or name == "PSN-Name":
            continue
        lookup[name.lower()] = _parse_rating(_cell(row, RANKING_COL_RATING))
    return lookup


def _parse_all_grids(
    rows: list[list],
    ranking_lookup: dict[str, float],
    grid2_name: str,
) -> list[dict]:
    """
    Parst alle Grid-Blöcke und die Warteliste.

    Regeln:
    - DR == 'Streamer' → is_streamer=1, rating=None, stream_url aus Zeile 22
    - DR == 'Host'     → is_host=1, rating aus Ranking-Lookup (0.0 wenn fehlt)
    - Host == Streamer (gleicher Name) → überspringen
    - DR == normales Rating → normaler Fahrer
    - Fahrernamen die mit 'Grid-' beginnen → Header-Zeilen, überspringen
    - Warteliste leer → stillschweigend ignorieren
    """
    entries: list[dict] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Stream-URLs aus Zeile 22 vorab auslesen
    stream_url_row = rows[STREAM_URL_ROW] if len(rows) > STREAM_URL_ROW else []

    all_blocks = list(GRID_BLOCKS) + [WAITLIST_BLOCK]

    for block in all_blocks:
        fixed_gn, c_pos, c_drv, c_dr, c_url = block

        # Grid-2 oder Grid-2a?
        grid_number = fixed_gn if fixed_gn is not None else grid2_name

        # Stream-URL für diesen Block aus Zeile 22
        stream_url = _cell(stream_url_row, c_url) if c_url is not None else None

        streamer_name: str | None = None

        for row in rows[DATA_START_ROW:]:
            pos_raw = _cell(row, c_pos)
            drv_raw = _cell(row, c_drv)
            dr_raw  = _cell(row, c_dr)

            if not pos_raw or not drv_raw:
                continue
            try:
                pos_int = int(pos_raw)
            except ValueError:
                continue

            # Header-Zeilen überspringen (z.B. 'Grid-1', 'Grid-2a')
            if drv_raw.startswith("Grid-"):
                continue

            is_streamer  = 0
            is_host      = 0
            rating: float | None = None
            entry_stream_url: str | None = None

            if dr_raw == "Streamer":
                is_streamer      = 1
                streamer_name    = drv_raw.lower()
                rating           = None
                entry_stream_url = stream_url

            elif dr_raw == "Host":
                if streamer_name and drv_raw.lower() == streamer_name:
                    log.debug(
                        "Grid %s: Host '%s' == Streamer – wird übersprungen.",
                        grid_number, drv_raw,
                    )
                    continue
                is_host = 1
                rating  = ranking_lookup.get(drv_raw.lower(), 0.0)
                if rating == 0.0:
                    log.info(
                        "Grid %s: Host '%s' nicht im Ranking – Rating 0.00.",
                        grid_number, drv_raw,
                    )

            else:
                rating = _parse_rating(dr_raw)
                if rating == 0.0:
                    log.info(
                        "Grid %s: Fahrer '%s' ohne lesbares Rating ('%s') "
                        "– Rating 0.00.",
                        grid_number, drv_raw, dr_raw,
                    )

            entries.append({
                "grid_number":    grid_number,
                "position":       pos_int,
                "psn_name":       drv_raw,
                "current_rating": rating,
                "is_host":        is_host,
                "is_streamer":    is_streamer,
                "stream_url":     entry_stream_url,
                "updated_at":     now,
            })

    return entries


# ── Datenbank ─────────────────────────────────────────────────────────────────

def _fetch_driver_id_map(cursor) -> dict[str, int]:
    cursor.execute("SELECT driver_id, psn_name FROM drivers")
    return {row[1].lower(): row[0] for row in cursor.fetchall()}


def _upsert_streamer(cursor, name: str, url: str | None) -> int:
    """
    Legt einen Streamer an wenn er noch nicht existiert, oder aktualisiert
    die URL wenn sie sich geändert hat.
    Gibt die streamer_id zurück.
    """
    platform = _detect_platform(url)

    cursor.execute(
        "SELECT streamer_id, url FROM streamers WHERE name = %s",
        (name,)
    )
    existing = cursor.fetchone()

    if existing:
        streamer_id, existing_url = existing
        if existing_url != url and url is not None:
            cursor.execute(
                "UPDATE streamers SET url = %s, platform = %s WHERE streamer_id = %s",
                (url, platform, streamer_id)
            )
            log.info("Streamer '%s' aktualisiert (URL geändert).", name)
        return streamer_id
    else:
        cursor.execute(
            "INSERT INTO streamers (name, platform, url, is_active) "
            "VALUES (%s, %s, %s, 1)",
            (name, platform, url)
        )
        streamer_id = cursor.lastrowid
        log.info(
            "Neuer Streamer '%s' angelegt (platform=%s, id=%d).",
            name, platform, streamer_id,
        )
        return streamer_id


def _write_to_db(entries: list[dict]) -> tuple[int, int]:
    """
    Schreibt den Grid-Snapshot in die DB.
    Ablauf: TRUNCATE → Streamer upserten → INSERT mit FKs.
    Gibt (inserted_count, unmatched_count) zurück.
    """
    conn   = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        cursor.execute("TRUNCATE TABLE grid_assignments")
        log.info("Tabelle grid_assignments geleert.")

        driver_map = _fetch_driver_id_map(cursor)

        insert_sql = """
            INSERT INTO grid_assignments
                (grid_number, position, psn_name, driver_id, streamer_id,
                 current_rating, is_host, is_streamer, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        inserted  = 0
        unmatched = 0

        for e in entries:
            driver_id   = None
            streamer_id = None

            if e["is_streamer"]:
                streamer_id = _upsert_streamer(
                    cursor, e["psn_name"], e["stream_url"]
                )
            else:
                driver_id = driver_map.get(e["psn_name"].lower())
                if driver_id is None:
                    log.warning(
                        "Kein driver_id für '%s' (Grid %s, Pos %s) – "
                        "wird ohne FK gespeichert.",
                        e["psn_name"], e["grid_number"], e["position"],
                    )
                    unmatched += 1

            cursor.execute(insert_sql, (
                e["grid_number"],
                e["position"],
                e["psn_name"],
                driver_id,
                streamer_id,
                e["current_rating"],
                e["is_host"],
                e["is_streamer"],
                e["updated_at"],
            ))
            inserted += 1

        conn.commit()
        return inserted, unmatched

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    missing = [
        k for k in (
            "GOOGLE_CREDENTIALS_FILE", "SPREADSHEET_ID",
            "DB_USER", "DB_PASSWORD", "DB_NAME",
        )
        if not os.getenv(k)
    ]
    if missing:
        log.error("Fehlende .env-Variablen: %s", ", ".join(missing))
        sys.exit(1)

    # 1. Google Sheet laden – EIN API-Call
    log.info("Verbinde mit Google Sheets …")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds       = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    gc          = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    worksheet   = spreadsheet.worksheet(GRID_SHEET_NAME)

    log.info("Lade Sheet '%s' …", GRID_SHEET_NAME)
    all_values = worksheet.get_all_values()   # <-- einziger API-Call
    log.info(
        "Sheet geladen: %d Zeilen, %d Spalten.",
        len(all_values),
        max(len(r) for r in all_values) if all_values else 0,
    )

    # 2. Grid-2 oder Grid-2a?
    grid2_name = "2a" if _has_2b_entries(all_values) else "2"
    log.info("Grid-2 wird als '%s' gespeichert.", grid2_name)

    # 3. Daten parsen
    ranking_lookup = _build_ranking_lookup(all_values)
    log.info("Ranking-Lookup: %d Fahrer.", len(ranking_lookup))

    entries = _parse_all_grids(all_values, ranking_lookup, grid2_name)
    log.info("Einträge geparst: %d.", len(entries))

    if not entries:
        log.warning("Keine Einträge gefunden – DB wird nicht verändert.")
        sys.exit(0)

    # 4. In DB schreiben
    log.info("Schreibe in Datenbank …")
    inserted, unmatched = _write_to_db(entries)

    log.info(
        "Fertig: %d Einträge gespeichert, davon %d ohne driver_id-Match.",
        inserted, unmatched,
    )
    if unmatched:
        log.warning(
            "%d Fahrer nicht in der drivers-Tabelle gefunden – "
            "ohne FK gespeichert.",
            unmatched,
        )


if __name__ == "__main__":
    main()
