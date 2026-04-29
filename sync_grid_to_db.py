#!/usr/bin/env python3
"""
sync_grid_to_db.py
==================
Liest die aktuelle Grideinteilung aus dem Google Sheet (Tab "Grids")
und schreibt sie als Snapshot in die MySQL-Tabelle `grid_assignments`.

Aufruf direkt:
    python3 sync_grid_to_db.py

Aufruf vom Bot (non-blocking, wartet nicht auf Ergebnis):
    import subprocess
    subprocess.Popen(
        ["python3", "/home/user/sync_grid_to_db.py"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

Google Sheets API:
    Nur EIN get_all_values()-Call, um das Rate-Limit (60 Req/Min) zu schonen.
    Das komplette Sheet wird einmal geladen und dann lokal geparst.

Benötigte .env-Variablen:
    GOOGLE_CREDENTIALS_FILE   Pfad zur service-account JSON
    SPREADSHEET_ID            ID des Google Spreadsheets
    GRID_SHEET_NAME           Name des Tabs (Standard: Grids)
    DB_HOST                   MySQL-Host (Standard: localhost)
    DB_PORT                   MySQL-Port  (Standard: 3306)
    DB_USER                   MySQL-Benutzer
    DB_PASSWORD               MySQL-Passwort
    DB_NAME                   Datenbankname

SQL zum einmaligen Anlegen der Tabelle:
    CREATE TABLE grid_assignments (
        assignment_id  INT AUTO_INCREMENT PRIMARY KEY,
        grid_number    VARCHAR(5)    NOT NULL,
        position       TINYINT       NOT NULL,
        psn_name       VARCHAR(100)  NOT NULL,
        driver_id      INT           NULL,
        current_rating DECIMAL(6,4)  NULL,
        is_host        TINYINT(1)    NOT NULL DEFAULT 0,
        is_streamer    TINYINT(1)    NOT NULL DEFAULT 0,
        updated_at     DATETIME      NOT NULL,
        INDEX idx_grid (grid_number),
        FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
            ON DELETE SET NULL
    );

Grid-Nummern in der DB:
    '1'   Grid 1
    '2'   Grid 2 / 2a
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
load_dotenv()

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
# Datenzeilen starten ab Index 4 (= Zeile 5 im Sheet).
#
# Oberer Block (ohne Teams):
#   Grid-1:  B=1(#)  C=2(Driver)  D=3(DR)
#   Grid-2:  G=6(#)  H=7(Driver)  I=8(DR)
#   Grid-2b: L=11(#) M=12(Driver) N=13(DR)
#   Grid-3:  Q=16(#) R=17(Driver) S=18(DR)
#   Warte:   V=21(#) W=22(Driver) X=23(DR)
#
# Ranking-Lookup für Host-Rating (wenn DR-Zelle == 'Host'):
#   AF=31 (PSN-Name), AG=32 (Ranking in %-Format)

GRID_BLOCKS = [
    # (grid_number, col_pos, col_driver, col_dr)
    ("1",  1,  2,  3),
    ("2",  6,  7,  8),
    ("2b", 11, 12, 13),
    ("3",  16, 17, 18),
]

WAITLIST_BLOCK     = ("WL", 21, 22, 23)
RANKING_COL_NAME   = 31   # Spalte AF
RANKING_COL_RATING = 32   # Spalte AG
DATA_START_ROW     = 4    # Index 4 = Zeile 5


# ── Hilfsfunktionen ───────────────────────────────────────────────────────────

def _cell(row: list, col: int) -> str | None:
    """Gibt bereinigten Zellinhalt zurück, None wenn leer oder Spalte fehlt."""
    if col < len(row):
        val = str(row[col]).strip()
        return val if val else None
    return None


def _parse_rating(raw: str | None) -> float:
    """
    Wandelt '101,27%' → 1.0127.
    Gibt 0.0 zurück wenn der Wert fehlt, leer oder nicht parsebar ist
    (neue Fahrer ohne Ranking-Eintrag, Hosts nicht in der Liste, etc.).
    'Streamer' und 'Host' als Eingabe ergeben ebenfalls 0.0.
    """
    if not raw:
        return 0.0
    val = raw.replace("%", "").replace(",", ".").strip()
    try:
        parsed = float(val)
        # Plausibilitäts-Check: Ratings liegen als %-Wert zwischen ~99 und 115
        if parsed > 10:
            return round(parsed / 100.0, 6)
        return round(parsed, 6)
    except ValueError:
        return 0.0


def _build_ranking_lookup(rows: list[list]) -> dict[str, float]:
    """
    Erstellt {psn_name_lower: rating_float} aus den Ranking-Spalten AF/AG.
    Fahrer ohne Rating-Wert in der Liste → 0.0.
    """
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
) -> list[dict]:
    """
    Parst alle Grid-Blöcke und die Warteliste aus den Sheet-Rohdaten.

    Regeln:
    - DR == 'Streamer' → is_streamer=1, rating=None
    - DR == 'Host'     → is_host=1, rating aus Ranking-Lookup (0.0 wenn fehlt)
    - Pos.2 Host hat gleichen Namen wie Pos.1 Streamer → überspringen
    - DR == normales Rating → is_host=0, is_streamer=0, rating aus DR-Zelle
    - Warteliste leer → stillschweigend ignorieren
    """
    entries: list[dict] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    for block in list(GRID_BLOCKS) + [WAITLIST_BLOCK]:
        grid_number, c_pos, c_drv, c_dr = block
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

            is_streamer = 0
            is_host     = 0
            rating: float | None = None

            if dr_raw == "Streamer":
                is_streamer   = 1
                streamer_name = drv_raw.lower()
                rating        = None   # Streamer haben kein DR-Rating

            elif dr_raw == "Host":
                # Host identisch mit Streamer → kein zweiter Eintrag
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
                # Normaler Fahrer
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
                "updated_at":     now,
            })

    return entries


# ── Datenbank ─────────────────────────────────────────────────────────────────

def _fetch_driver_id_map(cursor) -> dict[str, int]:
    """Lädt alle PSN-Namen aus `drivers` als Lowercase-Lookup."""
    cursor.execute("SELECT driver_id, psn_name FROM drivers")
    return {row[1].lower(): row[0] for row in cursor.fetchall()}


def _write_to_db(entries: list[dict]) -> tuple[int, int]:
    """
    Schreibt den Grid-Snapshot in die DB.
    Ablauf: TRUNCATE → INSERT mit driver_id-Lookup.
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
                (grid_number, position, psn_name, driver_id,
                 current_rating, is_host, is_streamer, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        inserted  = 0
        unmatched = 0

        for e in entries:
            if e["is_streamer"]:
                driver_id = None   # Streamer nicht gegen drivers matchen
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

    # 2. Daten parsen
    ranking_lookup = _build_ranking_lookup(all_values)
    log.info("Ranking-Lookup: %d Fahrer.", len(ranking_lookup))

    entries = _parse_all_grids(all_values, ranking_lookup)
    log.info("Einträge geparst: %d.", len(entries))

    if not entries:
        log.warning("Keine Einträge gefunden – DB wird nicht verändert.")
        sys.exit(0)

    # 3. In DB schreiben
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
