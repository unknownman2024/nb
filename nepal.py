#!/usr/bin/env python3
"""
Nepal Boxoffice + Advance Tracker + Aggregator (single file).

Modes:
  boxoffice   today's shows, filtered by rolling cutoff window (default 240 min)
  advance     future-date shows (offset N days or explicit --date YYYY-MM-DD)

Commands:
  fetch   pull from Khalti → save local JSON (never deletes, only adds/updates)
  build   aggregate local JSON → compact dataset (movies/index/daily/advance + per-movie)

Examples:
  python nepal.py fetch --mode boxoffice --cutoff-minutes 240 --build
  python nepal.py fetch --mode advance --offset 1 --build
  python nepal.py fetch --mode advance --date 2026-12-12 --build
  python nepal.py build
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import requests

# =========================================================
# CONFIG
# =========================================================

IST = ZoneInfo(os.getenv("NEPAL_TZ", "Asia/Kathmandu"))

MOVIE_LIST_URL = "https://khaltimovies.text2024mail.workers.dev/"
MOVIE_INFO_URL = "https://khalti.com/api/v5/movie-info/{movie_id}"
SHOWINFO_URL   = "https://khalti.com/api/v5/service/use/movie/showinfo-v2/"
TOKEN_URL      = "https://boxoffice24.pages.dev/Nepal/khaltitoken.txt"

DEFAULT_BO_DIR   = "Nepal Boxoffice"
DEFAULT_ADV_DIR  = "Nepal Advance"
DEFAULT_DATA_DIR = "Nepal Data"

MAX_WORKERS         = 10
MAX_RETRIES         = 5
TIMEOUT             = 15
GLOBAL_COOLDOWN_SEC = 6
DEFAULT_CUTOFF      = 240

# =========================================================
# UTILS
# =========================================================

def log(msg: str) -> None:
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def atomic_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def canon(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def write_if_changed(path: Path, obj: Any) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    new = canon(obj) + "\n"
    if path.exists():
        try:
            if sha(path.read_text(encoding="utf-8")) == sha(new):
                return False
        except Exception:
            pass
    path.write_text(new, encoding="utf-8")
    return True


def to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None or isinstance(v, bool):
            return default
        return int(float(v))
    except Exception:
        return default


def round2(v: float) -> float:
    return round(v + 1e-12, 2)


def occupancy(sold: int, reserved: int, seats: int) -> float:
    if seats <= 0:
        return 0.0
    return round2(((sold + reserved) / seats) * 100.0)


def compact_day(d: str) -> str:
    return d.replace("-", "")[:8]


def slugify(name: str) -> str:
    s = name.strip().lower().replace("&", " and ")
    s = re.sub(r"[\u2010-\u2015/\\|]+", "-", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or "movie"


def parse_show_datetime(dt_str: str) -> Optional[datetime]:
    if not dt_str:
        return None
    s = str(dt_str).replace("T", " ").strip()
    if "." in s:
        s = s.split(".")[0]
    if s.endswith("Z"):
        s = s[:-1]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=IST)
        except ValueError:
            continue
    return None


def split_show_datetime(dt: str, fallback_date: str) -> Tuple[str, str]:
    if not dt:
        return fallback_date, ""
    s = str(dt).replace("T", " ").strip()
    parts = s.split(" ")
    date_part = parts[0][:10] if parts and parts[0] else fallback_date
    time_part = parts[1][:5] if len(parts) >= 2 and parts[1] else ""
    return date_part, time_part



# =========================================================
# HTTP
# =========================================================

AUTH_TOKEN: Optional[str] = None
_cooldown_lock = threading.Lock()
_cooldown_until = 0.0
_cooldown_active = False


def init_token() -> None:
    global AUTH_TOKEN
    log("🔑 Fetching token once...")
    r = requests.get(TOKEN_URL, timeout=TIMEOUT)
    r.raise_for_status()
    AUTH_TOKEN = r.text.strip()
    log("✅ Token locked for this run")


def _trigger_cooldown() -> None:
    global _cooldown_until, _cooldown_active
    with _cooldown_lock:
        _cooldown_until = time.time() + GLOBAL_COOLDOWN_SEC
        if not _cooldown_active:
            log(f"🧊 GLOBAL COOLDOWN {GLOBAL_COOLDOWN_SEC}s")
            _cooldown_active = True


def _wait_if_cooldown() -> None:
    global _cooldown_active
    while True:
        with _cooldown_lock:
            remaining = _cooldown_until - time.time()
        if remaining <= 0:
            if _cooldown_active:
                log("✅ Cooldown ended → resume")
                _cooldown_active = False
            return
        time.sleep(min(1.0, remaining))


def random_ip() -> str:
    return ".".join(str(random.randint(0, 255)) for _ in range(4))


def random_user_agent() -> str:
    chrome = random.randint(120, 135)
    android = random.randint(6, 11)
    model = random.choice(["Pixel 4", "Nexus 5", "Moto G5", "Galaxy S7", "Redmi Note 8"])
    return (f"Mozilla/5.0 (Linux; Android {android}; {model}) "
            f"AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{chrome}.0.0.0 Mobile Safari/537.36")


def random_device_id() -> str:
    return "kwa-" + uuid.uuid4().hex[:16]


def safe_request(method: str, url: str, **kwargs) -> requests.Response:
    retries = MAX_RETRIES
    while True:
        _wait_if_cooldown()
        try:
            r = requests.request(method, url, timeout=TIMEOUT, **kwargs)
            if r.status_code == 429:
                _trigger_cooldown()
                if retries <= 0:
                    raise Exception("HTTP 429")
                retries -= 1
                continue
            if not r.ok:
                if r.status_code == 404:
                    r.raise_for_status()
                if retries <= 0:
                    r.raise_for_status()
                time.sleep(1.5)
                retries -= 1
                continue
            return r
        except (requests.ConnectionError, requests.Timeout):
            if retries <= 0:
                raise
            time.sleep(1.5)
            retries -= 1

# =========================================================
# FETCH
# =========================================================

def fetch_show_summary(movie_id: str, movie_name: str, show_id: str,
                       fallback_date: str) -> Dict[str, Any]:
    try:
        headers = {
            "accept": "application/json, text/plain, */*",
            "authorization": AUTH_TOKEN,
            "content-type": "application/json",
            "deviceid": random_device_id(),
            "origin": "https://web.khalti.com",
            "referer": "https://web.khalti.com",
            "user-agent": random_user_agent(),
            "x-forwarded-for": random_ip(),
        }
        payload = {"show_id": show_id, "new_layout": False}
        r = safe_request("POST", SHOWINFO_URL, headers=headers, json=payload)
        data = r.json()

        seat_data = data.get("new_seats") or []
        showinfo  = data.get("showinfo") or {}
        tickets   = showinfo.get("tickets") or []

        total = {"seats": 0, "sold": 0, "reserved": 0, "available": 0, "gross": 0}
        ticket_types: Dict[Any, Dict[str, int]] = {}

        for row in seat_data:
            for s in row.get("seats", []):
                if not s.get("is_active") or s.get("seat_status") == "Gap":
                    continue
                t = s.get("ticket_type")
                tt = ticket_types.setdefault(
                    t, {"price": 0, "seats": 0, "sold": 0, "reserved": 0, "available": 0}
                )
                tt["seats"] += 1
                if s.get("seat_status") == "Sold":
                    tt["sold"] += 1
                elif s.get("seat_status") == "Reserved":
                    tt["reserved"] += 1
                else:
                    tt["available"] += 1

        for t in tickets:
            level = t.get("price_level")
            price = round((t.get("price") or 0) / 100)
            if level in ticket_types:
                ticket_types[level]["price"] = price

        for tt in ticket_types.values():
            gross = tt["price"] * (tt["sold"] + tt["reserved"])
            total["seats"]     += tt["seats"]
            total["sold"]      += tt["sold"]
            total["reserved"]  += tt["reserved"]
            total["available"] += tt["available"]
            total["gross"]     += gross

        occ = round2(100 * (total["sold"] + total["reserved"]) / total["seats"]) if total["seats"] else 0

        show = showinfo.get("show", {}) or {}
        dt = show.get("datetime") or ""
        date_part, time_part = split_show_datetime(dt, fallback_date)

        theatre_full = f"{show.get('theatre_name')} - {show.get('auditorium_name')}"
        venue = show_id.split(":")[1] if ":" in show_id else show_id

        return {
            "movie_id": movie_id,
            "movie_name": movie_name,
            "show_id": show_id,
            "venue": venue,
            "theatre": theatre_full,
            "date": date_part,
            "time": time_part,
            "seats": total["seats"],
            "sold": total["sold"],
            "reserved": total["reserved"],
            "available": total["available"],
            "gross": total["gross"],
            "occupancy_percent": occ,
        }
    except Exception as e:
        return {
            "movie_id": movie_id, "movie_name": movie_name, "show_id": show_id,
            "venue": None, "theatre": None, "date": fallback_date, "time": None,
            "seats": 0, "sold": 0, "reserved": 0, "available": 0, "gross": 0,
            "occupancy_percent": 0, "error": str(e), "skipped": True,
        }


def fetch_movie_list() -> List[Dict[str, str]]:
    log("📥 Fetching movie list...")
    r = safe_request("GET", MOVIE_LIST_URL)
    data = r.json()
    movies = data.get("movies", [])
    log(f"🎬 Movies found: {len(movies)}")
    return [{"id": m.get("idx"), "name": m.get("name")}
            for m in movies if m.get("idx")]


def process_movie(movie_id: str, movie_name: str,
                  target_date: date, mode: str, cutoff_minutes: int
                  ) -> List[Dict[str, Any]]:
    log(f"\n🎥 Processing: {movie_name}")
    try:
        r = safe_request("GET", MOVIE_INFO_URL.format(movie_id=movie_id))
        movie_json = r.json()
    except requests.exceptions.HTTPError as e:
        if getattr(e.response, "status_code", None) == 404:
            log(f"⛔ Skipping {movie_name} → invalid movie_id")
            return []
        raise

    theatres = movie_json.get("theatres", []) or []
    now = datetime.now(IST)
    show_ids: List[str] = []

    for t in theatres:
        for s in t.get("shows", []) or []:
            dt_str = s.get("datetime")
            if not dt_str:
                continue
            show_dt = parse_show_datetime(dt_str)
            if not show_dt:
                continue
            if mode == "boxoffice":
                mins_left = (show_dt - now).total_seconds() / 60.0
                if not (0 <= mins_left <= cutoff_minutes):
                    continue
            else:
                if show_dt.date() != target_date:
                    continue
            sid = s.get("show_id")
            if sid:
                show_ids.append(sid)

    total = len(show_ids)
    label = f"within {cutoff_minutes}m" if mode == "boxoffice" else target_date.isoformat()
    log(f"🎯 {movie_name} → {total} shows ({label})")
    if total == 0:
        return []

    results: List[Dict[str, Any]] = []
    completed = 0
    lock = threading.Lock()
    fallback = target_date.isoformat()

    def wrapped(sid: str) -> Dict[str, Any]:
        nonlocal completed
        res = fetch_show_summary(movie_id, movie_name, sid, fallback)
        with lock:
            completed += 1
            log(f"📊 {movie_name} → {completed}/{total}")
        return res

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(wrapped, sid) for sid in show_ids]
        for f in as_completed(futures):
            results.append(f.result())
    return results


def build_summary_by_movie(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    movies: Dict[str, Dict[str, Any]] = {}
    for s in rows:
        mid = s["movie_id"]
        mname = s["movie_name"]
        venue = s.get("venue") or "Unknown"

        m = movies.setdefault(mid, {
            "movie_id": mid, "movie_name": mname, "total_shows": 0,
            "seats": 0, "sold": 0, "reserved": 0, "available": 0,
            "housefull": 0, "fastfilling": 0, "venues": {},
        })
        m["total_shows"] += 1
        m["seats"]     += s["seats"]
        m["sold"]      += s["sold"]
        m["reserved"]  += s["reserved"]
        m["available"] += s["available"]

        occ = s["occupancy_percent"]
        if occ >= 98:
            m["housefull"] += 1
        elif 50 <= occ < 98:
            m["fastfilling"] += 1

        v = m["venues"].setdefault(venue, {
            "venue": venue, "total_shows": 0,
            "seats": 0, "sold": 0, "reserved": 0, "available": 0,
            "housefull": 0, "fastfilling": 0,
        })
        v["total_shows"] += 1
        v["seats"]     += s["seats"]
        v["sold"]      += s["sold"]
        v["reserved"]  += s["reserved"]
        v["available"] += s["available"]
        if occ >= 98:
            v["housefull"] += 1
        elif 50 <= occ < 98:
            v["fastfilling"] += 1

    out = []
    for m in movies.values():
        m["venues"] = sorted(m["venues"].values(), key=lambda x: x["venue"])
        out.append(m)
    return out




# =========================================================
# FETCH + SAVE  (never deletes — only adds/updates)
# =========================================================

def fetch_and_save(target_date: date, mode: str, cutoff_minutes: int,
                   bo_dir: Path, adv_dir: Path) -> Tuple[int, int, int]:
    target_iso = target_date.isoformat()
    base_dir = bo_dir if mode == "boxoffice" else adv_dir
    year_dir = base_dir / target_date.strftime("%Y")
    year_dir.mkdir(parents=True, exist_ok=True)
    detail_file  = year_dir / f"{target_iso}_Detailed.json"
    summary_file = year_dir / f"{target_iso}_Summary.json"

    existing: List[Dict[str, Any]] = []
    if detail_file.exists():
        try:
            existing = json.loads(detail_file.read_text(encoding="utf-8")).get("shows", []) or []
            log(f"📁 Loaded existing DB: {len(existing)} shows")
        except Exception as e:
            log(f"⚠ Existing DB unreadable ({e}) → fresh start")

    init_token()
    movies = fetch_movie_list()
    new_rows: List[Dict[str, Any]] = []
    for m in movies:
        new_rows.extend(process_movie(m["id"], m["name"],
                                      target_date, mode, cutoff_minutes))

    log(f"\n🆕 Newly fetched this run: {len(new_rows)}")

    merged: Dict[str, Dict[str, Any]] = {
        s["show_id"]: s for s in existing if s.get("show_id")
    }
    for s in new_rows:
        sid = s.get("show_id")
        if not sid:
            continue
        prev = merged.get(sid)
        if s.get("skipped") and prev and not prev.get("skipped"):
            continue
        merged[sid] = s

    all_rows = list(merged.values())

    if len(all_rows) < len(existing):
        log(f"⚠ shows decreased: {len(existing)} → {len(all_rows)} (investigate)")

    timestamp = datetime.now(IST).strftime("%I:%M %p, %d %B %Y")
    atomic_dump(detail_file, {
        "date": target_iso, "mode": mode,
        "lastUpdated": timestamp, "shows": all_rows,
    })
    atomic_dump(summary_file, build_summary_by_movie(all_rows))

    print("\n================================================")
    print(f"Mode       : {mode}")
    print(f"Target     : {target_iso}")
    print(f"New shows  : {len(new_rows)}")
    print(f"Total      : {len(all_rows)}")
    print(f"Summary    → {summary_file}")
    print(f"Detailed   → {detail_file}")
    print("================================================\n")
    return len(new_rows), len(all_rows), len(existing)

# =========================================================
# AGGREGATION
# =========================================================

def discover_files(base_dir: Path, start: Optional[date], end: Optional[date]) -> List[Path]:
    if not base_dir.exists():
        return []
    out: List[Path] = []
    for p in sorted(base_dir.glob("*/*_Detailed.json")):
        ds = p.name[:10]
        try:
            d = datetime.strptime(ds, "%Y-%m-%d").date()
        except Exception:
            continue
        if start and d < start:
            continue
        if end and d > end:
            continue
        out.append(p)
    return out


def load_json_sync(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[read error] {path} → {e}")
        return None


async def load_many(paths: List[Path], concurrency: int
                    ) -> List[Optional[Dict[str, Any]]]:
    sem = asyncio.Semaphore(concurrency)
    async def worker(p: Path):
        async with sem:
            return await asyncio.to_thread(load_json_sync, p)
    return await asyncio.gather(*(worker(p) for p in paths))


def normalize_show(raw: Dict[str, Any], fallback_date: str) -> Dict[str, Any]:
    d = str(raw.get("date") or "").strip()
    d = d[:10] if len(d) >= 10 and d[4] == "-" else fallback_date
    return {
        "movie_id":   str(raw.get("movie_id", "")).strip(),
        "movie_name": str(raw.get("movie_name", "")).strip(),
        "show_id":    str(raw.get("show_id", "")).strip(),
        "venue":      str(raw.get("venue", "")).strip(),
        "theatre":    str(raw.get("theatre", "")).strip(),
        "date":       d,
        "seats":      to_int(raw.get("seats")),
        "sold":       to_int(raw.get("sold")),
        "reserved":   to_int(raw.get("reserved")),
        "available":  to_int(raw.get("available")),
        "gross":      to_int(raw.get("gross")),
    }


def aggregate(payloads: List[Dict[str, Any]]) -> Tuple[
    Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]
]:
    seen: Set[Tuple[str, str, str]] = set()
    movies: Dict[str, Dict[str, Any]] = {}
    source_dates: List[str] = []
    daily: Dict[str, Dict[str, Any]] = {}
    today_iso = datetime.now(IST).date().isoformat()

    for payload in payloads:
        if not payload:
            continue
        file_date = str(payload.get("date") or "").strip()
        if file_date:
            source_dates.append(file_date)
        shows = payload.get("shows") or []
        if not isinstance(shows, list):
            continue

        for raw in shows:
            if not isinstance(raw, dict):
                continue
            s = normalize_show(raw, file_date)
            mid, mname, sid, d = s["movie_id"], s["movie_name"], s["show_id"], s["date"]
            if not (mid and mname and sid and d):
                continue
            key = (d, mid, sid)
            if key in seen:
                continue
            seen.add(key)
            is_played = d <= today_iso

            if mid not in movies:
                movies[mid] = {
                    "movie_name": mname,
                    "first_date": d, "last_date": d,
                    "venues": set(), "daily": {},
                    "totals":   {"gross": 0, "sold": 0, "reserved": 0, "seats": 0, "shows": 0},
                    "played":   {"gross": 0, "sold": 0, "reserved": 0, "seats": 0, "shows": 0},
                    "upcoming": {"gross": 0, "sold": 0, "reserved": 0, "seats": 0, "shows": 0},
                }
            ms = movies[mid]
            ms["movie_name"] = mname or ms["movie_name"]
            ms["first_date"] = min(ms["first_date"], d)
            ms["last_date"]  = max(ms["last_date"],  d)
            if s["venue"]:
                ms["venues"].add(s["venue"])

            bucket = ms["played"] if is_played else ms["upcoming"]
            for tgt in (ms["totals"], bucket):
                tgt["gross"]    += s["gross"]
                tgt["sold"]     += s["sold"]
                tgt["reserved"] += s["reserved"]
                tgt["seats"]    += s["seats"]
                tgt["shows"]    += 1

            day = ms["daily"].setdefault(d, {"gross": 0, "sold": 0, "reserved": 0,
                                              "seats": 0, "shows": 0, "venues": set()})
            day["gross"]    += s["gross"]
            day["sold"]     += s["sold"]
            day["reserved"] += s["reserved"]
            day["seats"]    += s["seats"]
            day["shows"]    += 1
            if s["venue"]:
                day["venues"].add(s["venue"])

            dd = daily.setdefault(d, {"gross": 0, "sold": 0, "reserved": 0,
                                       "seats": 0, "shows": 0,
                                       "movies": set(), "per_movie": {}})
            dd["gross"]    += s["gross"]
            dd["sold"]     += s["sold"]
            dd["reserved"] += s["reserved"]
            dd["seats"]    += s["seats"]
            dd["shows"]    += 1
            dd["movies"].add(mid)
            pm = dd["per_movie"].setdefault(mid, {"mn": mname, "gross": 0, "sold": 0,
                                                   "reserved": 0, "shows": 0})
            pm["gross"]    += s["gross"]
            pm["sold"]     += s["sold"]
            pm["reserved"] += s["reserved"]
            pm["shows"]    += 1

    movie_docs: Dict[str, Dict[str, Any]] = {}
    index_rows: List[Dict[str, Any]] = []

    for mid, ms in movies.items():
        db = []
        for d in sorted(ms["daily"].keys()):
            day = ms["daily"][d]
            db.append([
                compact_day(d),
                day["gross"],
                occupancy(day["sold"], day["reserved"], day["seats"]),
                day["reserved"], day["seats"], day["shows"], day["sold"],
                len(day["venues"]),
            ])

        def tot(t):
            return {"g": t["gross"], "l": t["sold"], "r": t["reserved"],
                    "h": t["shows"], "s": t["seats"],
                    "o": occupancy(t["sold"], t["reserved"], t["seats"])}

        movie_docs[mid] = {
            "mn": ms["movie_name"], "mi": mid,
            "t":   tot(ms["totals"]),
            "tpl": tot(ms["played"]),
            "tup": tot(ms["upcoming"]),
            "r":   {"f": ms["first_date"], "l": ms["last_date"], "d": len(db)},
            "db":  db,
        }
        index_rows.append({
            "mi": mid, "mn": ms["movie_name"],
            "g":  ms["totals"]["gross"],
            "gp": ms["played"]["gross"],
            "gu": ms["upcoming"]["gross"],
            "l":  ms["totals"]["sold"],
            "r":  ms["totals"]["reserved"],
            "h":  ms["totals"]["shows"],
            "v":  len(ms["venues"]),
            "s":  ms["totals"]["seats"],
            "o":  occupancy(ms["totals"]["sold"], ms["totals"]["reserved"], ms["totals"]["seats"]),
            "d":  len(db),
            "f":  ms["first_date"],
            "e":  ms["last_date"],
        })

    index_rows.sort(key=lambda x: (-x["gp"], x["mn"], x["mi"]))
    def rank(key: str):
        return [x["mi"] for x in sorted(index_rows, key=lambda x: (-x[key], x["mn"], x["mi"]))]

    index_doc = {
        "g": datetime.now(IST).isoformat(timespec="seconds"),
        "n": len(movie_docs),
        "m": index_rows,
        "r": {"g": rank("gp"), "l": rank("l"), "h": rank("h"),
              "o": rank("o"),  "v": rank("v"), "u": rank("gu")},
        "sd": len(set(source_dates)),
    }

    daily_doc: Dict[str, Any] = {"g": datetime.now(IST).isoformat(timespec="seconds"), "dates": {}}
    for d in sorted(daily.keys()):
        if d > today_iso:
            continue
        dd = daily[d]
        top = sorted(dd["per_movie"].items(), key=lambda kv: -kv[1]["gross"])[:10]
        daily_doc["dates"][d] = {
            "shows": dd["shows"], "movies": len(dd["movies"]),
            "seats": dd["seats"], "sold": dd["sold"],
            "reserved": dd["reserved"], "gross": dd["gross"],
            "occ": occupancy(dd["sold"], dd["reserved"], dd["seats"]),
            "top": [{"mi": mid, "mn": v["mn"], "gross": v["gross"]} for mid, v in top],
        }

    advance_doc: Dict[str, Any] = {"g": datetime.now(IST).isoformat(timespec="seconds"), "dates": {}}
    for d in sorted(daily.keys()):
        if d <= today_iso:
            continue
        dd = daily[d]
        ranked = sorted(dd["per_movie"].items(), key=lambda kv: -kv[1]["gross"])
        advance_doc["dates"][d] = {
            "shows": dd["shows"], "movies": len(dd["movies"]),
            "seats": dd["seats"], "sold": dd["sold"],
            "reserved": dd["reserved"], "gross": dd["gross"],
            "occ": occupancy(dd["sold"], dd["reserved"], dd["seats"]),
            "list": [
                {"mi": mid, "mn": v["mn"], "shows": v["shows"],
                 "gross": v["gross"], "sold": v["sold"], "reserved": v["reserved"]}
                for mid, v in ranked
            ],
        }

    return movie_docs, index_doc, daily_doc, advance_doc


def build_slug_registry(movie_docs: Dict[str, Any]) -> Dict[str, str]:
    taken: Dict[str, str] = {}
    out: Dict[str, str] = {}
    items = sorted(movie_docs.items(), key=lambda kv: (-kv[1]["t"]["g"], kv[1]["mn"], kv[0]))
    for mid, doc in items:
        base = slugify(doc["mn"])
        slug = base
        if slug in taken and taken[slug] != mid:
            i = 2
            while f"{base}-{i}" in taken:
                i += 1
            slug = f"{base}-{i}"
        taken[slug] = mid
        out[mid] = slug
    return out


async def build_dataset(bo_dir: Path, adv_dir: Path,
                        start: Optional[date], end: Optional[date],
                        out_dir: Path, concurrency: int,
                        keep_missing: bool) -> None:
    bo_files  = discover_files(bo_dir,  start, end)
    adv_files = discover_files(adv_dir, start, end)
    log(f"[build] boxoffice={len(bo_files)} advance={len(adv_files)}")

    all_files = bo_files + adv_files
    if not all_files:
        log("[build] nothing to aggregate")
        return

    payloads = await load_many(all_files, concurrency=concurrency)
    loaded: List[Dict[str, Any]] = []
    missing = 0
    for p, data in zip(all_files, payloads):
        if data is None:
            missing += 1
            if keep_missing:
                loaded.append({"date": p.name[:10], "shows": []})
            continue
        loaded.append(data)
    log(f"[build] loaded={len(loaded)} failed={missing}")

    movie_docs, index_doc, daily_doc, advance_doc = aggregate(loaded)
    slug_map = build_slug_registry(movie_docs)

    movies_doc = {"g": datetime.now(IST).isoformat(timespec="seconds"),
                  "n": len(movie_docs), "m": []}
    for mid, doc in sorted(movie_docs.items(),
                            key=lambda kv: (-kv[1]["t"]["g"], kv[1]["mn"], kv[0])):
        row = dict(doc)
        row["sl"] = slug_map[mid]
        movies_doc["m"].append(row)

    w_movies  = write_if_changed(out_dir / "movies.json",  movies_doc)
    w_index   = write_if_changed(out_dir / "index.json",   index_doc)
    w_daily   = write_if_changed(out_dir / "daily.json",   daily_doc)
    w_advance = write_if_changed(out_dir / "advance.json", advance_doc)

    movie_dir = out_dir / "movie"
    movie_dir.mkdir(parents=True, exist_ok=True)
    updated = 0
    for mid, doc in movie_docs.items():
        slug = slug_map[mid]
        file_doc = dict(doc)
        file_doc["sl"] = slug
        if write_if_changed(movie_dir / f"{slug}.json", file_doc):
            updated += 1

    log(f"[build] done | movies={'upd' if w_movies else 'same'} "
        f"index={'upd' if w_index else 'same'} "
        f"daily={'upd' if w_daily else 'same'} "
        f"advance={'upd' if w_advance else 'same'} "
        f"movie_files={updated}/{len(movie_docs)}")



# =========================================================
# CLI
# =========================================================

def resolve_target(args) -> date:
    today = datetime.now(IST).date()
    if getattr(args, "date", None) and getattr(args, "offset", None) is not None:
        raise SystemExit("--date and --offset are mutually exclusive")
    if getattr(args, "date", None):
        try:
            return datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit(f"Invalid --date: {args.date} (use YYYY-MM-DD)")
    if args.mode == "advance":
        offset = args.offset if args.offset is not None else 1
        if offset < 0:
            raise SystemExit("--offset must be >= 0")
        return today + timedelta(days=offset)
    return today


def run_fetch(args) -> None:
    target = resolve_target(args)
    log(f"\n🚀 FETCH | mode={args.mode} | date={target.isoformat()}")
    if args.mode == "boxoffice" and target != datetime.now(IST).date():
        raise SystemExit(f"boxoffice requires today; use --mode advance for {target}")
    fetch_and_save(
        target_date=target, mode=args.mode,
        cutoff_minutes=args.cutoff_minutes,
        bo_dir=Path(args.bo_dir), adv_dir=Path(args.adv_dir),
    )


def run_build(args) -> None:
    start = (datetime.strptime(args.start, "%Y-%m-%d").date()
             if getattr(args, "start", None) else None)
    end = (datetime.strptime(args.end, "%Y-%m-%d").date()
           if getattr(args, "end", None) else None)
    if start and end and end < start:
        raise SystemExit("--end must be >= --start")
    asyncio.run(build_dataset(
        bo_dir=Path(args.bo_dir), adv_dir=Path(args.adv_dir),
        start=start, end=end, out_dir=Path(args.out),
        concurrency=getattr(args, "concurrency", 100),
        keep_missing=getattr(args, "keep_missing", False),
    ))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Nepal Boxoffice + Advance Tracker + Aggregator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_fetch = sub.add_parser("fetch", help="Fetch show data from Khalti")
    p_fetch.add_argument("--mode", required=True, choices=["boxoffice", "advance"])
    p_fetch.add_argument("--offset", type=int, default=None)
    p_fetch.add_argument("--date", default=None)
    p_fetch.add_argument("--cutoff-minutes", type=int, default=DEFAULT_CUTOFF)
    p_fetch.add_argument("--build", action="store_true")
    p_fetch.add_argument("--bo-dir",  default=DEFAULT_BO_DIR)
    p_fetch.add_argument("--adv-dir", default=DEFAULT_ADV_DIR)
    p_fetch.add_argument("--out",     default=DEFAULT_DATA_DIR)

    p_build = sub.add_parser("build", help="Aggregate local files → compact dataset")
    p_build.add_argument("--start", default=None)
    p_build.add_argument("--end",   default=None)
    p_build.add_argument("--bo-dir",  default=DEFAULT_BO_DIR)
    p_build.add_argument("--adv-dir", default=DEFAULT_ADV_DIR)
    p_build.add_argument("--out",     default=DEFAULT_DATA_DIR)
    p_build.add_argument("--concurrency", type=int, default=100)
    p_build.add_argument("--keep-missing", action="store_true")

    args = parser.parse_args()
    if args.cmd == "fetch":
        run_fetch(args)
        if args.build:
            run_build(args)
    elif args.cmd == "build":
        run_build(args)


if __name__ == "__main__":
    main()
