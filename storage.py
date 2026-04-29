from __future__ import annotations

import csv
import io
import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DATA_DIR = Path("data")
CACHE_DIR = Path("cache")
EXPORT_DIR = Path("exports")

RESULTS_FILE = DATA_DIR / "results.json"
RESULTS_JSONL_FILE = DATA_DIR / "results.jsonl"
RAW_HITS_FILE = DATA_DIR / "raw_hits.jsonl"
JOBS_FILE = DATA_DIR / "jobs.json"
SETTINGS_FILE = DATA_DIR / "settings.json"
LOG_FILE = DATA_DIR / "logs.jsonl"


DEFAULT_SETTINGS: dict[str, Any] = {
    # 0 means unlimited. Be careful on free Streamlit; the host can still restart/kill heavy apps.
    "max_depth": 0,
    "max_pages_total": 0,
    "max_pages_per_domain": 0,
    "http_concurrency": 25,
    "browser_concurrency": 1,
    "http_timeout": 12.0,
    "request_delay": 0.05,
    "use_browser_fallback": True,
    "browser_timeout_ms": 15000,
    "browser_steps": 10,
    "ajax_wait_seconds": 8.0,
    "scroll_rounds": 10,
    "same_domain_only": True,
    "save_every_results": 1,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_dirs() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    CACHE_DIR.mkdir(exist_ok=True)
    EXPORT_DIR.mkdir(exist_ok=True)

    if not RESULTS_FILE.exists():
        atomic_write_json(RESULTS_FILE, [])
    if not RESULTS_JSONL_FILE.exists():
        RESULTS_JSONL_FILE.write_text("", encoding="utf-8")
    if not RAW_HITS_FILE.exists():
        RAW_HITS_FILE.write_text("", encoding="utf-8")
    if not JOBS_FILE.exists():
        atomic_write_json(JOBS_FILE, [])
    if not SETTINGS_FILE.exists():
        atomic_write_json(SETTINGS_FILE, DEFAULT_SETTINGS)
    if not LOG_FILE.exists():
        LOG_FILE.write_text("", encoding="utf-8")


def atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        delete=False,
        dir=str(path.parent),
        suffix=".tmp",
    ) as tmp:
        json.dump(data, tmp, indent=2, ensure_ascii=False)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def load_json(path: Path, default: Any) -> Any:
    ensure_dirs()
    try:
        if not path.exists():
            return default
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            return default
        return json.loads(text)
    except Exception:
        return default


def save_settings(settings: dict[str, Any]) -> None:
    ensure_dirs()
    clean = DEFAULT_SETTINGS.copy()
    clean.update(settings or {})
    atomic_write_json(SETTINGS_FILE, clean)


def load_settings() -> dict[str, Any]:
    settings = load_json(SETTINGS_FILE, DEFAULT_SETTINGS.copy())
    clean = DEFAULT_SETTINGS.copy()
    clean.update(settings or {})
    return clean



def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    ensure_dirs()
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
                if isinstance(value, dict):
                    rows.append(value)
            except Exception:
                continue
    return rows


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_dirs()
    if not rows:
        return
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _stable_result_id(key: str) -> str:
    import hashlib
    return "r_" + hashlib.sha1(key.encode("utf-8", errors="ignore")).hexdigest()[:16]


def load_raw_hits() -> list[dict[str, Any]]:
    return _read_jsonl(RAW_HITS_FILE)


def append_raw_hits(rows: list[dict[str, Any]]) -> int:
    """
    Store every discovery event, including duplicates.
    This is the number that should match the crawler's raw discovery count.
    """
    clean_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        r = dict(row)
        r.setdefault("raw_saved_at", utc_now())
        clean_rows.append(r)

    _append_jsonl(RAW_HITS_FILE, clean_rows)
    return len(clean_rows)


def load_results() -> list[dict[str, Any]]:
    """
    Load unique reviewed result rows.

    Primary storage is JSONL because it is append-safe and more reliable on Streamlit Cloud.
    The old JSON file is still supported for backward compatibility.
    """
    rows = _read_jsonl(RESULTS_JSONL_FILE)

    if not rows:
        old_rows = load_json(RESULTS_FILE, [])
        if isinstance(old_rows, list):
            rows = old_rows

    # Deduplicate on load, preserving the latest manager fields for each normalized URL.
    by_key: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("normalized_url") or row.get("invite_url") or "").strip()
        if not key:
            continue
        if key not in by_key:
            order.append(key)
        by_key[key] = row

    return [by_key[k] for k in order]


def save_results(rows: list[dict[str, Any]]) -> None:
    """
    Rewrite unique result storage after manager edits.
    """
    ensure_dirs()
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()

    for row in rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("normalized_url") or row.get("invite_url") or "").strip()
        if not key or key in seen:
            continue
        r = dict(row)
        r.setdefault("id", _stable_result_id(key))
        r.setdefault("selected", False)
        r.setdefault("review_status", "unreviewed")
        r.setdefault("keep_status", "keep")
        r.setdefault("notes", "")
        r.setdefault("tags", "")
        r.setdefault("saved_at", utc_now())
        normalized.append(r)
        seen.add(key)

    # JSONL is primary.
    RESULTS_JSONL_FILE.write_text("", encoding="utf-8")
    _append_jsonl(RESULTS_JSONL_FILE, normalized)

    # JSON mirror helps simple manual inspection and old code compatibility.
    atomic_write_json(RESULTS_FILE, normalized)


def append_results(new_rows: list[dict[str, Any]]) -> dict[str, int]:
    """
    Append raw discoveries and unique results.

    Returns:
      raw_added: every discovery event saved to raw_hits.jsonl
      unique_added: new unique normalized URLs saved to results.jsonl/results.json
      duplicates: discoveries that were already known unique URLs
      total_unique: total unique rows after save
      total_raw: total raw discoveries after save
    """
    ensure_dirs()

    raw_added = append_raw_hits(new_rows)

    existing = load_results()
    seen = {str(r.get("normalized_url") or r.get("invite_url")) for r in existing}
    unique_added = 0
    duplicates = 0

    for row in new_rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("normalized_url") or row.get("invite_url") or "").strip()
        if not key:
            continue

        if key in seen:
            duplicates += 1
            continue

        r = dict(row)
        r.setdefault("id", _stable_result_id(key))
        r.setdefault("selected", False)
        r.setdefault("review_status", "unreviewed")
        r.setdefault("keep_status", "keep")
        r.setdefault("notes", "")
        r.setdefault("tags", "")
        r.setdefault("saved_at", utc_now())

        existing.append(r)
        seen.add(key)
        unique_added += 1

    if unique_added:
        save_results(existing)

    total_raw = len(load_raw_hits())
    total_unique = len(existing)

    return {
        "raw_added": raw_added,
        "unique_added": unique_added,
        "duplicates": duplicates,
        "total_unique": total_unique,
        "total_raw": total_raw,
    }


def get_storage_counts() -> dict[str, int]:
    return {
        "unique_saved": len(load_results()),
        "raw_saved": len(load_raw_hits()),
    }


def update_results_from_table(rows: list[dict[str, Any]]) -> None:
    """
    Save edited manager fields while preserving all crawler fields.
    """
    save_results(rows)


def append_log(level: str, message: str, **extra: Any) -> None:
    ensure_dirs()
    payload = {
        "time": utc_now(),
        "level": level.upper(),
        "message": message,
        **extra,
    }
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def read_logs(limit: int = 500) -> list[dict[str, Any]]:
    ensure_dirs()
    if not LOG_FILE.exists():
        return []

    lines = LOG_FILE.read_text(encoding="utf-8", errors="ignore").splitlines()
    selected = lines[-limit:]
    out: list[dict[str, Any]] = []

    for line in selected:
        try:
            out.append(json.loads(line))
        except Exception:
            out.append({"time": "", "level": "RAW", "message": line})

    return out


def clear_results() -> None:
    save_results([])
    RAW_HITS_FILE.write_text("", encoding="utf-8")
    append_log("INFO", "Saved unique results and raw discoveries cleared")


def clear_logs() -> None:
    ensure_dirs()
    LOG_FILE.write_text("", encoding="utf-8")


def clear_cache() -> None:
    ensure_dirs()
    if CACHE_DIR.exists():
        shutil.rmtree(CACHE_DIR)
    CACHE_DIR.mkdir(exist_ok=True)
    append_log("INFO", "Cache directory cleared")


def reset_all_local_data() -> None:
    ensure_dirs()
    save_results([])
    RAW_HITS_FILE.write_text("", encoding="utf-8")
    RESULTS_JSONL_FILE.write_text("", encoding="utf-8")
    atomic_write_json(JOBS_FILE, [])
    atomic_write_json(SETTINGS_FILE, DEFAULT_SETTINGS)
    clear_cache()
    clear_logs()
    append_log("INFO", "Local app data reset")


def rows_to_csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    if not rows:
        return b""

    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

    return buf.getvalue().encode("utf-8-sig")


def rows_to_json_bytes(rows: list[dict[str, Any]]) -> bytes:
    return json.dumps(rows, indent=2, ensure_ascii=False).encode("utf-8")


def import_backup_json(uploaded_bytes: bytes) -> tuple[bool, str]:
    try:
        data = json.loads(uploaded_bytes.decode("utf-8"))
    except Exception as exc:
        return False, f"Invalid JSON backup: {exc}"

    if not isinstance(data, list):
        return False, "Backup must be a JSON list of result rows."

    result = append_results(data)
    return True, (
        f"Imported {result['raw_added']} raw row(s), "
        f"{result['unique_added']} new unique row(s). "
        f"Total unique rows: {result['total_unique']}."
    )
