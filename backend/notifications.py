"""
Notification dispatch — ntfy.sh and/or SMTP email.

Preferences are read from the DB at each trigger call so UI changes take effect immediately.
Env vars (config.py) serve as fallback when DB has no override.
"""

import json
import logging
import smtplib
import time
import urllib.request
from email.message import EmailMessage

import config

log = logging.getLogger(__name__)

_notified: set[str] = set()

# Prefs cache — avoid a DB query on every per-aircraft call
_prefs_cache: dict = {}
_prefs_cache_ts: float = 0.0
_PREFS_TTL = 10.0  # re-read DB at most every 10 seconds


def reset_daily() -> None:
    _notified.clear()
    log.debug("Notification dedup set cleared for new day")


def already_notified(key: str) -> bool:
    """Fast in-process check — call before dispatching asyncio.to_thread."""
    return key in _notified


def trigger_enabled(trigger: str, default: bool = False) -> bool:
    """Return whether a named trigger is enabled, using cached prefs.
    Call once per _push_updates cycle to gate entire trigger types."""
    prefs = _get_prefs()
    defaults: dict[str, bool] = {
        "notify_military":    config.NOTIFY_MILITARY,
        "notify_interesting": config.NOTIFY_INTERESTING,
        "notify_acas":        config.NOTIFY_ACAS,
        "notify_emergency":   config.NOTIFY_EMERGENCY,
    }
    return _pref_bool(prefs, trigger, defaults.get(trigger, default))


def any_channel() -> bool:
    return bool(config.NTFY_URL or config.NOTIFY_EMAIL_TO)


# Keep old name as alias so nothing else breaks
_any_channel = any_channel


def _get_prefs() -> dict:
    """Load prefs from DB, caching for _PREFS_TTL seconds."""
    global _prefs_cache, _prefs_cache_ts
    now = time.monotonic()
    if now - _prefs_cache_ts < _PREFS_TTL:
        return _prefs_cache
    try:
        from db import stats_db
        _prefs_cache = stats_db.get_notify_prefs()
    except Exception:
        _prefs_cache = {}
    _prefs_cache_ts = now
    return _prefs_cache


def _pref_bool(prefs: dict, key: str, default: bool) -> bool:
    v = prefs.get(key, "")
    if not v:
        return default
    return v.lower() not in ("0", "false", "no")


def _pref_range(prefs: dict, key: str) -> float | None:
    v = prefs.get(key, "")
    try:
        return float(v) if v else None
    except ValueError:
        return None


def _in_range(max_nm: float | None, aircraft_range_nm: float | None) -> bool:
    """True if no range limit set, or aircraft is within the limit."""
    if max_nm is None:
        return True
    if aircraft_range_nm is None:
        return True   # no position data — let it through
    return aircraft_range_nm <= max_nm


def _fetch_planespotters_thumb(icao: str) -> str | None:
    """Return thumbnail URL from planespotters.net, or None on failure."""
    try:
        url = f"https://api.planespotters.net/pub/photos/hex/{icao.upper()}"
        req = urllib.request.Request(url, headers={"User-Agent": "adsb-dashboard/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())
        photos = data.get("photos", [])
        if photos:
            return photos[0].get("thumbnail_large", {}).get("src") or \
                   photos[0].get("thumbnail", {}).get("src")
    except Exception as exc:
        log.debug("Planespotters lookup failed for %s: %s", icao, exc)
    return None


def _ntfy(title: str, body: str, priority: str = "default",
          tags: str = "", photo_url: str | None = None) -> None:
    if not config.NTFY_URL:
        return
    try:
        headers = {
            "Title":    title,
            "Priority": priority,
            "Tags":     tags,
        }
        if photo_url:
            headers["Attach"] = photo_url
        req = urllib.request.Request(
            config.NTFY_URL,
            data=body.encode(),
            headers=headers,
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
        log.debug("ntfy sent: %s", title)
    except Exception as exc:
        log.warning("ntfy send failed: %s", exc)


def _email(subject: str, body: str, photo_url: str | None = None) -> None:
    if not config.NOTIFY_EMAIL_TO:
        return
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"]    = config.NOTIFY_EMAIL_FROM or config.NOTIFY_EMAIL_TO
        msg["To"]      = config.NOTIFY_EMAIL_TO
        full_body = body
        if photo_url:
            full_body += f"\n\nPhoto: {photo_url}"
        msg.set_content(full_body)

        if config.NOTIFY_SMTP_PORT == 465:
            smtp = smtplib.SMTP_SSL(config.NOTIFY_SMTP_HOST, config.NOTIFY_SMTP_PORT, timeout=15)
        else:
            smtp = smtplib.SMTP(config.NOTIFY_SMTP_HOST, config.NOTIFY_SMTP_PORT, timeout=15)
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()

        with smtp:
            if config.NOTIFY_SMTP_USER:
                smtp.login(config.NOTIFY_SMTP_USER, config.NOTIFY_SMTP_PASS)
            smtp.send_message(msg)
        log.debug("Email sent: %s", subject)
    except Exception as exc:
        log.warning("Email send failed: %s", exc)


def _send(title: str, body: str, priority: str = "default",
          tags: str = "", icao: str | None = None) -> None:
    photo_url = _fetch_planespotters_thumb(icao) if icao else None
    _ntfy(title, body, priority, tags, photo_url)
    _email(title, body, photo_url)


# ---------------------------------------------------------------------------
# Trigger functions
# ---------------------------------------------------------------------------

def notify_emergency_squawk(icao: str, squawk: str, callsign: str | None,
                             altitude: int | None, operator: str | None) -> None:
    if not _any_channel():
        return
    prefs = _get_prefs()
    if not _pref_bool(prefs, "notify_emergency", config.NOTIFY_EMERGENCY):
        return
    key = f"emergency:{icao}:{squawk}"
    if key in _notified:
        return
    _notified.add(key)

    labels = {"7700": "General emergency", "7600": "Radio failure", "7500": "Hijack"}
    label = labels.get(squawk, squawk)
    ident = callsign or icao
    lines = [f"{ident} squawking {squawk} — {label}"]
    if altitude:
        lines.append(f"Altitude: {altitude:,} ft")
    if operator:
        lines.append(f"Operator: {operator}")
    lines.append(f"ICAO: {icao}")

    priority = "urgent" if squawk in ("7700", "7500") else "high"
    tags = "rotating_light" if squawk == "7700" else "warning"
    _send(f"Squawk {squawk} — {label}", "\n".join(lines), priority, tags, icao)


def notify_acas(icao: str, description: str, corrective: bool,
                registration: str | None, operator: str | None,
                altitude: int | None, range_nm: float | None = None) -> None:
    if not _any_channel():
        return
    key = f"acas:{icao}"
    if key in _notified:
        return
    prefs = _get_prefs()
    if not _pref_bool(prefs, "notify_acas", config.NOTIFY_ACAS):
        return
    max_nm = _pref_range(prefs, "acas_max_range_nm")
    if not _in_range(max_nm, range_nm):
        return
    _notified.add(key)

    ident = registration or icao
    kind  = "Corrective" if corrective else "Preventive"
    lines = [f"ACAS/TCAS RA ({kind}): {description}", f"Aircraft: {ident}"]
    if altitude:
        lines.append(f"Altitude: {altitude:,} ft")
    if operator:
        lines.append(f"Operator: {operator}")
    _send(f"ACAS RA — {ident}", "\n".join(lines), tags="warning", icao=icao)


def notify_watchlist(icao: str, callsign: str | None, registration: str | None,
                     operator: str | None, altitude: int | None,
                     range_nm: float | None = None,
                     max_range_nm: float | None = None) -> None:
    """max_range_nm comes from the watchlist entry in DB, not global prefs."""
    if not _any_channel():
        return
    if not _in_range(max_range_nm, range_nm):
        return
    key = f"watchlist:{icao}"
    if key in _notified:
        return
    _notified.add(key)

    ident = callsign or registration or icao
    lines = [f"Watchlist aircraft spotted: {ident}"]
    if range_nm is not None:
        lines.append(f"Range: {range_nm:.1f} nm")
    if altitude:
        lines.append(f"Altitude: {altitude:,} ft")
    if operator:
        lines.append(f"Operator: {operator}")
    lines.append(f"ICAO: {icao}")
    _send(f"Watchlist: {ident}", "\n".join(lines), priority="high", tags="airplane", icao=icao)


def notify_military(icao: str, callsign: str | None, operator: str | None,
                    country: str | None, altitude: int | None,
                    range_nm: float | None = None) -> None:
    if not _any_channel():
        return
    key = f"military:{icao}"
    if key in _notified:
        return
    prefs = _get_prefs()
    if not _pref_bool(prefs, "notify_military", config.NOTIFY_MILITARY):
        return
    max_nm = _pref_range(prefs, "military_max_range_nm")
    if not _in_range(max_nm, range_nm):
        return
    _notified.add(key)

    ident = callsign or icao
    lines = [f"Military aircraft spotted: {ident}"]
    if range_nm is not None:
        lines.append(f"Range: {range_nm:.1f} nm")
    if altitude:
        lines.append(f"Altitude: {altitude:,} ft")
    if operator:
        lines.append(f"Operator: {operator}")
    if country:
        lines.append(f"Country: {country}")
    lines.append(f"ICAO: {icao}")
    _send(f"Military: {ident}", "\n".join(lines), tags="military_helmet", icao=icao)


def notify_interesting(icao: str, callsign: str | None, type_code: str | None,
                       operator: str | None, altitude: int | None,
                       range_nm: float | None = None) -> None:
    if not _any_channel():
        return
    key = f"interesting:{icao}"
    if key in _notified:
        return
    prefs = _get_prefs()
    if not _pref_bool(prefs, "notify_interesting", config.NOTIFY_INTERESTING):
        return
    max_nm = _pref_range(prefs, "interesting_max_range_nm")
    if not _in_range(max_nm, range_nm):
        return
    _notified.add(key)

    ident = callsign or icao
    lines = [f"Interesting aircraft spotted: {ident}"]
    if type_code:
        lines.append(f"Type: {type_code}")
    if range_nm is not None:
        lines.append(f"Range: {range_nm:.1f} nm")
    if altitude:
        lines.append(f"Altitude: {altitude:,} ft")
    if operator:
        lines.append(f"Operator: {operator}")
    lines.append(f"ICAO: {icao}")
    _send(f"Interesting: {ident}", "\n".join(lines), tags="eyes", icao=icao)
