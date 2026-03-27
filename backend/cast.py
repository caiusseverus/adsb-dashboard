"""
Chromecast notifier — rule evaluation, image generation, and Cast dispatch.

Preferences and rules are read from the DB at each trigger call so UI changes
take effect immediately without a restart.

Flow:
  main.py broadcast loop → cast.check(aircraft_snapshot)
    → rule match + time gate + cooldown check
    → generate short-lived token, store snapshot
    → send Cast command: Chromecast fetches /api/cast/display/<token>
    → /api/cast/display/<token> renders JPEG via Pillow (photo + data overlay)
"""

import io
import json
import logging
import time
import urllib.request
import uuid
from datetime import datetime

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Token store — maps token → (aircraft_snapshot, expiry_ts)
# Tokens expire after 90 s (plenty for Chromecast to fetch) or on first use.
# ---------------------------------------------------------------------------
_TOKEN_TTL = 90
_tokens: dict[str, tuple[dict, float]] = {}


def _store_token(aircraft: dict) -> str:
    token = uuid.uuid4().hex
    _tokens[token] = (aircraft, time.monotonic() + _TOKEN_TTL)
    _expire_tokens()
    return token


def consume_token(token: str) -> dict | None:
    """Return aircraft snapshot and remove token. Returns None if expired/unknown."""
    _expire_tokens()
    entry = _tokens.pop(token, None)
    if entry is None:
        return None
    snapshot, expiry = entry
    if time.monotonic() > expiry:
        return None
    return snapshot


def _expire_tokens() -> None:
    now = time.monotonic()
    expired = [t for t, (_, exp) in _tokens.items() if now > exp]
    for t in expired:
        _tokens.pop(t, None)


# ---------------------------------------------------------------------------
# Cooldown — prevent re-notifying the same ICAO within the configured window
# ---------------------------------------------------------------------------
_cooldown: dict[str, float] = {}  # icao → last_cast_ts


def _on_cooldown(icao: str, cooldown_minutes: int) -> bool:
    last = _cooldown.get(icao)
    if last is None:
        return False
    return (time.time() - last) < (cooldown_minutes * 60)


def _mark_cast(icao: str) -> None:
    _cooldown[icao] = time.time()


# ---------------------------------------------------------------------------
# Config helpers — read from DB each call (TTL-cached in caller)
# ---------------------------------------------------------------------------

_config_cache: dict = {}
_config_cache_ts: float = 0.0
_CONFIG_TTL = 10.0  # re-read DB at most every 10 s


def _get_config() -> dict:
    global _config_cache, _config_cache_ts
    now = time.time()
    if now - _config_cache_ts < _CONFIG_TTL:
        return _config_cache
    from db import stats_db
    _config_cache = stats_db.get_cast_config()
    _config_cache_ts = now
    return _config_cache


def _get_rules() -> list[dict]:
    from db import stats_db
    return stats_db.get_cast_rules()


# ---------------------------------------------------------------------------
# Time gate
# ---------------------------------------------------------------------------

def _in_active_hours(cfg: dict) -> bool:
    """Return True if current local time is within the configured active window."""
    start_str = cfg.get("active_hours_start", "")
    end_str   = cfg.get("active_hours_end", "")
    if not start_str or not end_str:
        return True  # no restriction configured
    try:
        now_t = datetime.now().time()
        start = datetime.strptime(start_str, "%H:%M").time()
        end   = datetime.strptime(end_str,   "%H:%M").time()
        if start <= end:
            return start <= now_t <= end
        # overnight window e.g. 22:00–06:00
        return now_t >= start or now_t <= end
    except ValueError:
        log.warning("cast: invalid active_hours config: %r %r", start_str, end_str)
        return True


# ---------------------------------------------------------------------------
# Rule evaluation
# ---------------------------------------------------------------------------

_VALID_MATCH_TYPES = {"icao", "military", "watchlist", "interesting", "emergency", "any"}


def _rule_matches(rule: dict, ac: dict) -> bool:
    """Return True if the aircraft matches this rule's criteria."""
    if not rule.get("enabled", 1):
        return False

    match_type = rule.get("match_type", "")
    match_value = (rule.get("match_value") or "").strip().upper()

    if match_type == "icao":
        if ac.get("icao", "").upper() != match_value:
            return False
    elif match_type == "military":
        if not ac.get("military"):
            return False
    elif match_type == "watchlist":
        if not ac.get("watched"):
            return False
    elif match_type == "interesting":
        if not ac.get("interesting"):
            return False
    elif match_type == "emergency":
        if ac.get("squawk") not in ("7500", "7600", "7700"):
            return False
    elif match_type == "any":
        pass  # always matches
    else:
        log.warning("cast: unknown match_type %r, skipping rule", match_type)
        return False

    # Range gate — only applied when aircraft has a known position
    max_nm = rule.get("max_range_nm")
    if max_nm is not None:
        ac_range = ac.get("range_nm")
        if ac_range is not None and ac_range > max_nm:
            return False

    return True


# ---------------------------------------------------------------------------
# Image generation
# ---------------------------------------------------------------------------

def _fetch_photo(icao: str) -> bytes | None:
    """Fetch thumbnail image bytes from planespotters.net, or None on failure."""
    try:
        api_url = f"https://api.planespotters.net/pub/photos/hex/{icao.upper()}"
        req = urllib.request.Request(api_url, headers={"User-Agent": "adsb-dashboard/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())
        photos = data.get("photos", [])
        if not photos:
            return None
        src = (photos[0].get("thumbnail_large") or {}).get("src") or \
              (photos[0].get("thumbnail") or {}).get("src")
        if not src:
            return None
        with urllib.request.urlopen(
            urllib.request.Request(src, headers={"User-Agent": "adsb-dashboard/1.0"}),
            timeout=10,
        ) as img_resp:
            return img_resp.read()
    except Exception as exc:
        log.debug("cast: photo fetch failed for %s: %s", icao, exc)
        return None


def render_display_image(aircraft: dict) -> bytes:
    """
    Generate a 1280×720 JPEG for display on the Chromecast.
    Shows aircraft photo (if available) with a data overlay.
    Returns raw JPEG bytes.
    """
    from PIL import Image, ImageDraw, ImageFont

    W, H = 1280, 720
    BG       = (11, 12, 16)       # #0b0c10 — matches dashboard background
    CARD     = (22, 27, 34)       # #161b22
    ACCENT   = (56, 139, 253)     # #388bfd blue
    TEXT     = (201, 209, 217)    # #c9d1d9
    SUBTEXT  = (110, 118, 129)    # muted
    WHITE    = (255, 255, 255)
    MILITARY = (188, 140, 255)    # #bc8cff purple

    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    # --- load fonts (fall back to default if not available) ---
    def _font(size: int):
        try:
            return ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", size)
        except Exception:
            return ImageFont.load_default()

    def _font_regular(size: int):
        try:
            return ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", size)
        except Exception:
            return ImageFont.load_default()

    font_large  = _font(52)
    font_medium = _font(32)
    font_small  = _font_regular(22)
    font_tiny   = _font_regular(18)

    # --- photo panel (left half) ---
    photo_bytes = _fetch_photo(aircraft.get("icao", ""))
    if photo_bytes:
        try:
            photo = Image.open(io.BytesIO(photo_bytes)).convert("RGB")
            # fill left 640×720, letterboxed
            photo.thumbnail((640, 720), Image.LANCZOS)
            px = (640 - photo.width)  // 2
            py = (720 - photo.height) // 2
            img.paste(photo, (px, py))
            # subtle gradient overlay on right edge of photo to blend into data panel
            for x in range(560, 640):
                alpha = (x - 560) / 80
                for y in range(H):
                    r0, g0, b0 = img.getpixel((x, y))
                    br, bg, bb = BG
                    blended = (
                        int(r0 * (1 - alpha) + br * alpha),
                        int(g0 * (1 - alpha) + bg * alpha),
                        int(b0 * (1 - alpha) + bb * alpha),
                    )
                    img.putpixel((x, y), blended)
        except Exception as exc:
            log.debug("cast: photo render failed: %s", exc)

    # --- data panel (right half) ---
    PX = 680  # left edge of data panel
    PY = 60   # top padding

    icao      = aircraft.get("icao", "").upper()
    callsign  = aircraft.get("callsign") or ""
    operator  = aircraft.get("operator") or ""
    type_desc = aircraft.get("type_desc") or aircraft.get("type_code") or ""
    altitude  = aircraft.get("altitude")
    squawk    = aircraft.get("squawk") or ""
    range_nm  = aircraft.get("range_nm")
    military  = aircraft.get("military", False)
    reg       = aircraft.get("registration") or ""

    # callsign / ICAO headline
    headline = callsign if callsign else icao
    draw.text((PX, PY), headline, font=font_large, fill=WHITE)
    if callsign:
        draw.text((PX, PY + 58), icao, font=font_small, fill=SUBTEXT)

    y = PY + 110

    # military badge
    if military:
        draw.rectangle([PX, y, PX + 120, y + 34], fill=MILITARY)
        draw.text((PX + 8, y + 6), "MILITARY", font=font_tiny, fill=BG)
        y += 46

    # emergency squawk badge
    if squawk in ("7500", "7600", "7700"):
        labels = {"7500": "HIJACK", "7600": "RADIO FAIL", "7700": "EMERGENCY"}
        draw.rectangle([PX, y, PX + 180, y + 34], fill=(218, 54, 51))
        draw.text((PX + 8, y + 6), labels[squawk], font=font_tiny, fill=WHITE)
        y += 46

    y += 10

    # data rows
    def row(label: str, value: str, colour=TEXT) -> None:
        nonlocal y
        draw.text((PX, y), label, font=font_tiny, fill=SUBTEXT)
        draw.text((PX, y + 20), value, font=font_medium, fill=colour)
        y += 72

    if operator:
        row("OPERATOR", operator, ACCENT)
    if type_desc:
        row("TYPE", type_desc)
    if reg:
        row("REGISTRATION", reg)
    if altitude is not None:
        row("ALTITUDE", f"{altitude:,} ft")
    if range_nm is not None:
        row("RANGE", f"{range_nm:.1f} nm")
    if squawk and squawk not in ("7500", "7600", "7700"):
        row("SQUAWK", squawk)

    # timestamp footer
    ts = datetime.now().strftime("%H:%M:%S")
    draw.text((PX, H - 40), ts, font=font_tiny, fill=SUBTEXT)

    # accent bar along top of data panel
    draw.rectangle([PX - 4, 0, PX - 1, H], fill=ACCENT)

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Cast dispatch
# ---------------------------------------------------------------------------

def _cast(lan_url: str, device_name: str, display_seconds: int, token: str) -> None:
    """Send Cast command to the named Chromecast device (blocking, run in thread)."""
    try:
        import pychromecast
    except ImportError:
        log.error("cast: pychromecast not installed — cannot cast")
        return

    display_url = f"{lan_url.rstrip('/')}/api/cast/display/{token}"
    log.info("cast: discovering %r on LAN", device_name)

    chromecasts, browser = pychromecast.get_listed_chromecasts(
        friendly_names=[device_name], timeout=10
    )
    pychromecast.discovery.stop_discovery(browser)

    if not chromecasts:
        log.warning("cast: device %r not found on LAN", device_name)
        return

    cc = chromecasts[0]
    cc.wait()
    mc = cc.media_controller
    mc.play_media(display_url, "image/jpeg")
    mc.block_until_active(timeout=15)
    log.info("cast: displaying on %r for %d s", device_name, display_seconds)

    time.sleep(display_seconds)
    cc.quit_app()
    log.info("cast: display complete")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def check(aircraft_list: list[dict]) -> None:
    """
    Evaluate cast rules against the current aircraft snapshot list.
    Called from the broadcast loop — must return quickly (does no I/O itself;
    dispatches blocking work to a thread).
    """
    import asyncio
    cfg = _get_config()

    device_name = cfg.get("device_name", "").strip()
    lan_url     = cfg.get("lan_url", "").strip()
    if not device_name or not lan_url:
        log.warning("cast: skipping — device_name or lan_url not configured (cfg=%r)", cfg)
        return

    if not _in_active_hours(cfg):
        log.debug("cast: skipping — outside active hours")
        return

    try:
        cooldown_minutes = int(cfg.get("cooldown_minutes", "30"))
        display_seconds  = int(cfg.get("display_seconds",  "30"))
    except ValueError:
        cooldown_minutes, display_seconds = 30, 30

    rules = _get_rules()
    if not rules:
        log.warning("cast: skipping — no rules in DB")
        return

    log.debug("cast: checking %d aircraft against %d rules", len(aircraft_list), len(rules))
    for ac in aircraft_list:
        icao = ac.get("icao", "")
        if not icao:
            continue
        if _on_cooldown(icao, cooldown_minutes):
            continue
        for rule in rules:
            if _rule_matches(rule, ac):
                _mark_cast(icao)
                token = _store_token(ac)
                log.info(
                    "cast: triggered for %s (%s) by rule %s:%s",
                    icao, ac.get("callsign") or "-",
                    rule["match_type"], rule.get("match_value") or "*",
                )
                # Run blocking Cast I/O in a background thread — don't block the event loop
                try:
                    loop = asyncio.get_running_loop()
                    loop.run_in_executor(
                        None, _cast, lan_url, device_name, display_seconds, token
                    )
                except RuntimeError:
                    # No running loop (e.g. during tests) — skip
                    pass
                break  # one cast per aircraft per check cycle


# ---------------------------------------------------------------------------
# Chromecast discovery (for UI device scan)
# ---------------------------------------------------------------------------

def discover_devices(timeout: int = 8) -> list[str]:
    """Return friendly names of Chromecast devices found on the LAN."""
    try:
        import pychromecast
        chromecasts, browser = pychromecast.get_chromecasts(timeout=timeout)
        pychromecast.discovery.stop_discovery(browser)
        return sorted({cc.cast_info.friendly_name for cc in chromecasts})
    except ImportError:
        log.error("cast: pychromecast not installed")
        return []
    except Exception as exc:
        log.warning("cast: discovery error: %s", exc)
        return []
