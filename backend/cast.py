"""
Chromecast notifier — rule evaluation, image generation, and Cast dispatch.

Preferences and rules are read from the DB at each trigger call so UI changes
take effect immediately without a restart.

Flow:
  main.py broadcast loop → cast.check(aircraft_snapshot)
    → rule match + time gate + cooldown check
    → if cast idle: dispatch immediately; if busy: store as pending (last-in wins)
    → _cast_worker() loop: generate token, send Cast command, wait, then check pending
    → Chromecast fetches /api/cast/display/<token>
    → /api/cast/display/<token> renders JPEG via Pillow (photo + data overlay + route)
"""

import io
import json
import logging
import threading
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
# Serialized cast queue — one active session + one pending slot (last-in wins)
# A threading.Event signals the worker thread to pick up the pending aircraft.
# ---------------------------------------------------------------------------
_cast_lock      = threading.Lock()
_cast_busy      = False          # True while a _cast() session is running
_cast_pending:  dict | None = None   # aircraft snapshot waiting to be cast
_cast_event     = threading.Event()  # set when a pending item is ready


def _enqueue(aircraft: dict, lan_url: str, device_name: str,
             display_seconds: int) -> None:
    """Schedule aircraft for casting. If idle, start worker; if busy, replace pending."""
    global _cast_busy, _cast_pending

    with _cast_lock:
        _cast_pending = aircraft
        if _cast_busy:
            # Worker is already running; it will pick up _cast_pending when done.
            _cast_event.set()
            return
        _cast_busy = True

    # Start worker thread — it drains the pending slot until empty.
    t = threading.Thread(
        target=_cast_worker,
        args=(lan_url, device_name, display_seconds),
        daemon=True,
        name="cast-worker",
    )
    t.start()


def _cast_worker(lan_url: str, device_name: str, display_seconds: int) -> None:
    """Worker: cast pending aircraft, then loop until no more are queued."""
    global _cast_busy, _cast_pending

    while True:
        with _cast_lock:
            aircraft = _cast_pending
            _cast_pending = None
            _cast_event.clear()

        if aircraft is None:
            with _cast_lock:
                _cast_busy = False
            return

        token = _store_token(aircraft)
        try:
            _cast(lan_url, device_name, display_seconds, token)
        except Exception:
            log.exception("cast: unhandled error in _cast()")

        # After displaying, check if another aircraft queued up during the session.
        with _cast_lock:
            if _cast_pending is None:
                _cast_busy = False
                return
            # There's a pending aircraft — loop to cast it.


# ---------------------------------------------------------------------------
# Config helpers — read from DB each call (TTL-cached)
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


def reset_config_cache() -> None:
    """Force next _get_config() call to re-read from DB. Call after a config save."""
    global _config_cache_ts
    _config_cache_ts = 0.0


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
        return True
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

    match_type  = rule.get("match_type", "")
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
        pass
    else:
        log.warning("cast: unknown match_type %r, skipping rule", match_type)
        return False

    max_nm = rule.get("max_range_nm")
    if max_nm is not None:
        ac_range = ac.get("range_nm")
        if ac_range is None or ac_range > max_nm:
            return False

    return True


# ---------------------------------------------------------------------------
# Airport name lookup (reuses the same airports.json as fleet.py/coverage.py)
# ---------------------------------------------------------------------------

_airport_map: dict[str, str] | None = None


def _airport_name(icao: str) -> str | None:
    global _airport_map
    if _airport_map is None:
        try:
            from coverage import _load_airports
            _airport_map = {
                ap["icao"]: ap["name"]
                for ap in _load_airports()
                if ap.get("icao") and ap.get("name")
            }
        except Exception:
            _airport_map = {}
    return _airport_map.get((icao or "").upper())


def _get_route(icao: str) -> tuple[str | None, str | None, str | None, str | None]:
    """Return (origin_icao, origin_name, dest_icao, dest_name) from the most recent visit."""
    try:
        from db import stats_db
        with stats_db._connect() as conn:
            row = conn.execute(
                "SELECT origin_icao, dest_icao FROM visits "
                "WHERE icao=? AND origin_icao IS NOT NULL AND origin_icao != '' "
                "ORDER BY start_ts DESC LIMIT 1",
                (icao.upper(),),
            ).fetchone()
        if not row:
            return None, None, None, None
        o, d = row["origin_icao"], row["dest_icao"]
        return o, _airport_name(o), d, _airport_name(d)
    except Exception as exc:
        log.debug("cast: route lookup failed for %s: %s", icao, exc)
        return None, None, None, None


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


def _gradient_edge(img, x_start: int, x_end: int, bg: tuple) -> None:
    """Blend columns x_start..x_end toward bg colour (right-to-left fade)."""
    w = x_end - x_start
    h = img.height
    pixels = img.load()
    for x in range(x_start, x_end):
        alpha = (x - x_start) / w
        br, bg_c, bb = bg
        for y in range(h):
            r0, g0, b0 = pixels[x, y]
            pixels[x, y] = (
                int(r0 * (1 - alpha) + br * alpha),
                int(g0 * (1 - alpha) + bg_c * alpha),
                int(b0 * (1 - alpha) + bb * alpha),
            )


def render_display_image(aircraft: dict) -> bytes:
    """
    Generate a 1280×720 JPEG for display on the Chromecast.
    Photo occupies the left 800 px; data overlay the right 480 px.
    Returns raw JPEG bytes.
    """
    from PIL import Image, ImageDraw, ImageFont

    W, H      = 1280, 720
    PHOTO_W   = 800          # photo panel width
    DATA_X    = PHOTO_W + 8  # left edge of data text
    DATA_W    = W - DATA_X   # available text width

    BG       = (11, 12, 16)
    ACCENT   = (56, 139, 253)
    TEXT     = (201, 209, 217)
    SUBTEXT  = (110, 118, 129)
    DIM      = (72, 79, 88)
    WHITE    = (255, 255, 255)
    MILITARY = (188, 140, 255)
    EMERGENCY = (218, 54, 51)

    img  = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    # --- fonts ---
    def _bold(size: int):
        for path in (
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        ):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                pass
        return ImageFont.load_default()

    def _regular(size: int):
        for path in (
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        ):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                pass
        return ImageFont.load_default()

    f_headline = _bold(46)
    f_sub      = _regular(18)
    f_label    = _regular(13)
    f_value    = _bold(22)
    f_route    = _bold(18)
    f_badge    = _bold(13)

    # --- photo panel ---
    icao = aircraft.get("icao", "").upper()
    photo_bytes = _fetch_photo(icao)
    if photo_bytes:
        try:
            photo = Image.open(io.BytesIO(photo_bytes)).convert("RGB")
            # Scale to fill PHOTO_W × H, cropping to centre
            scale = max(PHOTO_W / photo.width, H / photo.height)
            new_w = int(photo.width  * scale)
            new_h = int(photo.height * scale)
            photo = photo.resize((new_w, new_h), Image.LANCZOS)
            cx = (new_w - PHOTO_W) // 2
            cy = (new_h - H)       // 2
            photo = photo.crop((cx, cy, cx + PHOTO_W, cy + H))
            img.paste(photo, (0, 0))
            # Fade right edge of photo into background
            _gradient_edge(img, PHOTO_W - 120, PHOTO_W, BG)
        except Exception as exc:
            log.debug("cast: photo render failed: %s", exc)

    # Accent bar separating photo from data panel
    draw.rectangle([PHOTO_W, 0, PHOTO_W + 3, H], fill=ACCENT)

    # --- data panel ---
    callsign  = aircraft.get("callsign") or ""
    operator  = aircraft.get("operator") or ""
    type_desc = aircraft.get("type_desc") or aircraft.get("type_code") or ""
    altitude  = aircraft.get("altitude")
    squawk    = aircraft.get("squawk") or ""
    range_nm  = aircraft.get("range_nm")
    military  = aircraft.get("military", False)
    reg       = aircraft.get("registration") or ""

    origin_icao, origin_name, dest_icao, dest_name = _get_route(icao)

    y = 32

    # Headline: callsign (large) + ICAO below
    headline = callsign if callsign else icao
    draw.text((DATA_X, y), headline, font=f_headline, fill=WHITE)
    y += 50
    if callsign:
        draw.text((DATA_X, y), icao, font=f_sub, fill=SUBTEXT)
        y += 22
    y += 6

    # Badges (military / emergency) — inline on one row
    badge_x = DATA_X
    if military:
        bw = 90
        draw.rectangle([badge_x, y, badge_x + bw, y + 22], fill=MILITARY)
        draw.text((badge_x + 5, y + 4), "MILITARY", font=f_badge, fill=BG)
        badge_x += bw + 6
    if squawk in ("7500", "7600", "7700"):
        labels = {"7500": "HIJACK", "7600": "RADIO FAIL", "7700": "EMERGENCY"}
        label  = labels[squawk]
        bw     = 8 + len(label) * 7
        draw.rectangle([badge_x, y, badge_x + bw, y + 22], fill=EMERGENCY)
        draw.text((badge_x + 5, y + 4), label, font=f_badge, fill=WHITE)
        badge_x += bw + 6
    if military or squawk in ("7500", "7600", "7700"):
        y += 30

    y += 4

    # Helper: draw a label + value row
    def row(label: str, value: str, colour=TEXT, gap_after: int = 28) -> None:
        nonlocal y
        draw.text((DATA_X, y), label, font=f_label, fill=DIM)
        y += 14
        draw.text((DATA_X, y), value, font=f_value, fill=colour)
        y += gap_after

    # Helper to truncate text to DATA_W pixels
    def trunc(text: str, font) -> str:
        if not text:
            return text
        while text and draw.textlength(text, font=font) > DATA_W - 4:
            text = text[:-1]
        return text

    if operator:
        draw.text((DATA_X, y), "OPERATOR", font=f_label, fill=DIM)
        y += 14
        draw.text((DATA_X, y), trunc(operator, f_value), font=f_value, fill=ACCENT)
        y += 28

    if type_desc:
        draw.text((DATA_X, y), "TYPE", font=f_label, fill=DIM)
        y += 14
        draw.text((DATA_X, y), trunc(type_desc, f_value), font=f_value, fill=TEXT)
        y += 28

    if reg:
        draw.text((DATA_X, y), "REGISTRATION", font=f_label, fill=DIM)
        y += 14
        draw.text((DATA_X, y), reg, font=f_value, fill=TEXT)
        y += 28

    # Route — origin → dest with airport names below codes
    if origin_icao or dest_icao:
        draw.text((DATA_X, y), "ROUTE", font=f_label, fill=DIM)
        y += 14
        o_str = origin_icao or "?"
        d_str = dest_icao   or "?"
        draw.text((DATA_X, y), f"{o_str}  →  {d_str}", font=f_route, fill=ACCENT)
        y += 20
        o_name = trunc(origin_name or "", f_label) if origin_name else ""
        d_name = trunc(dest_name   or "", f_label) if dest_name   else ""
        if o_name or d_name:
            sep = "  →  " if o_name and d_name else ""
            draw.text((DATA_X, y), f"{o_name}{sep}{d_name}", font=f_label, fill=SUBTEXT)
            y += 16
        y += 8

    if altitude is not None:
        draw.text((DATA_X, y), "ALTITUDE", font=f_label, fill=DIM)
        y += 14
        draw.text((DATA_X, y), f"{altitude:,} ft", font=f_value, fill=TEXT)
        y += 28

    if range_nm is not None:
        draw.text((DATA_X, y), "RANGE", font=f_label, fill=DIM)
        y += 14
        draw.text((DATA_X, y), f"{range_nm:.1f} nm", font=f_value, fill=TEXT)
        y += 28

    if squawk and squawk not in ("7500", "7600", "7700"):
        draw.text((DATA_X, y), "SQUAWK", font=f_label, fill=DIM)
        y += 14
        draw.text((DATA_X, y), squawk, font=f_value, fill=TEXT)
        y += 28

    # Timestamp footer
    ts = datetime.now().strftime("%H:%M:%S")
    draw.text((DATA_X, H - 24), ts, font=f_label, fill=DIM)

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=88)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Cast dispatch
# ---------------------------------------------------------------------------

def _cast(lan_url: str, device_name: str, display_seconds: int, token: str) -> None:
    """Send Cast command to the named Chromecast device (blocking, runs in worker thread)."""
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

    if not chromecasts:
        pychromecast.discovery.stop_discovery(browser)
        log.warning("cast: device %r not found on LAN", device_name)
        return

    cc = chromecasts[0]
    cc.wait()  # resolve service info — zeroconf must still be running
    pychromecast.discovery.stop_discovery(browser)

    mc = cc.media_controller
    log.info("cast: sending to %r — %s", device_name, display_url)
    mc.play_media(display_url, "image/jpeg", stream_type="NONE")
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
    Called from the broadcast loop — returns immediately; all blocking work
    is handled by the worker thread via _enqueue().
    """
    cfg = _get_config()

    device_name = cfg.get("device_name", "").strip()
    lan_url     = cfg.get("lan_url", "").strip()
    if not device_name or not lan_url:
        log.debug("cast: skipping — device_name or lan_url not configured")
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
        log.debug("cast: skipping — no rules in DB")
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
                log.info(
                    "cast: triggered for %s (%s) by rule %s:%s",
                    icao, ac.get("callsign") or "-",
                    rule["match_type"], rule.get("match_value") or "*",
                )
                _enqueue(ac, lan_url, device_name, display_seconds)
                break  # one trigger per aircraft per cycle


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
