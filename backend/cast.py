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
# Live snapshot — updated every broadcast cycle so the display worker can
# check whether a triggered aircraft is still visible.
# Assignment is GIL-atomic; we copy the list to avoid mid-iteration replacement.
# ---------------------------------------------------------------------------
_current_snapshot: list[dict] = []


# ---------------------------------------------------------------------------
# Cooldown — prevent re-notifying the same ICAO within the configured window
# ---------------------------------------------------------------------------
_cooldown: dict[str, float] = {}  # icao → last_cast_ts


def _on_cooldown(icao: str, cooldown_minutes: int) -> bool:
    last = _cooldown.get(icao)
    if last is None:
        return False
    if (time.time() - last) >= (cooldown_minutes * 60):
        del _cooldown[icao]   # prune expired entry so dict stays bounded
        return False
    return True


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
        icao  = aircraft.get("icao", "").upper()
        try:
            _cast(lan_url, device_name, display_seconds, token, icao)
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

    max_alt = rule.get("max_altitude_ft")
    if max_alt is not None:
        ac_alt = ac.get("altitude")
        if ac_alt is None or ac_alt > max_alt:
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


def render_display_image(aircraft: dict) -> bytes:
    """
    Generate a 1280×720 JPEG for display on the Chromecast.
    Full-bleed photo with dark gradient bands at top and bottom.
    Top band: registration, aircraft type, operator.
    Bottom band: callsign, route (if known).
    Returns raw JPEG bytes.
    """
    from PIL import Image, ImageDraw, ImageFont

    W, H = 1280, 720
    PAD  = 32

    BG        = (11, 12, 16)
    ACCENT    = (56, 139, 253)
    TEXT      = (201, 209, 217)
    SUBTEXT   = (110, 118, 129)
    WHITE     = (255, 255, 255)
    MILITARY  = (188, 140, 255)
    EMERGENCY = (218, 54, 51)

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

    f_reg      = _bold(48)
    f_type_sm  = _regular(40)
    f_op       = _regular(32)
    f_call     = _bold(52)
    f_route    = _bold(28)
    f_rname    = _bold(44)
    f_badge    = _bold(13)
    f_ts       = _regular(13)
    f_sightings = _bold(18)

    # --- base: dark BG, then full-bleed photo ---
    img = Image.new("RGBA", (W, H), (*BG, 255))

    icao = aircraft.get("icao", "").upper()
    photo_bytes = _fetch_photo(icao)
    if photo_bytes:
        try:
            photo = Image.open(io.BytesIO(photo_bytes)).convert("RGBA")
            scale = max(W / photo.width, H / photo.height)
            new_w = int(photo.width  * scale)
            new_h = int(photo.height * scale)
            photo = photo.resize((new_w, new_h), Image.LANCZOS)
            cx = (new_w - W) // 2
            cy = (new_h - H) // 2
            photo = photo.crop((cx, cy, cx + W, cy + H))
            img.paste(photo, (0, 0))
        except Exception as exc:
            log.debug("cast: photo render failed: %s", exc)

    # --- gradient bands: top and bottom darken toward BG ---
    BAND = 210
    overlay = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw_ov = ImageDraw.Draw(overlay)
    for i in range(BAND):
        top_alpha = int(230 * (1 - i / BAND))
        draw_ov.line([(0, i), (W - 1, i)], fill=(*BG, top_alpha))
        bot_alpha = int(230 * i / BAND)
        draw_ov.line([(0, H - BAND + i), (W - 1, H - BAND + i)], fill=(*BG, bot_alpha))
    img = Image.alpha_composite(img, overlay)

    draw = ImageDraw.Draw(img)

    def trunc(text: str, font, max_w: int) -> str:
        while text and draw.textlength(text, font=font) > max_w:
            text = text[:-1]
        return text

    max_w = W - 2 * PAD

    # --- top block ---
    reg       = aircraft.get("registration") or ""
    type_name = (aircraft.get("type_full_name") or aircraft.get("type_desc")
                 or aircraft.get("type_code") or "")
    operator  = aircraft.get("operator") or ""

    y = PAD

    # Line 1: registration + type name on the same line
    if reg:
        draw.text((PAD, y), reg, font=f_reg, fill=WHITE)
        reg_w = int(draw.textlength(reg, font=f_reg))
        if type_name:
            type_text = trunc(type_name, f_type_sm, max_w - reg_w - 20)
            # vertically center the slightly smaller type text alongside reg
            draw.text((PAD + reg_w + 16, y + 4), type_text, font=f_type_sm, fill=TEXT)
        y += 56
    elif type_name:
        draw.text((PAD, y), trunc(type_name, f_type_sm, max_w), font=f_type_sm, fill=TEXT)
        y += 32

    # Line 2: operator — no pill, plain text below reg/type
    if operator:
        op_text = trunc(operator, f_op, max_w)
        draw.text((PAD, y), op_text, font=f_op, fill=TEXT)
        y += 40

    # Sightings count — top right corner
    sighting_count = aircraft.get("sighting_count")
    if sighting_count:
        sc_label = f"{sighting_count} sighting{'s' if sighting_count != 1 else ''}"
        sc_w = int(draw.textlength(sc_label, font=f_sightings))
        draw.text((W - PAD - sc_w, PAD + 4), sc_label, font=f_sightings, fill=SUBTEXT)

    # --- bottom block (built upward from the bottom edge) ---
    callsign = aircraft.get("callsign") or icao
    military = aircraft.get("military", False)
    squawk   = aircraft.get("squawk") or ""
    origin_icao, origin_name, dest_icao, dest_name = _get_route(icao)

    y = H - PAD

    # Timestamp — bottom right (absolute position)
    ts   = datetime.now().strftime("%H:%M:%S")
    ts_w = int(draw.textlength(ts, font=f_ts))
    draw.text((W - PAD - ts_w, H - PAD - 14), ts, font=f_ts, fill=SUBTEXT)

    # Right-side data lines stacked upward: range/bearing (bottom), altitude (above)
    range_nm  = aircraft.get("range_nm")
    bearing   = aircraft.get("bearing_deg")
    altitude  = aircraft.get("altitude")
    right_items = []
    if range_nm is not None and bearing is not None:
        right_items.append(f"{range_nm:.1f} nm  {bearing:.0f}°")
    if altitude is not None:
        right_items.append(f"{altitude:,} ft")  # altitude sits above range/bearing
    for i, line in enumerate(right_items):
        lw = int(draw.textlength(line, font=f_route))
        draw.text((W - PAD - lw, H - PAD - 48 - i * 36), line, font=f_route, fill=TEXT)

    # Route: airport names only
    if origin_icao or dest_icao:
        if origin_name and dest_name:
            name_line = f"{origin_name}  →  {dest_name}"
        else:
            name_line = origin_name or dest_name or ""
        if name_line:
            draw.text((PAD, y - 50), trunc(name_line, f_rname, max_w), font=f_rname, fill=TEXT)
            y -= 58

    y -= 10  # gap above callsign

    # Callsign
    draw.text((PAD, y - 58), callsign, font=f_call, fill=WHITE)
    y -= 66

    # Badges (above callsign)
    badge_x = PAD
    if military:
        bw = 90
        draw.rectangle([badge_x, y - 22, badge_x + bw, y], fill=MILITARY)
        draw.text((badge_x + 5, y - 18), "MILITARY", font=f_badge, fill=BG)
        badge_x += bw + 6
    if squawk in ("7500", "7600", "7700"):
        labels = {"7500": "HIJACK", "7600": "RADIO FAIL", "7700": "EMERGENCY"}
        label  = labels[squawk]
        bw     = 8 + len(label) * 7
        draw.rectangle([badge_x, y - 22, badge_x + bw, y], fill=EMERGENCY)
        draw.text((badge_x + 5, y - 18), label, font=f_badge, fill=WHITE)

    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=88)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Cast dispatch
# ---------------------------------------------------------------------------

_DISPLAY_POLL_S = 5    # check interval while displaying
_DISPLAY_MAX_S  = 900  # hard cap (15 min) so a forgotten cast doesn't run forever


def _cast(lan_url: str, device_name: str, display_seconds: int,
          token: str, icao: str) -> None:
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
    log.info("cast: displaying %s on %r (min %ds, cap %ds)",
             icao, device_name, display_seconds, _DISPLAY_MAX_S)

    # Stay on screen while the aircraft is still visible.
    # Yield early only when another aircraft is pending AND the minimum
    # display time has elapsed — so multiple triggers cycle fairly.
    elapsed = 0
    while elapsed < _DISPLAY_MAX_S:
        time.sleep(_DISPLAY_POLL_S)
        elapsed += _DISPLAY_POLL_S

        with _cast_lock:
            has_pending = _cast_pending is not None
        if has_pending and elapsed >= display_seconds:
            log.info("cast: %s — pending aircraft waiting, yielding after %ds", icao, elapsed)
            break

        snapshot = _current_snapshot  # atomic read of current reference
        if not any(ac.get("icao", "").upper() == icao for ac in snapshot):
            log.info("cast: %s left snapshot after %ds — ending display", icao, elapsed)
            break

    cc.quit_app()
    log.info("cast: display ended for %s after %ds", icao, elapsed)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def check(aircraft_list: list[dict]) -> None:
    """
    Evaluate cast rules against the current aircraft snapshot list.
    Called from the broadcast loop — returns immediately; all blocking work
    is handled by the worker thread via _enqueue().
    """
    global _current_snapshot
    _current_snapshot = list(aircraft_list)  # copy so worker sees a stable list

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
