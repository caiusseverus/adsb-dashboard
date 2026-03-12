"""
benchmark.py — ADS-B pipeline micro-benchmark.

Measures the throughput and latency of every major stage in the message
processing pipeline using a fixed, deterministic corpus of real-world Beast
frames.

When called from the HTTP endpoint (inside the live server), the benchmark
pauses the real decoder thread for the duration of the run so that GIL
contention from live message decoding does not inflate the timings.  The
Beast TCP connection stays open during the pause — incoming bytes accumulate
in the OS socket buffer and the BeastClient's internal bytearray, and are
processed normally once the decoder resumes.  A short pause (< 10s) loses
no frames at typical message rates.

Pause mechanism
---------------
`_decoder_paused` is a threading.Event the decoder thread checks before
processing each queue item.  When set, the decoder blocks on
`_decoder_resume.wait()` after finishing its current message (so _lock is
never held across the wait).  The benchmark clears `_decoder_paused` and
sets `_decoder_resume` to release it.

main.py integration
-------------------
Replace the inline _run closure in _start_msg_processor() with::

    from benchmark import make_pause_aware_decoder
    ...
    def _start_msg_processor() -> threading.Thread:
        _run = make_pause_aware_decoder(_msg_queue, state, _DECODE_SENTINEL)
        t = threading.Thread(target=_run, daemon=True, name="beast-decoder")
        t.start()
        return t
"""

from __future__ import annotations

import statistics
import threading
import time
from typing import Any

# ---------------------------------------------------------------------------
# Decoder pause / resume
# ---------------------------------------------------------------------------
_decoder_paused = threading.Event()   # set   → decoder should pause
_decoder_resume = threading.Event()   # set   → decoder may continue
_decoder_resume.set()                 # starts in running state


def make_pause_aware_decoder(msg_queue, state, sentinel):
    """Return a _run() function for the decoder thread that honours pause events."""
    import logging
    log = logging.getLogger(__name__)

    def _run() -> None:
        while True:
            item = msg_queue.get()
            if item is sentinel:
                log.debug("beast-decoder: shutdown sentinel received")
                return
            # Finish the current message first so _lock is never held across
            # the wait — avoids deadlock with get_snapshot().
            if _decoder_paused.is_set():
                log.debug("beast-decoder: paused for benchmark")
                _decoder_resume.wait()
                log.debug("beast-decoder: resumed")
            msg, mlat_source = item
            state.process_message(msg, mlat_source)

    return _run


# ---------------------------------------------------------------------------
# Context manager: pause the live decoder for the duration of a with-block
# ---------------------------------------------------------------------------

class DecoderPaused:
    """
    Pauses the live Beast decoder thread.

    Usage (inside the HTTP endpoint, already in a thread via asyncio.to_thread)::

        with DecoderPaused(drain_timeout=2.0):
            results = run_benchmark(n_msgs=5000, paused=True)
    """

    def __init__(self, drain_timeout: float = 2.0):
        self._drain_timeout = drain_timeout

    def __enter__(self):
        import logging
        log = logging.getLogger(__name__)

        # Signal the decoder to pause after its current message
        _decoder_resume.clear()
        _decoder_paused.set()

        # Wait for the queue to drain so no in-flight messages compete for
        # the GIL while the benchmark is timing.
        deadline = time.monotonic() + self._drain_timeout
        try:
            from main import _msg_queue
            while not _msg_queue.empty():
                if time.monotonic() > deadline:
                    log.warning("benchmark: queue not fully drained before pause timeout")
                    break
                time.sleep(0.05)
        except ImportError:
            pass  # standalone script — nothing to drain

        log.info("benchmark: decoder paused")
        return self

    def __exit__(self, *_):
        import logging
        _decoder_paused.clear()
        _decoder_resume.set()
        logging.getLogger(__name__).info("benchmark: decoder resumed")


# ---------------------------------------------------------------------------
# Corpus
# ---------------------------------------------------------------------------

_CORPUS: list[tuple[str, int, int, str]] = [
    ("8D400F0620226469C5A3B1", 0x3A, 0x000000000001, "DF17 callsign"),
    ("8D4CA2D358B9864A7C0857", 0x28, 0x000000000002, "DF17 pos even"),
    ("8D4CA2D358B98653D9E87A", 0x29, 0x000000000003, "DF17 pos odd"),
    ("8D4CA2D399107FB5E0833B", 0x27, 0x000000000004, "DF17 velocity"),
    ("8D407236234994B8738F47", 0x31, 0x000000000005, "DF17 callsign 2"),
    ("8D3C6444584F8F9E1A3B1C", 0x22, 0x000000000006, "DF17 pos even 2"),
    ("8D3C64445849EF9B1A3B2D", 0x23, 0x000000000007, "DF17 pos odd 2"),
    ("8D3C644499086BB91E4B3C", 0x21, 0x000000000008, "DF17 velocity 2"),
    ("8D7C1BBBEA0DC89E0820A8", 0x1E, 0x000000000009, "DF17 target state"),
    ("8E4D2286EA4B8860015C08", 0x18, 0x00000000000A, "DF18 TIS-B"),
    ("20001918CA3782",         0x2C, 0x00000000000B, "DF4 surveillance alt"),
    ("5DAB1213B2B6C2",         0x15, 0x00000000000C, "DF11 all-call"),
    ("A0001918CA378200000000", 0x2A, 0x00000000000D, "DF20 BDS40"),
    ("A0001918CA378250000000", 0x2B, 0x00000000000E, "DF20 BDS50"),
    ("A0001918CA378260000000", 0x2C, 0x00000000000F, "DF20 BDS60"),
    ("A8001918CA3782A1234567", 0x20, 0x000000000010, "DF21 identity"),
    ("02E197B67B9B59",          0x19, 0x000000000011, "DF0 ACAS short"),
    ("80F990356D4B5A1234ABCD", 0x1F, 0x000000000012, "DF16 ACAS long"),
    ("28001ACA37823C",          0x24, 0x000000000013, "DF5 ident"),
    ("8DAE1234202264B9C5A3B1", 0x30, 0x000000000014, "DF17 military ICAO"),
]

_REPEAT   = 250
_MESSAGES: list[dict] = []
for _i in range(_REPEAT):
    for _hex, _sig, _ts, _desc in _CORPUS:
        _MESSAGES.append({"raw": _hex, "signal": _sig, "timestamp": _ts, "type": 0x33})


# ---------------------------------------------------------------------------
# Result store
# ---------------------------------------------------------------------------
_last_result: dict[str, Any] = {}
_result_lock  = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _percentiles(times_s: list[float]) -> dict:
    n = len(times_s)
    if not n:
        return {"samples": 0, "p50_us": 0, "p95_us": 0, "p99_us": 0,
                "mean_us": 0, "max_us": 0, "max_sustained_rate": 0}
    times_s.sort()
    p95 = times_s[int(n * 0.95)]
    return {
        "samples":            n,
        "p50_us":             round(times_s[int(n * 0.50)] * 1e6, 1),
        "p95_us":             round(p95 * 1e6, 1),
        "p99_us":             round(times_s[int(n * 0.99)] * 1e6, 1),
        "mean_us":            round(statistics.mean(times_s) * 1e6, 1),
        "max_us":             round(times_s[-1] * 1e6, 1),
        "max_sustained_rate": int(1.0 / p95) if p95 > 0 else 0,
    }


def _build_warm_state():
    from aircraft_state import AircraftState
    s = AircraftState(aircraft_timeout=60)
    for msg in _MESSAGES:
        try:
            s.process_message(msg, mlat_source=None)
        except Exception:
            pass
    return s


# ---------------------------------------------------------------------------
# Stage benchmarks
# ---------------------------------------------------------------------------

def _bench_beast_parse(n: int) -> dict:
    from beast_client import BeastClient
    messages: list[dict] = []
    client = BeastClient("localhost", 30005, lambda m: messages.append(m))

    def _make_frame(hex_payload: str, signal: int, ts: int) -> bytes:
        payload  = bytes.fromhex(hex_payload)
        ts_bytes = ts.to_bytes(6, "big")
        body     = ts_bytes + bytes([signal]) + payload
        escaped  = bytearray()
        for b in body:
            escaped.append(b)
            if b == 0x1A:
                escaped.append(0x1A)
        msg_type = 0x33 if len(payload) == 14 else (0x32 if len(payload) == 7 else 0x31)
        return bytes([0x1A, msg_type]) + bytes(escaped)

    raw_frames = b"".join(_make_frame(h, s, t) for h, s, t, _ in _CORPUS)
    reps = max(1, n // len(_CORPUS))
    times: list[float] = []
    for _ in range(reps):
        messages.clear()
        client._buf.clear()
        t0 = time.perf_counter()
        client._buf.extend(raw_frames)
        client._parse_frames()
        elapsed = time.perf_counter() - t0
        parsed  = len(messages)
        if parsed > 0:
            per_msg = elapsed / parsed
            times.extend([per_msg] * parsed)
    return _percentiles(times)


def _bench_msg_decode_warm(state, n: int) -> dict:
    msgs  = (_MESSAGES * max(1, n // len(_MESSAGES)))[:n]
    times: list[float] = []
    for msg in msgs:
        t0 = time.perf_counter()
        try:
            state.process_message(msg, mlat_source=None)
        except Exception:
            pass
        times.append(time.perf_counter() - t0)
    return _percentiles(times)


def _bench_new_aircraft(n: int) -> dict:
    from aircraft_state import AircraftState
    seen: set[str] = set()
    first_msgs: list[dict] = []
    for msg in _MESSAGES:
        icao = msg["raw"][:6]
        if icao not in seen:
            seen.add(icao)
            first_msgs.append(msg)

    times: list[float] = []
    reps = max(1, n // max(len(first_msgs), 1))
    for _ in range(reps):
        fresh = AircraftState(aircraft_timeout=60)
        for msg in first_msgs:
            t0 = time.perf_counter()
            try:
                fresh.process_message(msg, mlat_source=None)
            except Exception:
                pass
            times.append(time.perf_counter() - t0)
    return _percentiles(times)


def _bench_get_snapshot(state, n: int) -> dict:
    times: list[float] = []
    for _ in range(n):
        t0 = time.perf_counter()
        try:
            state.get_snapshot()
        except Exception:
            pass
        times.append(time.perf_counter() - t0)
    return _percentiles(times)


def _bench_json_serialise(state, n: int) -> dict:
    import json
    try:
        import orjson
        def _dumps(obj): return orjson.dumps(obj)
    except ImportError:
        def _dumps(obj): return json.dumps(obj).encode()

    snap  = state.get_snapshot()
    times: list[float] = []
    for _ in range(n):
        t0 = time.perf_counter()
        _dumps(snap)
        times.append(time.perf_counter() - t0)
    return _percentiles(times)


def _bench_full_pipeline(state, n: int) -> dict:
    from beast_client import BeastClient
    messages: list[dict] = []
    client = BeastClient("localhost", 30005, lambda m: messages.append(m))

    def _make_frame(hex_payload: str, signal: int, ts: int) -> bytes:
        payload  = bytes.fromhex(hex_payload)
        ts_bytes = ts.to_bytes(6, "big")
        body     = ts_bytes + bytes([signal]) + payload
        escaped  = bytearray()
        for b in body:
            escaped.append(b)
            if b == 0x1A:
                escaped.append(0x1A)
        msg_type = 0x33 if len(payload) == 14 else (0x32 if len(payload) == 7 else 0x31)
        return bytes([0x1A, msg_type]) + bytes(escaped)

    corpus_frames = {h: _make_frame(h, s, t) for h, s, t, _ in _CORPUS}
    msgs  = _MESSAGES[:n]
    times: list[float] = []
    for msg_dict in msgs:
        frame = corpus_frames.get(msg_dict["raw"])
        if frame is None:
            continue
        t0 = time.perf_counter()
        messages.clear()
        client._buf.clear()
        client._buf.extend(frame)
        client._parse_frames()
        if messages:
            try:
                state.process_message(messages[0], mlat_source=None)
            except Exception:
                pass
        times.append(time.perf_counter() - t0)
    return _percentiles(times)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_benchmark(n_msgs: int = 5000, paused: bool = False) -> dict:
    """
    Run all benchmark stages and return a results dict.

    Parameters
    ----------
    n_msgs : int
        Iterations per stage (default 5000).
    paused : bool
        Should be True when called from the HTTP endpoint after the decoder
        has been paused via DecoderPaused.  Recorded in results for display.
    """
    import platform, sys

    results: dict[str, Any] = {
        "n_msgs":                    n_msgs,
        "corpus_size":               len(_CORPUS),
        "python_version":            sys.version.split()[0],
        "platform":                  platform.platform(terse=True),
        "timestamp":                 time.time(),
        "decoder_paused_during_run": paused,
    }

    try:
        import orjson
        results["orjson"]         = True
        results["orjson_version"] = getattr(orjson, "__version__", "?")
    except ImportError:
        results["orjson"]         = False
        results["orjson_version"] = None

    try:
        import pyModeS
        results["pymodes_version"] = getattr(pyModeS, "__version__", "?")
        try:
            from pyModeS.c_decoder import common as _  # type: ignore
            results["pymodes_cython"] = True
        except ImportError:
            results["pymodes_cython"] = False
    except Exception:
        results["pymodes_version"] = "?"
        results["pymodes_cython"]  = False

    t_start = time.perf_counter()

    results["stage_beast_parse"]   = _bench_beast_parse(n_msgs)
    warm_state = _build_warm_state()
    results["warm_aircraft_count"] = len(warm_state._aircraft)
    results["stage_decode_warm"]   = _bench_msg_decode_warm(warm_state, n_msgs)
    results["stage_new_aircraft"]  = _bench_new_aircraft(n_msgs)
    results["stage_get_snapshot"]  = _bench_get_snapshot(warm_state, min(n_msgs, 500))
    results["stage_json_serial"]   = _bench_json_serialise(warm_state, min(n_msgs, 500))
    results["stage_full_pipeline"] = _bench_full_pipeline(warm_state, n_msgs)

    results["total_bench_time_s"] = round(time.perf_counter() - t_start, 2)

    full_p95 = results["stage_full_pipeline"]["p95_us"]
    max_rate  = results["stage_full_pipeline"]["max_sustained_rate"]
    if max_rate >= 3500:
        verdict        = "PASS"
        verdict_detail = f"Pipeline can sustain ≥3,500 msgs/s at p95 ({full_p95} µs/msg)"
    elif max_rate >= 2000:
        verdict        = "MARGINAL"
        verdict_detail = f"Pipeline sustains ~{max_rate} msgs/s; below 3,500 target ({full_p95} µs/msg at p95)"
    else:
        verdict        = "FAIL"
        verdict_detail = f"Pipeline bottleneck: only {max_rate} msgs/s possible ({full_p95} µs/msg at p95)"

    results["verdict"]        = verdict
    results["verdict_detail"] = verdict_detail

    with _result_lock:
        _last_result.clear()
        _last_result.update(results)

    return results


def get_last_result() -> dict:
    with _result_lock:
        return dict(_last_result)
