import asyncio
import os
import sys
import time
from queue import Queue
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import aircraft_state as state_module
import beast_client as beast_client_module
import debug as debug_module


def test_get_perf_reports_total_and_predecode_timings(monkeypatch):
    state_module.msg_timings.clear()
    state_module.predecode_timings.clear()
    state_module.lock_wait_timings.clear()
    state_module.decode_timings.clear()
    state_module.push_timings.clear()

    state_module.msg_timings.extend([0.001, 0.003])
    state_module.predecode_timings.extend([0.0004, 0.0006])
    state_module.lock_wait_timings.extend([0.0001, 0.0002])
    state_module.decode_timings.extend([0.0005, 0.0022])
    debug_module._benchmark_module.decoder_batch_timings.clear()
    beast_client_module.chunk_timings.clear()
    debug_module._benchmark_module.decoder_batch_timings.extend([
        {
            "queue_wait_ms": 2.0,
            "batch_fill_ms": 0.5,
            "process_wall_ms": 8.0,
            "process_cpu_ms": 3.0,
            "process_offcpu_ms": 5.0,
            "batch_size": 4,
            "batch_target": 32,
        },
        {
            "queue_wait_ms": 4.0,
            "batch_fill_ms": 1.5,
            "process_wall_ms": 12.0,
            "process_cpu_ms": 7.0,
            "process_offcpu_ms": 5.0,
            "batch_size": 8,
            "batch_target": 64,
        },
    ])
    beast_client_module.chunk_timings.extend([
        {
            "chunk_bytes": 1024,
            "frames": 12,
            "parse_wall_ms": 4.0,
            "parse_cpu_ms": 3.0,
            "parse_offcpu_ms": 1.0,
            "parser_mode": "native",
        },
        {
            "chunk_bytes": 2048,
            "frames": 20,
            "parse_wall_ms": 6.0,
            "parse_cpu_ms": 5.0,
            "parse_offcpu_ms": 1.0,
            "parser_mode": "native",
        },
    ])

    fake_main = SimpleNamespace(
        _fm_run_timings=[],
        _msg_drops=7,
        _msg_queue=Queue(),
        _queue_depth_samples=[2, 4, 6],
        _radar_drops=0,
        _radar_loop_timings=[{"msg_queue_depth": 33, "skipped_for_backlog": True}],
        _radar_worker_timings=[
            {
                "queue_wait_ms": 1.0,
                "batch_fill_ms": 0.5,
                "process_wall_ms": 20.0,
                "process_cpu_ms": 15.0,
                "process_offcpu_ms": 5.0,
                "batch_size": 12,
                "batch_target": 32,
            },
            {
                "queue_wait_ms": 3.0,
                "batch_fill_ms": 1.5,
                "process_wall_ms": 40.0,
                "process_cpu_ms": 25.0,
                "process_offcpu_ms": 15.0,
                "batch_size": 24,
                "batch_target": 64,
            },
        ],
        _radar_queue=Queue(),
        _radar_queue_depth_samples=[1, 3],
    )
    monkeypatch.setitem(sys.modules, "main", fake_main)

    payload = asyncio.run(debug_module.get_perf())

    assert payload["msg_decode_us"]["samples"] == 2
    assert payload["msg_decode_us"]["mean"] == 2000.0
    assert payload["predecode_us"]["samples"] == 2
    assert payload["predecode_us"]["mean"] == 500.0
    assert payload["lock_wait_us"]["mean"] == 150.0
    assert payload["pure_decode_us"]["mean"] == 1350.0
    assert payload["radar_loop_ms"]["msg_queue_depth_avg"] == 33.0
    assert payload["radar_loop_ms"]["skipped_for_backlog_count"] == 1
    assert payload["radar_worker_ms"]["samples"] == 2
    assert payload["radar_worker_ms"]["queue_wait_avg"] == 2.0
    assert payload["radar_worker_ms"]["batch_fill_avg"] == 1.0
    assert payload["radar_worker_ms"]["process_wall_avg"] == 30.0
    assert payload["radar_worker_ms"]["process_cpu_avg"] == 20.0
    assert payload["radar_worker_ms"]["process_offcpu_avg"] == 10.0
    assert payload["radar_worker_ms"]["batch_size_avg"] == 18.0
    assert payload["radar_worker_ms"]["batch_target_avg"] == 48.0
    assert payload["decoder_thread_ms"]["samples"] == 2
    assert payload["decoder_thread_ms"]["queue_wait_avg"] == 3.0
    assert payload["decoder_thread_ms"]["batch_fill_avg"] == 1.0
    assert payload["decoder_thread_ms"]["process_wall_avg"] == 10.0
    assert payload["decoder_thread_ms"]["process_cpu_avg"] == 5.0
    assert payload["decoder_thread_ms"]["process_offcpu_avg"] == 5.0
    assert payload["decoder_thread_ms"]["batch_size_avg"] == 6.0
    assert payload["decoder_thread_ms"]["batch_target_avg"] == 48.0
    assert payload["beast_ingest_ms"]["samples"] == 2
    assert payload["beast_ingest_ms"]["chunk_bytes_avg"] == 1536.0
    assert payload["beast_ingest_ms"]["frames_avg"] == 16.0
    assert payload["beast_ingest_ms"]["parse_wall_avg"] == 5.0
    assert payload["beast_ingest_ms"]["parse_cpu_avg"] == 4.0
    assert payload["beast_ingest_ms"]["parse_offcpu_avg"] == 1.0


def test_latency_waveforms_include_total_predecode_and_apply_lanes():
    state_module.msg_perf_trace.clear()
    state_module.push_perf_trace.clear()

    now_s = time.time()
    state_module.msg_perf_trace.append((now_s, 9.0, 4.0, 1.0, 3.0))

    payload = state_module.get_latency_waveforms(window_s=5.0, bin_ms=250)

    assert max(payload["total_decode_ms"]) == 9.0
    assert max(payload["predecode_ms"]) == 4.0
    assert max(payload["lock_wait_ms"]) == 1.0
    assert max(payload["decode_ms"]) == 3.0
