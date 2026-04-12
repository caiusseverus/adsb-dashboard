import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from benchmark import _target_decode_batch_size


def test_target_decode_batch_size_stays_at_base_without_backlog():
    assert _target_decode_batch_size(0, base=16, max_size=128, backlog_threshold=64) == 16
    assert _target_decode_batch_size(63, base=16, max_size=128, backlog_threshold=64) == 16


def test_target_decode_batch_size_grows_with_backlog_and_caps():
    assert _target_decode_batch_size(64, base=16, max_size=128, backlog_threshold=64) == 65
    assert _target_decode_batch_size(5000, base=16, max_size=128, backlog_threshold=64) == 128
