import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from signal_utils import average_raw_signals_to_dbfs, dbfs_to_raw_signal, raw_signal_to_dbfs


def test_raw_signal_to_dbfs_matches_readsb_examples():
    assert raw_signal_to_dbfs(255) == 0.0
    assert raw_signal_to_dbfs(180) == -3.0
    assert raw_signal_to_dbfs(128) == -6.0
    assert raw_signal_to_dbfs(45) == -15.1
    assert raw_signal_to_dbfs(14) == -25.2


def test_dbfs_to_raw_signal_matches_readsb_inverse():
    assert dbfs_to_raw_signal(0.0) == 255
    assert abs(dbfs_to_raw_signal(-3.0) - 180) <= 1
    assert abs(dbfs_to_raw_signal(-6.0) - 128) <= 1
    assert abs(dbfs_to_raw_signal(-33.7) - 5) <= 1


def test_average_raw_signals_to_dbfs_uses_readsb_aircraft_window():
    averaged = average_raw_signals_to_dbfs([5] * 8)

    assert math.isclose(averaged, -34.1)
