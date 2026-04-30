# Radar Sync Baseline Capture (Stage 0)

This folder stores Stage 0 baseline captures for radar sync remediation comparison.

## Required scenarios

1. Steady single-radar traffic: capture at least 15 minutes.
2. Residual-slope drift case: capture at least 10 minutes and include at least one correction event.
3. Holdover episode: include holdover entry and recovery.
4. Anchor churn episode: include at least one reference-aircraft swap.
5. Recorded vs recomputed chart mode screenshot pair for the same IID/time window.

If a scenario is rare, capture it opportunistically or induce it using an existing debug switch if one already exists. Do not add new perturbation/debug logic in Stage 0.

## Capture utility

Use `tools/radar_sync_capture.py`.

Example:

```bash
python tools/radar_sync_capture.py --iid 7 --duration-s 900 --interval-s 1.0 --label steady_single_radar
```

Default output directory is this folder (`tasks/radar_sync_baseline`).
