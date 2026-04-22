"""
radar/localiser.py — Hyperbolic TDOA solver for radar position estimation.

Uses scipy.optimize.least_squares (Levenberg-Marquardt) to solve the
hyperbolic constraint set from co-sweep ADS-B calibration pairs.
"""

from __future__ import annotations

import logging
import math
import random
from collections import defaultdict, deque
from typing import Optional

from .models import CalibrationPair

log = logging.getLogger(__name__)

# Earth radius in metres
_R_EARTH = 6_371_000.0
# Speed of radio propagation (m/µs)
_C_MUS = 299.792458  # m/µs

# Minimum pairs for solver to attempt
MIN_PAIRS = 10
MIN_SWEEP_ESTIMATES = 3
MIN_SWEEP_AIRCRAFT = 4
# Minimum azimuth spread (degrees) across calibration aircraft
MIN_AZ_SPREAD_DEG = 30.0
MAX_COINCIDENT_TDOA_US = 500.0
MIN_COINCIDENT_PAIRS = 4
MIN_COINCIDENT_INTERSECTIONS = 3
MIN_COINCIDENT_AZ_SPREAD_DEG = 20.0
MAX_COINCIDENT_CLUSTER_RADIUS_M = 25_000.0
MIN_PAIR_FAMILY_REPEATS = 3
MAX_PAIR_FAMILY_WINDOW_S = 180.0
MAX_PAIR_FAMILY_SPREAD_US = 800.0
SWEEP_GROUP_WINDOW_S = 0.25
MAX_SOLVER_RADIUS_M = 400_000.0
MIN_SOLVER_RADIUS_M = 75_000.0
MAX_SOLVER_CEP_M = 25_000.0
MAX_SOLVER_RMS_US = 15.0


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * _R_EARTH * math.asin(math.sqrt(a))


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Forward azimuth from point 1 to point 2, in degrees [0, 360)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlam = math.radians(lon2 - lon1)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlam)
    y = math.sin(dlam) * math.cos(phi2)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def _latlon_to_xy(lat: float, lon: float, origin_lat: float, origin_lon: float) -> tuple[float, float]:
    """Convert lat/lon to local ENU metres relative to an origin."""
    dlat = math.radians(lat - origin_lat)
    dlon = math.radians(lon - origin_lon)
    x = _R_EARTH * dlon * math.cos(math.radians(origin_lat))
    y = _R_EARTH * dlat
    return x, y


def _xy_to_latlon(x: float, y: float, origin_lat: float, origin_lon: float) -> tuple[float, float]:
    """Convert local ENU metres back to lat/lon."""
    lat = origin_lat + math.degrees(y / _R_EARTH)
    lon = origin_lon + math.degrees(x / (_R_EARTH * math.cos(math.radians(origin_lat))))
    return lat, lon


class RadarLocaliser:
    """Hyperbolic TDOA solver for one IID."""

    def _classify_sweep_failure(self, exc: Exception) -> str:
        message = str(exc)
        if "Insufficient sweep aircraft" in message:
            return "too_few_aircraft"
        if "anchor pair connectivity" in message:
            return "insufficient_connected_component"
        if "Poor sweep geometry" in message:
            return "poor_geometry"
        if "search boundary" in message:
            return "search_boundary"
        if "residual too large" in message:
            return "residual_too_large"
        if "CEP too large" in message:
            return "cep_too_large"
        if "did not converge" in message:
            return "did_not_converge"
        return "other"

    def filter_coincident_pairs(self, pairs: list[CalibrationPair]) -> list[CalibrationPair]:
        """Return near-coincident co-sweep pairs as a higher-grade subset."""
        return [pair for pair in pairs if abs(pair.tdoa_us) <= MAX_COINCIDENT_TDOA_US]

    def filter_consistent_pairs(self, pairs: list[CalibrationPair]) -> list[CalibrationPair]:
        """Keep only pair families with a locally stable repeated timing signature.

        Repeated observations for the same aircraft pair need not be constant over
        long periods because geometry changes, but they should form short runs of
        closely agreeing TDOA values if the co-sweep association is sound.
        """
        from collections import defaultdict

        grouped: dict[tuple[str, str], list[CalibrationPair]] = defaultdict(list)
        for pair in pairs:
            grouped[(pair.icao_a, pair.icao_b)].append(pair)

        kept_ids: set[int] = set()
        for family_pairs in grouped.values():
            if len(family_pairs) < MIN_PAIR_FAMILY_REPEATS:
                continue

            ordered = sorted(family_pairs, key=lambda pair: pair.ts)
            for idx in range(len(ordered) - MIN_PAIR_FAMILY_REPEATS + 1):
                window = ordered[idx: idx + MIN_PAIR_FAMILY_REPEATS]
                if (window[-1].ts - window[0].ts) > MAX_PAIR_FAMILY_WINDOW_S:
                    continue
                spread_us = max(pair.tdoa_us for pair in window) - min(pair.tdoa_us for pair in window)
                if spread_us > MAX_PAIR_FAMILY_SPREAD_US:
                    continue
                kept_ids.update(id(pair) for pair in window)

        filtered = [pair for pair in pairs if id(pair) in kept_ids]
        filtered.sort(key=lambda pair: pair.ts)
        return filtered

    def select_solver_pairs(self, pairs: list[CalibrationPair]) -> list[CalibrationPair]:
        """Prefer a consistent near-coincident subset when it is solver-sized."""
        consistent_pairs = self.filter_consistent_pairs(pairs)
        coincident_pairs = self.filter_coincident_pairs(consistent_pairs)
        if len(coincident_pairs) >= MIN_PAIRS:
            return coincident_pairs
        return consistent_pairs

    def coincident_diagnostics(self, pairs: list[CalibrationPair]) -> dict:
        """Summarise coincident-solver readiness and current blocking stage."""
        family_groups: dict[tuple[str, str], list[CalibrationPair]] = defaultdict(list)
        for pair in pairs:
            family_groups[(pair.icao_a, pair.icao_b)].append(pair)

        kept_ids: set[int] = set()
        family_summaries: list[dict] = []
        for (icao_a, icao_b), family_pairs in family_groups.items():
            ordered = sorted(family_pairs, key=lambda pair: pair.ts)
            best_window = None
            stable = False
            for idx in range(len(ordered) - MIN_PAIR_FAMILY_REPEATS + 1):
                window = ordered[idx: idx + MIN_PAIR_FAMILY_REPEATS]
                window_span_s = window[-1].ts - window[0].ts
                if window_span_s > MAX_PAIR_FAMILY_WINDOW_S:
                    continue
                spread_us = max(pair.tdoa_us for pair in window) - min(pair.tdoa_us for pair in window)
                if spread_us > MAX_PAIR_FAMILY_SPREAD_US:
                    continue
                stable = True
                kept_ids.update(id(pair) for pair in window)
                if best_window is None or spread_us < best_window["spread_us"]:
                    best_window = {
                        "count": len(window),
                        "window_s": round(window_span_s, 1),
                        "spread_us": round(spread_us, 1),
                        "mean_tdoa_us": round(sum(pair.tdoa_us for pair in window) / len(window), 1),
                        "coincident_count": sum(1 for pair in window if abs(pair.tdoa_us) <= MAX_COINCIDENT_TDOA_US),
                    }

            family_summaries.append({
                "icao_a": icao_a,
                "icao_b": icao_b,
                "observations": len(ordered),
                "stable": stable,
                "best_window": best_window,
            })

        consistent_pairs = [pair for pair in pairs if id(pair) in kept_ids]
        coincident_pairs = [pair for pair in consistent_pairs if abs(pair.tdoa_us) <= MAX_COINCIDENT_TDOA_US]

        usable_lines = 0
        azimuths: list[float] = []
        intersections: list[tuple[float, float]] = []
        cluster_rms_m = None
        cluster_members = 0
        estimate = None

        # Cap the pairs used for the O(L²) intersection computation.  Family
        # analysis above already ran over the full consistent set; the geometry
        # solver only needs a representative spread of azimuths, not all L pairs.
        _MAX_INTERSECTION_PAIRS = 60
        geometry_pairs = (
            coincident_pairs[-_MAX_INTERSECTION_PAIRS :]
            if len(coincident_pairs) > _MAX_INTERSECTION_PAIRS
            else coincident_pairs
        )

        if geometry_pairs:
            origin_lat = geometry_pairs[0].receiver_lat
            origin_lon = geometry_pairs[0].receiver_lon
            lines: list[tuple[tuple[float, float], tuple[float, float]]] = []
            normals: list[tuple[float, float, float]] = []
            aircraft_pairs_xy: list[tuple[float, float, float, float]] = []
            for pair in geometry_pairs:
                ax, ay = _latlon_to_xy(pair.lat_a, pair.lon_a, origin_lat, origin_lon)
                bx, by = _latlon_to_xy(pair.lat_b, pair.lon_b, origin_lat, origin_lon)
                if math.hypot(bx - ax, by - ay) < 1000.0:
                    continue
                normal = self._line_normal((ax, ay), (bx, by))
                if normal is None:
                    continue
                lines.append(((ax, ay), (bx, by)))
                normals.append(normal)
                aircraft_pairs_xy.append((ax, ay, bx, by))
                azimuths.append(_bearing_deg(origin_lat, origin_lon, pair.lat_a, pair.lon_a))
                azimuths.append(_bearing_deg(origin_lat, origin_lon, pair.lat_b, pair.lon_b))

            usable_lines = len(lines)
            if len(lines) >= 2:
                ransac_result = self._ransac_beam_lines(lines, normals, aircraft_pairs_xy)
                if ransac_result is not None:
                    candidate_xy, inlier_indices = ransac_result
                    (ix, iy), cep_m = self._irls_beam_lines(candidate_xy, normals, inlier_indices)
                    # Use inlier count in place of raw intersection count for threshold checks
                    intersections = [(ix, iy)] * len(inlier_indices)
                    cluster_members = len(inlier_indices)
                    cluster_rms_m = cep_m
                    est_lat, est_lon = _xy_to_latlon(ix, iy, origin_lat, origin_lon)
                    estimate = {
                        "lat": round(est_lat, 6),
                        "lon": round(est_lon, 6),
                        "cep_m": round(cep_m, 1),
                    }

        az_spread = round(self._azimuth_spread_deg(azimuths), 1) if azimuths else 0.0
        stable_families = sum(1 for family in family_summaries if family["stable"])

        if not pairs:
            status = "no_pairs"
            blocker = "No calibration pairs have been captured yet."
        elif stable_families == 0:
            status = "collecting_repeats"
            blocker = (
                f"No pair family has reached {MIN_PAIR_FAMILY_REPEATS} locally consistent repeats "
                f"within {MAX_PAIR_FAMILY_WINDOW_S:.0f}s."
            )
        elif len(coincident_pairs) < MIN_COINCIDENT_PAIRS:
            status = "insufficient_coincident_pairs"
            blocker = (
                f"Only {len(coincident_pairs)} coincident pairs survived timing filters; "
                f"{MIN_COINCIDENT_PAIRS} are required."
            )
        elif usable_lines < MIN_COINCIDENT_PAIRS:
            status = "insufficient_lines"
            blocker = (
                f"Only {usable_lines} usable beam lines were formed from coincident pairs; "
                f"{MIN_COINCIDENT_PAIRS} are required."
            )
        elif az_spread < MIN_COINCIDENT_AZ_SPREAD_DEG:
            status = "poor_geometry"
            blocker = (
                f"Azimuth spread is {az_spread:.1f}°, below the {MIN_COINCIDENT_AZ_SPREAD_DEG:.0f}° minimum."
            )
        elif len(intersections) < MIN_COINCIDENT_INTERSECTIONS:
            status = "insufficient_intersections"
            blocker = (
                f"Only {len(intersections)} line intersections were produced; "
                f"{MIN_COINCIDENT_INTERSECTIONS} are required."
            )
        elif cluster_members < MIN_COINCIDENT_INTERSECTIONS:
            status = "cluster_too_sparse"
            blocker = (
                f"Best intersection cluster retained {cluster_members} members; "
                f"{MIN_COINCIDENT_INTERSECTIONS} are required."
            )
        else:
            status = "ready"
            blocker = None

        top_families = sorted(
            family_summaries,
            key=lambda family: (
                0 if family["stable"] else 1,
                -(family["best_window"]["coincident_count"] if family["best_window"] else 0),
                -family["observations"],
            ),
        )[:10]

        return {
            "status": status,
            "blocker": blocker,
            "total_pairs": len(pairs),
            "pair_families": len(family_summaries),
            "stable_families": stable_families,
            "consistent_pairs": len(consistent_pairs),
            "coincident_pairs": len(coincident_pairs),
            "usable_lines": usable_lines,
            "intersections": len(intersections),
            "cluster_members": cluster_members,
            "azimuth_spread_deg": az_spread,
            "cluster_rms_m": round(cluster_rms_m, 1) if cluster_rms_m is not None else None,
            "estimate": estimate,
            "thresholds": {
                "min_family_repeats": MIN_PAIR_FAMILY_REPEATS,
                "max_family_window_s": MAX_PAIR_FAMILY_WINDOW_S,
                "max_family_spread_us": MAX_PAIR_FAMILY_SPREAD_US,
                "max_coincident_tdoa_us": MAX_COINCIDENT_TDOA_US,
                "min_coincident_pairs": MIN_COINCIDENT_PAIRS,
                "min_azimuth_spread_deg": MIN_COINCIDENT_AZ_SPREAD_DEG,
                "min_intersections": MIN_COINCIDENT_INTERSECTIONS,
            },
            "top_families": top_families,
        }

    def solve_coincident(
        self,
        pairs: list[CalibrationPair],
        seed_lat: float | None = None,
        seed_lon: float | None = None,
    ) -> tuple[float, float, float, int]:
        """Solve radar position from coincident-illumination bearing lines.

        Each near-coincident pair implies the aircraft were illuminated on nearly
        the same radar bearing, so the radar lies approximately on the line
        through the two aircraft. We intersect those lines and cluster the
        plausible crossings.

        *seed_lat/seed_lon*: optional prior position estimate (e.g. from the FM
        solver) evaluated as a RANSAC hypothesis before random sampling.  When
        present this prevents coherent outlier clusters from producing a confident
        but wrong result.
        """
        consistent_pairs = self.filter_consistent_pairs(pairs)
        coincident_pairs = self.filter_coincident_pairs(consistent_pairs)
        if len(coincident_pairs) < MIN_COINCIDENT_PAIRS:
            raise ValueError(
                f"Insufficient coincident pairs ({len(coincident_pairs)} < {MIN_COINCIDENT_PAIRS})"
            )

        origin_lat = coincident_pairs[0].receiver_lat
        origin_lon = coincident_pairs[0].receiver_lon

        _MAX_SOLVER_LINES = 60
        solve_pairs = (
            coincident_pairs[-_MAX_SOLVER_LINES:]
            if len(coincident_pairs) > _MAX_SOLVER_LINES
            else coincident_pairs
        )

        lines: list[tuple[tuple[float, float], tuple[float, float]]] = []
        normals: list[tuple[float, float, float]] = []
        aircraft_pairs_xy: list[tuple[float, float, float, float]] = []
        azimuths: list[float] = []
        for pair in solve_pairs:
            ax, ay = _latlon_to_xy(pair.lat_a, pair.lon_a, origin_lat, origin_lon)
            bx, by = _latlon_to_xy(pair.lat_b, pair.lon_b, origin_lat, origin_lon)
            if math.hypot(bx - ax, by - ay) < 1000.0:
                continue
            normal = self._line_normal((ax, ay), (bx, by))
            if normal is None:
                continue
            lines.append(((ax, ay), (bx, by)))
            normals.append(normal)
            aircraft_pairs_xy.append((ax, ay, bx, by))
            azimuths.append(_bearing_deg(origin_lat, origin_lon, pair.lat_a, pair.lon_a))
            azimuths.append(_bearing_deg(origin_lat, origin_lon, pair.lat_b, pair.lon_b))

        if len(lines) < MIN_COINCIDENT_PAIRS:
            raise ValueError("Too few usable coincident bearing lines")

        az_spread = self._azimuth_spread_deg(azimuths)
        if az_spread < MIN_COINCIDENT_AZ_SPREAD_DEG:
            raise ValueError(f"Poor coincident geometry: azimuth spread < {MIN_COINCIDENT_AZ_SPREAD_DEG}°")

        seed_xy: tuple[float, float] | None = None
        if seed_lat is not None and seed_lon is not None:
            seed_xy = _latlon_to_xy(seed_lat, seed_lon, origin_lat, origin_lon)

        ransac_result = self._ransac_beam_lines(lines, normals, aircraft_pairs_xy, seed_xy=seed_xy)
        if ransac_result is None:
            raise ValueError("RANSAC found no consistent beam-line intersection")

        candidate_xy, inlier_indices = ransac_result
        if len(inlier_indices) < MIN_COINCIDENT_INTERSECTIONS:
            raise ValueError(
                f"Insufficient RANSAC inliers ({len(inlier_indices)} < {MIN_COINCIDENT_INTERSECTIONS})"
            )

        (ix, iy), rms_m = self._irls_beam_lines(candidate_xy, normals, inlier_indices)
        est_lat, est_lon = _xy_to_latlon(ix, iy, origin_lat, origin_lon)
        return est_lat, est_lon, rms_m, len(coincident_pairs)

    def _azimuth_spread_deg(self, azimuths: list[float]) -> float:
        if len(azimuths) < 2:
            return 0.0
        ordered = sorted({az % 360.0 for az in azimuths})
        if len(ordered) < 2:
            return 0.0
        gaps = [ordered[i + 1] - ordered[i] for i in range(len(ordered) - 1)]
        gaps.append(360.0 - ordered[-1] + ordered[0])
        return 360.0 - max(gaps)

    def _line_intersection_xy(
        self,
        a0: tuple[float, float],
        a1: tuple[float, float],
        b0: tuple[float, float],
        b1: tuple[float, float],
    ) -> tuple[float, float] | None:
        x1, y1 = a0
        x2, y2 = a1
        x3, y3 = b0
        x4, y4 = b1
        denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)
        if abs(denom) < 1e-6:
            return None
        det_a = x1 * y2 - y1 * x2
        det_b = x3 * y4 - y3 * x4
        px = (det_a * (x3 - x4) - (x1 - x2) * det_b) / denom
        py = (det_a * (y3 - y4) - (y1 - y2) * det_b) / denom
        return px, py

    def _cluster_intersections(
        self,
        intersections: list[tuple[float, float]],
    ) -> tuple[float, float, float, int]:
        best_members: list[tuple[float, float]] = []
        for cx, cy in intersections:
            members = [
                point for point in intersections
                if math.hypot(point[0] - cx, point[1] - cy) <= MAX_COINCIDENT_CLUSTER_RADIUS_M
            ]
            if len(members) > len(best_members):
                best_members = members

        if not best_members:
            raise ValueError("No coincident intersection cluster found")

        mean_x = sum(point[0] for point in best_members) / len(best_members)
        mean_y = sum(point[1] for point in best_members) / len(best_members)
        rms_m = math.sqrt(
            sum((point[0] - mean_x) ** 2 + (point[1] - mean_y) ** 2 for point in best_members)
            / len(best_members)
        )
        return mean_x, mean_y, rms_m, len(best_members)

    def _line_normal(
        self,
        p0: tuple[float, float],
        p1: tuple[float, float],
    ) -> tuple[float, float, float] | None:
        """Unit-normal form (nx, ny, c) for the infinite line through p0 and p1.

        The signed perpendicular distance from a test point (px, py) to the line is:
            nx*px + ny*py - c
        """
        dx = p1[0] - p0[0]
        dy = p1[1] - p0[1]
        length = math.hypot(dx, dy)
        if length < 1e-6:
            return None
        nx, ny = -dy / length, dx / length
        c = nx * p0[0] + ny * p0[1]
        return nx, ny, c

    def _angular_separation_deg(
        self,
        rx: float, ry: float,
        ax: float, ay: float,
        bx: float, by: float,
    ) -> float:
        """Angular separation (°) between aircraft A and B as seen from (rx, ry)."""
        phi_a = math.atan2(ay - ry, ax - rx)
        phi_b = math.atan2(by - ry, bx - rx)
        diff = abs(phi_a - phi_b) % (2 * math.pi)
        if diff > math.pi:
            diff = 2 * math.pi - diff
        return math.degrees(diff)

    def _ransac_beam_lines(
        self,
        lines: list[tuple[tuple[float, float], tuple[float, float]]],
        normals: list[tuple[float, float, float]],
        aircraft_pairs_xy: list[tuple[float, float, float, float]],
        n_iter: int = 100,
        inlier_threshold_m: float = 8_000.0,
        beam_width_deg: float = 5.0,
        seed_xy: tuple[float, float] | None = None,
    ) -> tuple[tuple[float, float], list[int]] | None:
        """RANSAC over beam lines. Returns (best_candidate_xy, inlier_indices) or None.

        Each iteration picks 2 random lines, finds their intersection as a candidate
        radar position, then counts inliers — lines whose perpendicular distance to
        the candidate is within *inlier_threshold_m* AND whose aircraft subtend more
        than 2×beam_width_deg at the candidate (pairs too close together angularly
        can't constrain the radar direction and are rejected).

        If *seed_xy* is provided (e.g. an FM-solver position in local ENU metres),
        it is evaluated as an additional hypothesis before random sampling.  This
        prevents coherent outlier groups from outscoring the physically-plausible
        consensus when a prior estimate is available.
        """
        n = len(lines)
        if n < 2:
            return None

        min_sep_deg = 2.0 * beam_width_deg
        best_candidate: tuple[float, float] | None = None
        best_inliers: list[int] = []
        idx_pool = list(range(n))

        def _score(rx: float, ry: float) -> list[int]:
            inliers: list[int] = []
            for k in range(n):
                nx, ny, c = normals[k]
                if abs(nx * rx + ny * ry - c) > inlier_threshold_m:
                    continue
                ax, ay, bx, by = aircraft_pairs_xy[k]
                if self._angular_separation_deg(rx, ry, ax, ay, bx, by) < min_sep_deg:
                    continue
                inliers.append(k)
            return inliers

        # When a seed (e.g. FM position) is provided and has sufficient inlier
        # support, use it directly and skip random sampling.  This prevents a
        # majority-outlier cluster from overriding a physically-grounded prior:
        # RANSAC is a vote, and if outliers outnumber good lines the random
        # hypotheses will converge to the wrong cluster regardless of the seed.
        if seed_xy is not None:
            rx, ry = seed_xy
            if math.hypot(rx, ry) <= MAX_SOLVER_RADIUS_M:
                inliers = _score(rx, ry)
                if len(inliers) >= MIN_COINCIDENT_PAIRS:
                    # Seed has enough support — return immediately without random sampling.
                    return seed_xy, inliers

        for _ in range(n_iter):
            i, j = random.sample(idx_pool, 2)
            candidate = self._line_intersection_xy(lines[i][0], lines[i][1], lines[j][0], lines[j][1])
            if candidate is None:
                continue
            rx, ry = candidate
            if math.hypot(rx, ry) > MAX_SOLVER_RADIUS_M:
                continue

            inliers = _score(rx, ry)
            if len(inliers) > len(best_inliers):
                best_inliers = inliers
                best_candidate = candidate

        if best_candidate is None or len(best_inliers) < 2:
            return None

        return best_candidate, best_inliers

    def _irls_beam_lines(
        self,
        init_xy: tuple[float, float],
        normals: list[tuple[float, float, float]],
        inlier_indices: list[int],
        n_iter: int = 8,
        tukey_c_m: float = 4_000.0,
    ) -> tuple[tuple[float, float], float]:
        """IRLS refinement with Tukey bisquare weights on inlier beam lines.

        Returns refined position (x, y) and RMS perpendicular residual (cep_m).
        """
        x, y = init_xy
        A00 = A01 = A11 = det = 0.0  # ensure defined if loop body never completes

        for _ in range(n_iter):
            # Residuals
            residuals = [
                normals[k][0] * x + normals[k][1] * y - normals[k][2]
                for k in inlier_indices
            ]
            # Tukey bisquare weights: w = (1-(r/c)²)² for |r|<c, else 0
            weights = []
            for r in residuals:
                ratio = r / tukey_c_m
                weights.append((1.0 - ratio * ratio) ** 2 if abs(ratio) < 1.0 else 1e-10)

            # Weighted least-squares normal equations
            A00 = A01 = A11 = b0 = b1 = 0.0
            for idx, k in enumerate(inlier_indices):
                nx, ny, c = normals[k]
                w = weights[idx]
                A00 += w * nx * nx
                A01 += w * nx * ny
                A11 += w * ny * ny
                b0 += w * c * nx
                b1 += w * c * ny

            det = A00 * A11 - A01 * A01
            if abs(det) < 1e-12:
                break
            x = (A11 * b0 - A01 * b1) / det
            y = (A00 * b1 - A01 * b0) / det

        # Covariance-based CEP.
        #
        # The IRLS normal equations build the weighted Fisher information matrix
        # F = [[A00, A01], [A01, A11]].  Its inverse is the position covariance
        # (up to a noise-scale factor).  The residual variance sigma² scales that
        # covariance to physical units.
        #
        # Critically: when beam lines are nearly parallel, A has a large eigenvalue
        # across the beams and a tiny eigenvalue along them.  F^{-1} therefore has
        # a large eigenvalue along the beam direction — correctly reporting high
        # uncertainty even when perpendicular residuals are small.  Using the raw
        # RMS residual as CEP would miss this completely.
        sq = sum(
            (normals[k][0] * x + normals[k][1] * y - normals[k][2]) ** 2
            for k in inlier_indices
        )
        n = len(inlier_indices)
        if n < 3 or abs(det) < 1e-12:
            cep_m = float("inf")
        else:
            sigma2 = sq / (n - 2)          # residual variance (2 DOF consumed by x, y)
            # Covariance matrix C = sigma2 * F^{-1}
            # C = sigma2 / det * [[A11, -A01], [-A01, A00]]
            # Eigenvalues of C: lambda = sigma2/det * eigenvalues of [[A11,-A01],[-A01,A00]]
            # = sigma2 * eigenvalues of F^{-1}
            # Largest eigenvalue of F^{-1} = 1 / smallest eigenvalue of F
            trace_f = A00 + A11
            disc_f = math.sqrt(max(0.0, ((A00 - A11) / 2) ** 2 + A01 ** 2))
            lambda_min_f = trace_f / 2 - disc_f          # smallest eigenvalue of F
            if lambda_min_f < 1e-12:
                cep_m = float("inf")
            else:
                lambda_max_cov = sigma2 / lambda_min_f   # largest eigenvalue of C
                cep_m = math.sqrt(lambda_max_cov)        # 1-sigma in worst direction
        return (x, y), cep_m

    def group_pairs_by_sweep(self, pairs: list[CalibrationPair]) -> list[list[CalibrationPair]]:
        """Cluster calibration pairs into sweep-sized groups by timestamp."""
        if not pairs:
            return []

        ordered = sorted(pairs, key=lambda pair: pair.ts)
        groups: list[list[CalibrationPair]] = [[ordered[0]]]
        for pair in ordered[1:]:
            if pair.ts - groups[-1][-1].ts <= SWEEP_GROUP_WINDOW_S:
                groups[-1].append(pair)
            else:
                groups.append([pair])
        return groups

    def solve_static_from_sweeps(
        self,
        pairs: list[CalibrationPair],
        initial_guess: Optional[tuple[float, float]] = None,
    ) -> tuple[float, float, float, int, int]:
        """Solve each sweep independently, then accumulate accepted sweep estimates."""
        sweep_groups = self.group_pairs_by_sweep(pairs)
        sweep_estimates: list[dict] = []

        for sweep_pairs in sweep_groups:
            try:
                lat, lon, cep_m, n_pairs = self.solve_sweep_group(sweep_pairs, initial_guess)
            except Exception as exc:
                failure_key = self._classify_sweep_failure(exc)
                log.debug("RadarLocaliser: sweep solve failed [%s]: %s", failure_key, exc)
                continue
            sweep_estimates.append({
                "lat": lat,
                "lon": lon,
                "cep_m": max(cep_m, 1.0),
                "n_pairs": n_pairs,
            })

        if len(sweep_estimates) < MIN_SWEEP_ESTIMATES:
            raise ValueError(
                f"Insufficient good sweep estimates ({len(sweep_estimates)} < {MIN_SWEEP_ESTIMATES})"
            )

        origin_lat = pairs[0].receiver_lat
        origin_lon = pairs[0].receiver_lon
        weighted_x = 0.0
        weighted_y = 0.0
        total_weight = 0.0
        total_pairs = 0
        for estimate in sweep_estimates:
            x, y = _latlon_to_xy(estimate["lat"], estimate["lon"], origin_lat, origin_lon)
            weight = estimate["n_pairs"] / (estimate["cep_m"] ** 2)
            weighted_x += x * weight
            weighted_y += y * weight
            total_weight += weight
            total_pairs += estimate["n_pairs"]

        if total_weight <= 0:
            raise ValueError("Sweep accumulation produced zero total weight")

        mean_x = weighted_x / total_weight
        mean_y = weighted_y / total_weight
        est_lat, est_lon = _xy_to_latlon(mean_x, mean_y, origin_lat, origin_lon)
        cep_m = math.sqrt(1.0 / total_weight)
        return est_lat, est_lon, cep_m, total_pairs, len(sweep_estimates)

    def diagnose_sweep_groups(
        self,
        pairs: list[CalibrationPair],
        initial_guess: Optional[tuple[float, float]] = None,
    ) -> dict:
        """Return success/failure counts for per-sweep solve attempts."""
        sweep_groups = self.group_pairs_by_sweep(pairs)
        return self._diagnose_from_sweep_groups(sweep_groups, initial_guess)

    # Limit sweep solves run during diagnostics to bound latency.  Historical
    # groups beyond this window don't affect the current blocker status message.
    _MAX_DIAG_SWEEP_GROUPS = 50

    def _diagnose_from_sweep_groups(
        self,
        sweep_groups: list[list[CalibrationPair]],
        initial_guess: Optional[tuple[float, float]] = None,
    ) -> dict:
        """Diagnose sweep-solve success/failure for pre-computed groups.

        Reports total_sweeps from the full group list but only evaluates the
        most recent _MAX_DIAG_SWEEP_GROUPS groups to keep per-call cost bounded.
        """
        counts: dict[str, int] = {
            "total_sweeps": len(sweep_groups),
            "solved": 0,
        }
        eval_groups = (
            sweep_groups[-self._MAX_DIAG_SWEEP_GROUPS :]
            if len(sweep_groups) > self._MAX_DIAG_SWEEP_GROUPS
            else sweep_groups
        )
        for sweep_pairs in eval_groups:
            try:
                self.solve_sweep_group(sweep_pairs, initial_guess)
            except Exception as exc:
                key = self._classify_sweep_failure(exc)
                counts[key] = counts.get(key, 0) + 1
            else:
                counts["solved"] += 1
        return counts

    def tdoa_diagnostics(self, pairs: list[CalibrationPair]) -> dict:
        """Summarise TDOA readiness, sweep viability, and current blocker state."""
        # Compute consistent_pairs once; inline select_solver_pairs to reuse it.
        consistent_pairs = self.filter_consistent_pairs(pairs)
        coincident_pairs = self.filter_coincident_pairs(consistent_pairs)
        selected_pairs = coincident_pairs if len(coincident_pairs) >= MIN_PAIRS else consistent_pairs
        # Compute sweep_groups once; pass to _diagnose_from_sweep_groups to avoid
        # a second group_pairs_by_sweep call inside diagnose_sweep_groups.
        sweep_groups = self.group_pairs_by_sweep(pairs)
        sweep_diag = self._diagnose_from_sweep_groups(sweep_groups) if pairs else {"total_sweeps": 0, "solved": 0}

        selected_azimuths: list[float] = []
        if selected_pairs:
            origin_lat = selected_pairs[0].receiver_lat
            origin_lon = selected_pairs[0].receiver_lon
            for pair in selected_pairs:
                selected_azimuths.append(_bearing_deg(origin_lat, origin_lon, pair.lat_a, pair.lon_a))
                selected_azimuths.append(_bearing_deg(origin_lat, origin_lon, pair.lat_b, pair.lon_b))
        az_spread = round(self._azimuth_spread_deg(selected_azimuths), 1) if selected_azimuths else 0.0

        if not pairs:
            status = "no_pairs"
            blocker = "No calibration pairs have been captured yet."
        elif len(consistent_pairs) < MIN_PAIRS:
            status = "insufficient_consistent_pairs"
            blocker = f"Only {len(consistent_pairs)} consistent pairs survived family filtering; {MIN_PAIRS} are required."
        elif len(selected_pairs) < MIN_PAIRS:
            status = "insufficient_solver_pairs"
            blocker = f"Only {len(selected_pairs)} pairs were eligible for the direct TDOA solver; {MIN_PAIRS} are required."
        elif az_spread < MIN_AZ_SPREAD_DEG:
            status = "poor_geometry"
            blocker = f"Selected-pair azimuth spread is {az_spread:.1f}°, below the {MIN_AZ_SPREAD_DEG:.0f}° minimum."
        elif sweep_diag.get("solved", 0) >= MIN_SWEEP_ESTIMATES:
            status = "ready"
            blocker = None
        elif len(sweep_groups) < MIN_SWEEP_ESTIMATES:
            status = "insufficient_sweeps"
            blocker = f"Only {len(sweep_groups)} sweep groups are available; {MIN_SWEEP_ESTIMATES} good sweep solves are required."
        else:
            status = "direct_only"
            blocker = (
                f"Sweep accumulation has only {sweep_diag.get('solved', 0)} good sweep solves; "
                f"direct TDOA may still work, but sweep accumulation is not ready."
            )

        max_pairs_in_sweep = max((len(group) for group in sweep_groups), default=0)
        mean_pairs_in_sweep = round(sum(len(group) for group in sweep_groups) / len(sweep_groups), 1) if sweep_groups else 0.0

        return {
            "status": status,
            "blocker": blocker,
            "total_pairs": len(pairs),
            "consistent_pairs": len(consistent_pairs),
            "selected_pairs": len(selected_pairs),
            "sweep_groups": len(sweep_groups),
            "max_pairs_in_sweep": max_pairs_in_sweep,
            "mean_pairs_in_sweep": mean_pairs_in_sweep,
            "selected_pair_azimuth_spread_deg": az_spread,
            "sweep_diagnostics": sweep_diag,
            "thresholds": {
                "min_pairs": MIN_PAIRS,
                "min_sweep_estimates": MIN_SWEEP_ESTIMATES,
                "min_sweep_aircraft": MIN_SWEEP_AIRCRAFT,
                "min_azimuth_spread_deg": MIN_AZ_SPREAD_DEG,
            },
        }

    def _build_sweep_observations(self, sweep_pairs: list[CalibrationPair]) -> list[dict]:
        """Reconstruct per-aircraft relative timing observations from one sweep's pairs."""
        aircraft_positions: dict[str, tuple[float, float]] = {}
        pair_tdoa_all: dict[tuple[str, str], list[float]] = defaultdict(list)
        for pair in sweep_pairs:
            aircraft_positions[pair.icao_a] = (pair.lat_a, pair.lon_a)
            aircraft_positions[pair.icao_b] = (pair.lat_b, pair.lon_b)
            pair_tdoa_all[(pair.icao_a, pair.icao_b)].append(pair.tdoa_us)

        # Combine duplicate same-sweep pair entries using median TDOA rather than
        # the last-write-wins overwrite that existed before this fix.
        pair_tdoa: dict[tuple[str, str], float] = {}
        for key, tdoa_values in pair_tdoa_all.items():
            s = sorted(tdoa_values)
            n = len(s)
            pair_tdoa[key] = s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2

        if len(aircraft_positions) < MIN_SWEEP_AIRCRAFT:
            raise ValueError(
                f"Insufficient sweep aircraft ({len(aircraft_positions)} < {MIN_SWEEP_AIRCRAFT})"
            )

        adjacency: dict[str, list[tuple[str, float]]] = {icao: [] for icao in aircraft_positions}
        for (icao_a, icao_b), tdoa_us in pair_tdoa.items():
            adjacency[icao_a].append((icao_b, -tdoa_us))
            adjacency[icao_b].append((icao_a, tdoa_us))

        components: list[dict[str, float]] = []
        seen: set[str] = set()
        for start_icao in sorted(aircraft_positions):
            if start_icao in seen:
                continue
            queue = deque([(start_icao, 0.0)])
            component: dict[str, float] = {}
            while queue:
                icao, rel_arrival_us = queue.popleft()
                if icao in component:
                    continue
                component[icao] = rel_arrival_us
                seen.add(icao)
                for next_icao, next_rel_arrival_us in adjacency.get(icao, []):
                    if next_icao not in component:
                        queue.append((next_icao, rel_arrival_us + next_rel_arrival_us))
            components.append(component)

        largest_component = max(components, key=len) if components else {}
        if len(largest_component) < MIN_SWEEP_AIRCRAFT:
            raise ValueError(
                f"Sweep missing anchor pair connectivity ({len(largest_component)} < {MIN_SWEEP_AIRCRAFT})"
            )

        anchor_icao = min(largest_component)
        anchor_offset = largest_component[anchor_icao]
        observations = []
        for icao in sorted(largest_component):
            lat, lon = aircraft_positions[icao]
            observations.append({
                "icao": icao,
                "lat": lat,
                "lon": lon,
                "rel_arrival_us": largest_component[icao] - anchor_offset,
            })
        return observations

    def _check_sweep_geometry(
        self,
        observations: list[dict],
        origin_lat: float,
        origin_lon: float,
    ) -> bool:
        azimuths = sorted({
            _bearing_deg(origin_lat, origin_lon, obs["lat"], obs["lon"])
            for obs in observations
        })
        if len(azimuths) < 2:
            return False
        gaps = [azimuths[i + 1] - azimuths[i] for i in range(len(azimuths) - 1)]
        gaps.append(360 - azimuths[-1] + azimuths[0])
        spread = 360 - max(gaps)
        return spread >= MIN_AZ_SPREAD_DEG

    def sweep_residuals(
        self,
        R: list[float],
        observations: list[dict],
        origin_lat: float,
        origin_lon: float,
    ) -> list[float]:
        """Residuals for one sweep solved directly from relative arrival offsets."""
        rx, ry = R
        ref = observations[0]
        ref_x, ref_y = _latlon_to_xy(ref["lat"], ref["lon"], origin_lat, origin_lon)
        ref_radar_m = math.hypot(rx - ref_x, ry - ref_y)
        ref_receiver_m = math.hypot(ref_x, ref_y)

        residuals = []
        for obs in observations[1:]:
            ax, ay = _latlon_to_xy(obs["lat"], obs["lon"], origin_lat, origin_lon)
            radar_m = math.hypot(rx - ax, ry - ay)
            receiver_m = math.hypot(ax, ay)
            predicted_rel_us = ((radar_m + receiver_m) - (ref_radar_m + ref_receiver_m)) / _C_MUS
            residuals.append(predicted_rel_us - obs["rel_arrival_us"])
        return residuals

    def solve_sweep_group(
        self,
        sweep_pairs: list[CalibrationPair],
        initial_guess: Optional[tuple[float, float]] = None,
    ) -> tuple[float, float, float, int]:
        """Solve one sweep directly from all aircraft in that sweep."""
        try:
            from scipy.optimize import least_squares
        except ImportError:
            raise RuntimeError("scipy is required for radar localisation")

        observations = self._build_sweep_observations(sweep_pairs)
        origin_lat = sweep_pairs[0].receiver_lat
        origin_lon = sweep_pairs[0].receiver_lon

        if not self._check_sweep_geometry(observations, origin_lat, origin_lon):
            raise ValueError(f"Poor sweep geometry: azimuth spread < {MIN_AZ_SPREAD_DEG}°")

        if initial_guess is not None:
            x0_lat, x0_lon = initial_guess
        else:
            x0_lat, x0_lon = origin_lat, origin_lon
        x0_x, x0_y = _latlon_to_xy(x0_lat, x0_lon, origin_lat, origin_lon)

        search_radius_m = self._estimate_search_radius_m(sweep_pairs, origin_lat, origin_lon)

        # Precompute static per-aircraft ENU coordinates so the residual evaluator
        # avoids repeated _latlon_to_xy calls on every least-squares evaluation.
        ref_x, ref_y = _latlon_to_xy(
            observations[0]["lat"], observations[0]["lon"], origin_lat, origin_lon
        )
        ref_recv_m = math.hypot(ref_x, ref_y)
        obs_precomp = [
            (*_latlon_to_xy(obs["lat"], obs["lon"], origin_lat, origin_lon), obs["rel_arrival_us"])
            for obs in observations[1:]
        ]
        # Precompute static receiver-path distances for non-reference aircraft.
        obs_precomp_full = [
            (ax, ay, math.hypot(ax, ay), rel_us)
            for ax, ay, rel_us in obs_precomp
        ]

        def _fast_sweep_residuals(R: list[float]) -> list[float]:
            rx, ry = R
            ref_radar_m = math.hypot(rx - ref_x, ry - ref_y)
            return [
                ((math.hypot(rx - ax, ry - ay) + recv_m) - (ref_radar_m + ref_recv_m)) / _C_MUS - rel_us
                for ax, ay, recv_m, rel_us in obs_precomp_full
            ]

        result = least_squares(
            fun=_fast_sweep_residuals,
            x0=[x0_x, x0_y],
            method="trf",
            bounds=(
                [-search_radius_m, -search_radius_m],
                [search_radius_m, search_radius_m],
            ),
            loss="soft_l1",
            f_scale=1.0,
            max_nfev=1000,
        )

        if not result.success:
            raise ValueError(f"Sweep solver did not converge: cost={result.cost:.1f}")

        rx, ry = result.x
        radial_distance_m = math.hypot(rx, ry)
        if radial_distance_m >= search_radius_m * 0.98:
            raise ValueError(f"Sweep solver landed on search boundary ({radial_distance_m:.0f} m)")

        est_lat, est_lon = _xy_to_latlon(rx, ry, origin_lat, origin_lon)
        residuals = result.fun
        rms_us = math.sqrt(sum(r ** 2 for r in residuals) / len(residuals)) if residuals else 0.0
        cep_m = _C_MUS * rms_us
        if rms_us > MAX_SOLVER_RMS_US:
            raise ValueError(f"Sweep solver residual too large ({rms_us:.2f} us)")
        if cep_m > MAX_SOLVER_CEP_M:
            raise ValueError(f"Sweep solver CEP too large ({cep_m:.0f} m)")

        return est_lat, est_lon, cep_m, len(sweep_pairs)

    def _corrected_range_difference_m(self, pair: CalibrationPair) -> float:
        """Return the proposal's receiver-path-corrected range-difference target.

        Observed burst TDOA includes both the unknown radar-to-aircraft path
        difference and the known receiver-to-aircraft path difference:

            c * Δt = (|R-A| - |R-B|) + (|A-S| - |B-S|)

        Therefore the solver target for the unknown radar position is:

            |R-A| - |R-B| = c * Δt - (|A-S| - |B-S|)
        """
        ax, ay = _latlon_to_xy(pair.lat_a, pair.lon_a, pair.receiver_lat, pair.receiver_lon)
        bx, by = _latlon_to_xy(pair.lat_b, pair.lon_b, pair.receiver_lat, pair.receiver_lon)
        d_sa = math.hypot(ax, ay)
        d_sb = math.hypot(bx, by)
        return (pair.tdoa_us * _C_MUS) - (d_sa - d_sb)

    def _estimate_search_radius_m(
        self,
        pairs: list[CalibrationPair],
        origin_lat: float,
        origin_lon: float,
    ) -> float:
        max_aircraft_range_m = 0.0
        for pair in pairs:
            max_aircraft_range_m = max(
                max_aircraft_range_m,
                _haversine_m(origin_lat, origin_lon, pair.lat_a, pair.lon_a),
                _haversine_m(origin_lat, origin_lon, pair.lat_b, pair.lon_b),
            )
        return min(
            MAX_SOLVER_RADIUS_M,
            max(MIN_SOLVER_RADIUS_M, max_aircraft_range_m + 100_000.0),
        )

    def tdoa_residuals(self, R: list[float], pairs: list[CalibrationPair],
                       origin_lat: float, origin_lon: float) -> list[float]:
        """Return residual vector for candidate radar position R = [rx, ry] (ENU metres).

        Each TDOA pair gives one residual:
            r_i = (d_radar_a - d_radar_b) - target_diff_ab

        where d_radar_X is the distance from the radar to aircraft X,
        and target_diff_ab is the receiver-path-corrected distance difference
        derived from the observed burst TDOA.
        """
        rx, ry = R
        residuals = []

        for pair in pairs:
            ax, ay = _latlon_to_xy(pair.lat_a, pair.lon_a, origin_lat, origin_lon)
            bx, by = _latlon_to_xy(pair.lat_b, pair.lon_b, origin_lat, origin_lon)

            d_ra = math.sqrt((rx - ax) ** 2 + (ry - ay) ** 2)
            d_rb = math.sqrt((rx - bx) ** 2 + (ry - by) ** 2)
            residuals.append(
                (d_ra - d_rb - self._corrected_range_difference_m(pair)) / _C_MUS
            )

        return residuals

    def _check_geometry(self, pairs: list[CalibrationPair],
                        origin_lat: float, origin_lon: float) -> bool:
        """Return True if calibration aircraft span at least MIN_AZ_SPREAD_DEG."""
        azimuths = set()
        for pair in pairs:
            az_a = _bearing_deg(origin_lat, origin_lon, pair.lat_a, pair.lon_a)
            az_b = _bearing_deg(origin_lat, origin_lon, pair.lat_b, pair.lon_b)
            azimuths.add(az_a)
            azimuths.add(az_b)

        if len(azimuths) < 2:
            return False

        azlist = sorted(azimuths)
        # Largest gap method for circular spread
        gaps = [azlist[i + 1] - azlist[i] for i in range(len(azlist) - 1)]
        gaps.append(360 - azlist[-1] + azlist[0])
        max_gap = max(gaps)
        spread = 360 - max_gap

        return spread >= MIN_AZ_SPREAD_DEG

    def solve(
        self,
        pairs: list[CalibrationPair],
        initial_guess: Optional[tuple[float, float]] = None,
    ) -> tuple[float, float, float, int]:
        """Run the hyperbolic TDOA solver.

        Args:
            pairs: list of CalibrationPair objects
            initial_guess: (lat, lon) seed for the solver; defaults to receiver position

        Returns:
            (lat, lon, cep_m, n_pairs_used)

        Raises:
            ValueError: if geometry is poor or insufficient pairs
        """
        try:
            from scipy.optimize import least_squares
        except ImportError:
            raise RuntimeError("scipy is required for radar localisation")

        pairs = self.select_solver_pairs(pairs)
        if len(pairs) < MIN_PAIRS:
            raise ValueError(f"Insufficient consistent pairs ({len(pairs)} < {MIN_PAIRS})")

        # Use receiver position as origin for ENU
        origin_lat = pairs[0].receiver_lat
        origin_lon = pairs[0].receiver_lon

        if not self._check_geometry(pairs, origin_lat, origin_lon):
            raise ValueError(f"Poor geometry: azimuth spread < {MIN_AZ_SPREAD_DEG}°")

        # Initial guess in ENU
        if initial_guess is not None:
            x0_lat, x0_lon = initial_guess
        else:
            x0_lat, x0_lon = origin_lat, origin_lon

        x0_x, x0_y = _latlon_to_xy(x0_lat, x0_lon, origin_lat, origin_lon)

        search_radius_m = self._estimate_search_radius_m(pairs, origin_lat, origin_lon)

        # Precompute per-pair ENU coordinates and corrected range-difference targets
        # so the residual evaluator avoids repeated _latlon_to_xy calls on every
        # least-squares function evaluation.
        pairs_precomp = [
            (
                *_latlon_to_xy(pair.lat_a, pair.lon_a, origin_lat, origin_lon),
                *_latlon_to_xy(pair.lat_b, pair.lon_b, origin_lat, origin_lon),
                self._corrected_range_difference_m(pair),
            )
            for pair in pairs
        ]

        def _fast_tdoa_residuals(R: list[float]) -> list[float]:
            rx, ry = R
            return [
                (math.sqrt((rx - ax) ** 2 + (ry - ay) ** 2)
                 - math.sqrt((rx - bx) ** 2 + (ry - by) ** 2)
                 - delta) / _C_MUS
                for ax, ay, bx, by, delta in pairs_precomp
            ]

        result = least_squares(
            fun=_fast_tdoa_residuals,
            x0=[x0_x, x0_y],
            method="trf",
            bounds=(
                [-search_radius_m, -search_radius_m],
                [search_radius_m, search_radius_m],
            ),
            loss="soft_l1",
            f_scale=1.0,
            max_nfev=1000,
        )

        if not result.success:
            raise ValueError(f"Solver did not converge: cost={result.cost:.1f}")

        rx, ry = result.x
        radial_distance_m = math.hypot(rx, ry)
        if radial_distance_m >= search_radius_m * 0.98:
            raise ValueError(
                f"Solver landed on search boundary ({radial_distance_m:.0f} m)"
            )

        est_lat, est_lon = _xy_to_latlon(rx, ry, origin_lat, origin_lon)

        # Estimate CEP from residuals RMS → distance uncertainty
        residuals = result.fun
        rms_us = 0.0
        if len(residuals) > 0:
            rms_us = math.sqrt(sum(r ** 2 for r in residuals) / len(residuals))
            cep_m = _C_MUS * rms_us
        else:
            cep_m = 0.0

        if rms_us > MAX_SOLVER_RMS_US:
            raise ValueError(f"Solver residual too large ({rms_us:.2f} us)")
        if cep_m > MAX_SOLVER_CEP_M:
            raise ValueError(f"Solver CEP too large ({cep_m:.0f} m)")

        return est_lat, est_lon, cep_m, len(pairs)

    def compute_hyperbola_points(
        self,
        pair: CalibrationPair,
        n_points: int = 50,
        max_dist_m: float = 500_000,
    ) -> list[dict]:
        """Sample points along the TDOA hyperbola for map overlay.

        The hyperbola is the locus of points P such that
            dist(P, A) - dist(P, B) = corrected_target_ab

        Returns list of {lat, lon} dicts (at most n_points).
        """
        origin_lat = pair.receiver_lat
        origin_lon = pair.receiver_lon

        ax, ay = _latlon_to_xy(pair.lat_a, pair.lon_a, origin_lat, origin_lon)
        bx, by = _latlon_to_xy(pair.lat_b, pair.lon_b, origin_lat, origin_lon)
        target_diff = self._corrected_range_difference_m(pair)

        center_x = (ax + bx) / 2.0
        center_y = (ay + by) / 2.0
        focal_dx = bx - ax
        focal_dy = by - ay
        focal_dist = math.hypot(focal_dx, focal_dy)
        if focal_dist < 1.0:
            return []

        c = focal_dist / 2.0
        a = abs(target_diff) / 2.0
        if a >= c:
            return []

        ux = focal_dx / focal_dist
        uy = focal_dy / focal_dist
        vx = -uy
        vy = ux

        if a < 1.0:
            # Near-zero corrected TDOA degenerates toward the perpendicular bisector.
            span = min(max_dist_m, 250_000.0)
            result_points = []
            for idx in range(n_points):
                frac = (idx / (n_points - 1)) if n_points > 1 else 0.5
                local_y = -span + (2.0 * span * frac)
                xi = center_x + (vx * local_y)
                yi = center_y + (vy * local_y)
                lat, lon = _xy_to_latlon(xi, yi, origin_lat, origin_lon)
                result_points.append({"lat": round(lat, 6), "lon": round(lon, 6)})
            return result_points

        b_sq = (c * c) - (a * a)
        if b_sq <= 0.0:
            return []
        b = math.sqrt(b_sq)

        branch_sign = 1.0 if target_diff > 0.0 else -1.0
        x_limit = max(abs(branch_sign * a), max_dist_m + c)
        t_max = math.acosh(max(1.000001, x_limit / a))

        result_points = []
        for idx in range(n_points):
            frac = (idx / (n_points - 1)) if n_points > 1 else 0.5
            t = -t_max + (2.0 * t_max * frac)
            local_x = branch_sign * a * math.cosh(t)
            local_y = b * math.sinh(t)
            xi = center_x + (ux * local_x) + (vx * local_y)
            yi = center_y + (uy * local_x) + (vy * local_y)
            if math.hypot(xi, yi) > max_dist_m:
                continue
            lat, lon = _xy_to_latlon(xi, yi, origin_lat, origin_lon)
            result_points.append({"lat": round(lat, 6), "lon": round(lon, 6)})

        return result_points
