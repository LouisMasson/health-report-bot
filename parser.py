"""
Parse Health Auto Export JSON into aggregated weekly metrics.

Health Auto Export sends JSON in this shape (v2):
{
  "data": {
    "metrics": [
      {
        "name": "step_count",
        "units": "count",
        "data": [
          {"date": "2026-03-15 00:00:00 +0100", "qty": 8432.0},
          ...
        ]
      },
      ...
    ],
    "workouts": [
      {
        "id": "...",
        "name": "Traditional Strength Training",
        "start": "2026-03-15 10:00:00 +0100",
        "end": "2026-03-15 11:05:00 +0100",
        "duration": 3900.0,
        "activeEnergyBurned": {"qty": 320.5, "units": "kcal"},
        "distance": {"qty": 5.2, "units": "km"},
        "heartRate": {"min": {"qty": 95}, "avg": {"qty": 125}, "max": {"qty": 165}},
        ...
      },
      ...
    ]
  }
}

Sleep analysis (aggregated) data points look like:
  {"date": "2026-03-15", "asleep": 6.5, "deep": 1.2, "rem": 1.8, "core": 3.5,
   "totalSleep": 6.5, "inBed": 7.2, "sleepStart": "...", "sleepEnd": "..."}
"""

from __future__ import annotations

import logging
from statistics import mean

logger = logging.getLogger(__name__)

# Mapping from Health Auto Export metric names to our internal keys
METRIC_MAP = {
    "step_count": "steps",
    "active_energy": "active_energy",
    "apple_exercise_time": "exercise_minutes",
    "resting_heart_rate": "resting_heart_rate",
    "heart_rate_variability": "hrv",
    "vo2_max": "vo2_max",
    "body_mass": "body_mass",
    "body_fat_percentage": "body_fat",
    "sleep_analysis": "sleep",
}


def _safe_values(data_points: list[dict]) -> list[float]:
    """Extract numeric values, skipping nulls/zeros."""
    values = []
    for point in data_points:
        qty = point.get("qty") or point.get("value")
        if qty is not None and qty != 0:
            values.append(float(qty))
    return values


def _aggregate_simple(values: list[float]) -> dict:
    """Return avg, min, max, total for a list of values."""
    if not values:
        return {"avg": 0, "min": 0, "max": 0, "total": 0, "count": 0}
    return {
        "avg": round(mean(values), 1),
        "min": round(min(values), 1),
        "max": round(max(values), 1),
        "total": round(sum(values), 1),
        "count": len(values),
    }


def _parse_sleep(data_points: list[dict]) -> dict:
    """
    Parse sleep_analysis data points.

    Aggregated format (Summarize ON):
      {"date": "...", "totalSleep": 6.5, "asleep": 6.5, "deep": 1.2,
       "rem": 1.8, "core": 3.5, "inBed": 7.2, ...}

    Unaggregated format (Summarize OFF):
      {"startDate": "...", "endDate": "...", "qty": 1.2,
       "value": "Deep|REM|Core|Asleep|In Bed|Awake", ...}
    """
    # Detect format: aggregated has "asleep" or "totalSleep" keys
    if data_points and ("asleep" in data_points[0] or "totalSleep" in data_points[0]):
        return _parse_sleep_aggregated(data_points)
    return _parse_sleep_unaggregated(data_points)


def _parse_sleep_aggregated(data_points: list[dict]) -> dict:
    """Parse aggregated sleep data (one entry per night)."""
    total_sleep: list[float] = []
    in_bed: list[float] = []
    deep: list[float] = []
    rem: list[float] = []
    core: list[float] = []

    for point in data_points:
        if val := point.get("totalSleep") or point.get("asleep"):
            total_sleep.append(float(val))
        if val := point.get("inBed"):
            in_bed.append(float(val))
        if val := point.get("deep"):
            deep.append(float(val))
        if val := point.get("rem"):
            rem.append(float(val))
        if val := point.get("core"):
            core.append(float(val))

    result: dict = {"nights": len(data_points)}
    if total_sleep:
        result["total_avg_hours"] = round(mean(total_sleep), 1)
    if in_bed:
        result["in_bed_avg_hours"] = round(mean(in_bed), 1)
    if deep:
        result["deep_avg_hours"] = round(mean(deep), 1)
    if rem:
        result["rem_avg_hours"] = round(mean(rem), 1)
    if core:
        result["core_avg_hours"] = round(mean(core), 1)
    return result


def _parse_sleep_unaggregated(data_points: list[dict]) -> dict:
    """Parse unaggregated sleep data (one entry per phase segment)."""
    phases: dict[str, list[float]] = {}
    for point in data_points:
        qty = point.get("qty", 0)
        if not qty:
            continue
        phase = str(point.get("value", "")).lower().replace(" ", "")
        if phase in ("deep", "rem", "core"):
            phases.setdefault(phase, []).append(float(qty))
        elif phase in ("asleep",):
            phases.setdefault("total", []).append(float(qty))
        elif phase in ("inbed",):
            phases.setdefault("in_bed", []).append(float(qty))

    result: dict = {}
    for key, values in phases.items():
        if values:
            result[f"{key}_avg_hours"] = round(mean(values), 1)
            result[f"{key}_sum_hours"] = round(sum(values), 1)
    return result


def _extract_qty(value) -> float | None:
    """Extract a numeric quantity from either a raw number or {"qty": N, "units": "..."}."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        q = value.get("qty")
        if q is not None:
            return float(q)
    return None


def _parse_workouts(raw_workouts: list[dict]) -> list[dict]:
    """Normalize workout entries (supports v1 and v2 formats)."""
    workouts = []
    for w in raw_workouts:
        duration_sec = float(w.get("duration", 0))
        duration_min = round(duration_sec / 60, 1)

        # Active energy: v2 = {"qty": N, "units": "..."}, v1 = {"qty": N, "units": "..."}
        active_energy = (
            _extract_qty(w.get("activeEnergyBurned"))
            or _extract_qty(w.get("activeEnergy"))
            or 0
        )

        workout: dict = {
            "name": w.get("name", "Unknown"),
            "start": w.get("start", ""),
            "duration_min": duration_min,
            "active_energy_kcal": round(active_energy, 1),
        }

        # Distance: v2 = {"qty": N, "units": "km"}, v1 = {"qty": N, "units": "km"}
        dist = _extract_qty(w.get("distance"))
        if dist:
            workout["distance_km"] = round(dist, 2)

        # Heart rate: v2 = {"min": {"qty": N}, "avg": {"qty": N}, "max": {"qty": N}}
        hr = w.get("heartRate")
        if isinstance(hr, dict):
            if hr.get("avg"):
                workout["hr_avg"] = round(_extract_qty(hr["avg"]) or 0)
            if hr.get("max"):
                workout["hr_max"] = round(_extract_qty(hr["max"]) or 0)
        else:
            # v1 fallback: avgHeartRate / maxHeartRate as {"qty": N}
            avg_hr = _extract_qty(w.get("avgHeartRate"))
            max_hr = _extract_qty(w.get("maxHeartRate"))
            if avg_hr:
                workout["hr_avg"] = round(avg_hr)
            if max_hr:
                workout["hr_max"] = round(max_hr)

        workouts.append(workout)

    return workouts


def parse_health_data(payload: dict) -> dict:
    """
    Main entry point. Returns:
    {
        "period": {"start": "...", "end": "..."},
        "metrics": {
            "steps": {"avg": ..., "total": ..., ...},
            "resting_heart_rate": {"avg": ..., ...},
            "sleep": {"total_avg_hours": ..., "deep_avg_hours": ..., ...},
            ...
        },
        "workouts": [...]
    }
    """
    # Health Auto Export wraps in {"data": ...}
    data = payload.get("data", payload)
    raw_metrics = data.get("metrics", [])
    raw_workouts = data.get("workouts", [])

    metrics: dict[str, dict] = {}
    all_dates: list[str] = []

    for metric in raw_metrics:
        name = metric.get("name", "")
        internal_key = METRIC_MAP.get(name)
        if not internal_key:
            continue

        data_points = metric.get("data", [])
        logger.info("Parsing metric %s (%d data points)", name, len(data_points))

        # Collect dates for period detection
        for dp in data_points:
            if dp.get("date"):
                all_dates.append(dp["date"])

        if internal_key == "sleep":
            metrics["sleep"] = _parse_sleep(data_points)
        else:
            values = _safe_values(data_points)
            metrics[internal_key] = _aggregate_simple(values)

    workouts = _parse_workouts(raw_workouts)

    # Determine period from data dates
    period = {}
    if all_dates:
        sorted_dates = sorted(all_dates)
        period = {"start": sorted_dates[0], "end": sorted_dates[-1]}

    return {
        "period": period,
        "metrics": metrics,
        "workouts": workouts,
    }
