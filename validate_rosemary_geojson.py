#!/usr/bin/env python3
"""
FAMM GeoJSON Validator & Cleaner
----------------------------------
Bridges Rosemary's inference output → dashboard-ready GeoJSON.

ALERT THRESHOLD POLICY:
  Rosemary's thresholds are used exactly as she calibrated them:
    HIGH   : confidence > 0.8
    MEDIUM : confidence > 0.5
    LOW    : confidence <= 0.5
  These are preserved from her deployment_inference.py and are NOT
  remapped. She calibrated them based on the model's precision/recall
  behaviour and they should be treated as authoritative.

FIXES APPLIED:
  1. Title-cases district names
  2. Ensures region has "Region" suffix if missing
  3. Fallback region from district substrings (safety net)
  4. Validates required fields present
  5. Validates date format is YYYY-MM-DD
  6. Drops features with invalid geometry

Usage:
  python validate_rosemary_geojson.py                           # defaults
  python validate_rosemary_geojson.py input.geojson output.geojson
"""

import json
import sys
from datetime import datetime
from pathlib import Path

REQUIRED_FIELDS = [
    "confidence", "district", "region",
    "date", "area_ha", "tile_id", "alert_level"
]

DISTRICT_TO_REGION = {
    "Kwabre":       "Ashanti Region",
    "Obuasi":       "Ashanti Region",
    "Amansie":      "Ashanti Region",
    "Adansi":       "Ashanti Region",
    "Bekwai":       "Ashanti Region",
    "Tarkwa":       "Western Region",
    "Prestea":      "Western Region",
    "Wassa":        "Western Region",
    "Bibiani":      "Western Region",
    "Mfantseman":   "Central Region",
    "Komenda":      "Central Region",
    "Cape Coast":   "Central Region",
    "Abura":        "Central Region",
    "Atiwa":        "Eastern Region",
    "Birim":        "Eastern Region",
    "Denkyembour":  "Eastern Region",
    "West Akim":    "Eastern Region",
    "East Akim":    "Eastern Region",
}


def remap_alert_level(confidence: float) -> str:
    """Apply Rosemary's calibrated thresholds (HIGH>0.8, MEDIUM>0.5)."""
    if confidence > 0.8:
        return "HIGH"
    elif confidence > 0.5:
        return "MEDIUM"
    return "LOW"


def title_case_district(name: str) -> str:
    def cap(part):
        return part[0].upper() + part[1:] if part else part
    return " ".join(
        "-".join(cap(p) for p in word.split("-"))
        for word in name.split(" ")
    )


def fallback_region(district: str):
    for key, region in DISTRICT_TO_REGION.items():
        if key.lower() in district.lower():
            return region
    return None


def validate_date(s: str) -> bool:
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def validate_and_clean(input_file: str, output_file: str) -> bool:
    print("=" * 60)
    print("FAMM GeoJSON Validator")
    print(f"  Input : {input_file}")
    print(f"  Output: {output_file}")
    print("=" * 60)

    try:
        with open(input_file) as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Not found: {input_file}")
        return False
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON: {e}")
        return False

    raw = data.get("features", [])
    print(f"\nLoaded {len(raw)} raw features")

    cleaned = []
    skipped = []
    fixes   = {"alert_remapped": 0, "district_cased": 0,
               "region_suffix_added": 0, "region_fallback": 0}

    for i, feat in enumerate(raw):
        props = feat.get("properties", {})
        geom  = feat.get("geometry", {})

        missing = [f for f in REQUIRED_FIELDS if f not in props]
        if missing:
            skipped.append((i, f"Missing: {missing}"))
            continue

        coords = geom.get("coordinates", [])
        if len(coords) < 2:
            skipped.append((i, "Invalid coordinates"))
            continue

        confidence = float(props["confidence"])
        district   = str(props["district"]).strip()
        region     = str(props["region"]).strip()
        date_str   = str(props["date"]).strip()
        area_ha    = float(props["area_ha"])
        tile_id    = str(props.get("tile_id", ""))
        alert_in   = str(props.get("alert_level", "")).strip()

        if not validate_date(date_str):
            skipped.append((i, f"Bad date: {date_str!r}"))
            continue

        alert_out = remap_alert_level(confidence)
        if alert_out != alert_in:
            fixes["alert_remapped"] += 1

        district_fixed = title_case_district(district)
        if district_fixed != district:
            fixes["district_cased"] += 1
        district = district_fixed

        if region and "Region" not in region and region not in ("Unknown", ""):
            region += " Region"
            fixes["region_suffix_added"] += 1

        if not region or region in ("Unknown", "Unknown Region", ""):
            fb = fallback_region(district)
            region = fb if fb else "Unknown Region"
            if fb:
                fixes["region_fallback"] += 1

        cleaned.append({
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "confidence":  round(confidence, 4),
                "district":    district,
                "region":      region,
                "date":        date_str,
                "area_ha":     round(area_ha, 2),
                "tile_id":     tile_id,
                "alert_level": alert_out,
            }
        })

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        json.dump({"type": "FeatureCollection", "features": cleaned}, f, indent=2)

    alert_counts  = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
    region_counts = {}
    for feat in cleaned:
        p = feat["properties"]
        alert_counts[p["alert_level"]] = alert_counts.get(p["alert_level"], 0) + 1
        region_counts[p["region"]]     = region_counts.get(p["region"], 0) + 1

    print(f"\nRESULTS")
    print(f"  Raw: {len(raw)}  Cleaned: {len(cleaned)}  Skipped: {len(skipped)}")
    print(f"\n  Alert levels (Rosemary thresholds: HIGH>0.8, MEDIUM>0.5):")
    for lvl in ("HIGH", "MEDIUM", "LOW"):
        print(f"    {lvl}: {alert_counts[lvl]}")
    print(f"\n  Regions:")
    for r, c in sorted(region_counts.items()):
        print(f"    {r}: {c}")
    fixes_applied = {k: v for k, v in fixes.items() if v}
    if fixes_applied:
        print(f"\n  Fixes applied: {fixes_applied}")
    else:
        print("\n  Data was clean - no fixes needed")
    if skipped:
        print(f"\n  Skipped:")
        for idx, reason in skipped:
            print(f"    Feature {idx}: {reason}")
    print(f"\nSaved to: {output_file}")
    print("=" * 60)
    return True


if __name__ == "__main__":
    inp = sys.argv[1] if len(sys.argv) > 1 else "asm_monitoring_results.geojson"
    out = sys.argv[2] if len(sys.argv) > 2 else "data/geojson/latest_detections.geojson"
    sys.exit(0 if validate_and_clean(inp, out) else 1)