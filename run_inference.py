#!/usr/bin/env python3
"""
FAMM Inference Runner — CI-Safe Wrapper
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import torch
import torch.nn as nn
import torch.nn.functional as F
from shapely.geometry import Point
from torchvision import models
from tqdm import tqdm

try:
    import rasterio
except ImportError:
    print("❌ rasterio not installed. Run: pip install rasterio")
    sys.exit(1)

# ── Paths ─────────────────────────────────────────────────────────────────────
CHECKPOINT_PATH = "models/mobilenetv3_best.pth"
INPUT_DIR       = "data/tif_input"
OUTPUT_PATH     = "asm_monitoring_results.geojson"   # validator picks this up
ADM1_PATH       = "deployment/geoBoundaries-GHA-ADM1.geojson"
ADM2_PATH       = "deployment/geoBoundaries-GHA-ADM2.geojson"

# ── Model settings (must match Rosemary's training config) ────────────────────
MODEL_SIZE = 224
STRIDE     = 224
NUM_BANDS  = 10
THRESHOLD  = 0.3   # Rosemary's inference threshold — validator re-maps alert levels

# ── Device: always CPU in CI (no MPS/CUDA on GitHub runners) ─────────────────
device = torch.device("cpu")
print(f"🖥️  Running on: {device}")


# ── Load model ────────────────────────────────────────────────────────────────
def load_model(checkpoint_path: str):
    if not Path(checkpoint_path).exists():
        print(f"❌ Model checkpoint not found: {checkpoint_path}")
        sys.exit(1)

    model = models.mobilenet_v3_large(weights=None)
    # Match Rosemary's 10-band first conv modification
    model.features[0][0] = nn.Conv2d(
        NUM_BANDS, 16, kernel_size=3, stride=2, padding=1, bias=False
    )
    model.classifier[3] = nn.Linear(model.classifier[3].in_features, 2)

    checkpoint = torch.load(checkpoint_path, map_location=device)
    model.load_state_dict(checkpoint["model_state_dict"])
    model.to(device).eval()
    print(f"✅ Model loaded from {checkpoint_path}")
    return model


# ── Boundary files ────────────────────────────────────────────────────────────
ghana_regions   = gpd.read_file(ADM1_PATH) if Path(ADM1_PATH).exists() else None
ghana_districts = gpd.read_file(ADM2_PATH) if Path(ADM2_PATH).exists() else None

if ghana_regions is None:
    print(f"⚠️  ADM1 boundary file not found: {ADM1_PATH}  — region will be 'Unknown'")
if ghana_districts is None:
    print(f"⚠️  ADM2 boundary file not found: {ADM2_PATH} — district will be 'Unknown'")


def get_location_details(longitude: float, latitude: float) -> tuple[str, str]:
    """Spatial join to get district (ADM2) and region (ADM1) for a point."""
    point    = Point(longitude, latitude)
    district = "Unknown"
    region   = "Unknown"

    if ghana_regions is not None:
        match = ghana_regions[ghana_regions.contains(point)]
        if not match.empty:
            region = match.iloc[0].get("shapeName", "Unknown")

    if ghana_districts is not None:
        match = ghana_districts[ghana_districts.contains(point)]
        if not match.empty:
            district = match.iloc[0].get("shapeName", "Unknown")

    return district, region


# ── Inference (mirrors Rosemary's process_shard exactly) ─────────────────────
def process_shard(model, tif_path: str) -> list[dict]:
    features = []
    tile_id  = os.path.basename(tif_path)
    area_ha  = (MODEL_SIZE * 10 * MODEL_SIZE * 10) / 10000.0

    try:
        with rasterio.open(tif_path) as src:
            image     = src.read()
            transform = src.transform
            _, H, W   = image.shape
    except Exception as e:
        print(f"⚠️  Could not read {tif_path}: {e} — skipping")
        return features

    for i in range(0, H - MODEL_SIZE + 1, STRIDE):
        for j in range(0, W - MODEL_SIZE + 1, STRIDE):
            patch = image[:, i:i + MODEL_SIZE, j:j + MODEL_SIZE]
            patch_tensor = torch.tensor(patch, dtype=torch.float32)

            if patch_tensor.max() > 1:
                patch_tensor /= 10000.0

            patch_tensor = patch_tensor.unsqueeze(0).to(device)
            with torch.no_grad():
                output     = model(patch_tensor)
                probs      = F.softmax(output, dim=1)
                confidence = probs[0, 1].item()

            if confidence >= THRESHOLD:
                # Rosemary's alert thresholds (validator will re-map to dashboard thresholds)
                alert = "HIGH" if confidence > 0.8 else "MEDIUM" if confidence > 0.5 else "LOW"
                longitude, latitude = transform * (j + (MODEL_SIZE // 2), i + (MODEL_SIZE // 2))
                district, region    = get_location_details(longitude, latitude)

                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(longitude), float(latitude)]
                    },
                    "properties": {
                        "confidence":  round(float(confidence), 4),
                        "district":    district,
                        "region":      region,
                        "date":        datetime.now().strftime("%Y-%m-%d"),
                        "area_ha":     round(area_ha, 2),
                        "tile_id":     tile_id,
                        "alert_level": alert
                    }
                })

    return features


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("FAMM Inference Runner")
    print("=" * 60)

    tif_files = list(Path(INPUT_DIR).glob("*.tif"))

    if not tif_files:
        print(f"\n⚠️  No .tif files found in {INPUT_DIR}/")
        print("   This can happen if the EE export produced no imagery")
        print("   (e.g. heavy cloud cover over the ROI this week).")
        print("   Keeping existing latest_detections.geojson unchanged.")
        # Exit 0 — not a failure, just no new data
        sys.exit(0)

    print(f"\n📂 Found {len(tif_files)} .tif file(s) to process")
    model        = load_model(CHECKPOINT_PATH)
    all_features = []

    for tif_path in tqdm(tif_files, desc="Processing tiles"):
        all_features.extend(process_shard(model, str(tif_path)))

    print(f"\n🔍 Detections above threshold ({THRESHOLD}): {len(all_features)}")

    output = {"type": "FeatureCollection", "features": all_features}
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"✅ Raw results saved to: {OUTPUT_PATH}")
    print("   → Next: run validate_rosemary_geojson.py to clean & move to dashboard path")
    print("=" * 60)
