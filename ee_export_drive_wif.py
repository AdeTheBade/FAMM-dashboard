#!/usr/bin/env python3
"""
FAMM Earth Engine Export — Google Drive (Personal Auth, All Ghana Regions)
--------------------------------------------------------------------------
ROI UPDATE (Rosemary, Feb 2026):
  Now exports a separate Sentinel-2 composite for every administrative region
  in Ghana (16 regions) using FAO/GAUL/2015/level1 boundaries — replacing
  the previous fixed 7-point polygon covering only the southern mining belt.

  Each region gets its own GeoTIFF exported to Drive. All exports are started
  in parallel (EE handles async processing) then the script waits for all of
  them to complete before downloading.

  WARNING: Full Ghana at 16 regions takes several hours to export and download.
  Rosemary noted ~1 hour per region for download alone. Start early.
  Ensure sufficient Drive storage (~200-400 MB per region GeoTIFF).

AUTHENTICATION:
  One set of credentials handles everything: EE export + Drive download.
  Stored as EE_USER_CREDENTIALS GitHub Secret.
  Must be generated with Drive scope:

    gcloud auth application-default login \
      --scopes="https://www.googleapis.com/auth/drive,\
https://www.googleapis.com/auth/earthengine,\
https://www.googleapis.com/auth/devstorage.full_control,\
https://www.googleapis.com/auth/cloud-platform,\
openid,\
https://www.googleapis.com/auth/userinfo.email"

  Then: cat ~/.config/gcloud/application_default_credentials.json
  Paste as EE_USER_CREDENTIALS GitHub Secret.

LOCAL USAGE:
  CI=false python ee_export_drive_wif.py

FUTURE:
  Migrate to GCS when billing is resolved — eliminates personal credentials
  from CI entirely and handles large multi-region exports cleanly.
"""

import ee
import datetime
import io
import json
import os
import sys
import time

import google.oauth2.credentials
from googleapiclient.discovery import build as gdrive_build

# ── Constants ─────────────────────────────────────────────────────────────────
PROJECT_ID   = "famm-472015"
DRIVE_FOLDER = "FAMM_EE_Exports"
COUNTRY      = "Ghana"
SCALE        = 10
BANDS        = ["B2","B3","B4","B5","B6","B7","B8","B8A","B11","B12"]

today = datetime.date.today()

# Scopes the credentials MUST have been issued with
REQUIRED_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/earthengine",
    "https://www.googleapis.com/auth/devstorage.full_control",
    "https://www.googleapis.com/auth/cloud-platform",
]


# ── Load credentials ──────────────────────────────────────────────────────────
def load_creds() -> dict:
    """
    Load credentials from EE_USER_CREDENTIALS secret (CI) or
    ~/.config/gcloud/application_default_credentials.json (local).
    Must have been generated with Drive scope — see module docstring.
    """
    in_ci = os.environ.get("CI", "false").lower() == "true"

    if in_ci:
        raw = os.environ.get("EE_USER_CREDENTIALS", "")
        if not raw:
            print("❌ EE_USER_CREDENTIALS secret is not set.")
            print("   Generate with Drive scope (see module docstring).")
            sys.exit(1)
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"❌ EE_USER_CREDENTIALS is not valid JSON: {e}")
            sys.exit(1)
    else:
        path = os.path.expanduser(
            "~/.config/gcloud/application_default_credentials.json"
        )
        if not os.path.exists(path):
            print(f"❌ gcloud ADC credentials not found at {path}")
            print("   Run: gcloud auth application-default login --scopes=...")
            sys.exit(1)
        with open(path) as f:
            return json.load(f)


# ── Build OAuth credentials object ────────────────────────────────────────────
def make_oauth_creds(creds_data: dict, scopes: list):
    return google.oauth2.credentials.Credentials(
        token         = None,
        refresh_token = creds_data["refresh_token"],
        token_uri     = "https://oauth2.googleapis.com/token",
        client_id     = creds_data["client_id"],
        client_secret = creds_data["client_secret"],
        scopes        = scopes
    )


# ── Authenticate Earth Engine ─────────────────────────────────────────────────
def initialize_ee(creds_data: dict):
    try:
        oauth_creds = make_oauth_creds(creds_data, [
            "https://www.googleapis.com/auth/earthengine",
            "https://www.googleapis.com/auth/devstorage.full_control",
            "https://www.googleapis.com/auth/cloud-platform",
        ])
        ee.Initialize(credentials=oauth_creds, project=PROJECT_ID)
        print("✅ EE authenticated via personal OAuth token")
    except KeyError as e:
        print(f"❌ Credentials missing field: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ EE auth failed: {e}")
        sys.exit(1)


# ── Build Drive client ────────────────────────────────────────────────────────
def build_drive_service(creds_data: dict):
    try:
        oauth_creds = google.oauth2.credentials.Credentials(
            token            = None,
            refresh_token    = creds_data["refresh_token"],
            token_uri        = "https://oauth2.googleapis.com/token",
            client_id        = creds_data["client_id"],
            client_secret    = creds_data["client_secret"],
            scopes           = ["https://www.googleapis.com/auth/drive"],
            quota_project_id = PROJECT_ID
        )
        return gdrive_build("drive", "v3", credentials=oauth_creds)
    except Exception as e:
        print(f"❌ Drive client build failed: {e}")
        sys.exit(1)


# ── Sentinel-2 cloud masking (Rosemary's logic — unchanged) ──────────────────
def mask_s2(image, roi):
    image_date = image.date()
    cloud_col  = (
        ee.ImageCollection("COPERNICUS/S2_CLOUD_PROBABILITY")
        .filterBounds(roi)
        .filterDate(image_date.advance(-3, "day"), image_date.advance(3, "day"))
        .sort("system:time_start")
    )
    cloud_mask = ee.Image(ee.Algorithms.If(
        cloud_col.size().gt(0),
        ee.Image(cloud_col.first()).select("probability").lt(60),
        ee.Image(1)
    ))
    scaled    = image.divide(10000)
    ndvi_mask = scaled.normalizedDifference(["B8", "B4"]).gte(0.25)
    return (scaled.updateMask(cloud_mask.Or(ndvi_mask))
                  .copyProperties(image, ["system:time_start"]))


# ── Build composite for a single region ──────────────────────────────────────
def build_composite(roi):
    end_date = ee.Date(str(today))
    s7  = ee.Date(str(today - datetime.timedelta(days=7)))
    s14 = ee.Date(str(today - datetime.timedelta(days=14)))
    s30 = ee.Date(str(today - datetime.timedelta(days=30)))

    def col(start):
        return (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                  .filterBounds(roi)
                  .filterDate(start, end_date)
                  .map(lambda img: mask_s2(img, roi))
                  .select(BANDS))

    composite = col(s7).median().unmask(col(s14).median()).unmask(col(s30).median())
    return composite.unmask(0).clip(roi)


# ── Start export for one region ───────────────────────────────────────────────
def start_region_export(composite, roi, export_name: str):
    task = ee.batch.Export.image.toDrive(
        image          = composite,
        description    = export_name,
        folder         = DRIVE_FOLDER,
        fileNamePrefix = export_name,
        region         = roi,
        scale          = SCALE,
        maxPixels      = 1e13,
        fileFormat     = "GeoTIFF"
    )
    task.start()
    return task


# ── Poll all tasks until complete ─────────────────────────────────────────────
def wait_for_all_tasks(tasks: list, max_minutes=300) -> list:
    """
    Poll all region export tasks every 60s until all complete or fail.
    Returns list of export_names that completed successfully.
    max_minutes=300 (5 hours) — full Ghana takes several hours per Rosemary.
    """
    print(f"\n⏳ Polling {len(tasks)} region tasks every 60s (max {max_minutes} min)...")
    pending = {name: task for name, task in tasks}
    completed = []
    failed    = []

    for minute in range(max_minutes):
        time.sleep(60)
        still_running = {}

        for name, task in pending.items():
            state = task.status().get("state", "UNKNOWN")
            if state == "COMPLETED":
                print(f"   ✅ [{minute+1:03d}m] COMPLETED → {name}")
                completed.append(name)
            elif state in ("FAILED", "CANCELLED"):
                err = task.status().get("error_message", "no details")
                print(f"   ❌ [{minute+1:03d}m] {state} → {name}: {err}")
                failed.append(name)
            else:
                still_running[name] = task

        pending = still_running
        if pending:
            print(f"   ⏳ [{minute+1:03d}m] {len(pending)} still running: "
                  f"{', '.join(pending.keys())}")
        else:
            break

    if pending:
        print(f"\n⚠️  Timed out — {len(pending)} tasks still running: {list(pending.keys())}")
        print("   Consider increasing max_minutes or running fewer regions.")

    print(f"\n📊 Export summary: {len(completed)} completed, "
          f"{len(failed)} failed, {len(pending)} timed out")
    return completed


# ── Download tiles for a completed export ─────────────────────────────────────
def download_region_tiles(service, export_name: str, local_dir: str) -> bool:
    from googleapiclient.http import MediaIoBaseDownload

    results = service.files().list(
        q        = f"name contains '{export_name}' and trashed=false",
        fields   = "files(id,name,size)",
        orderBy  = "name",
        pageSize = 20
    ).execute()

    files = results.get("files", [])
    # Filter to only .tif files (exclude metadata GeoJSON if present)
    files = [f for f in files if f["name"].endswith(".tif")]

    if not files:
        print(f"   ⚠️  No .tif tiles found for {export_name}")
        return False

    os.makedirs(local_dir, exist_ok=True)
    total_mb = sum(int(f.get("size", 0)) for f in files) / 1_048_576
    print(f"   Found {len(files)} tile(s) (~{total_mb:.1f} MB)")

    for i, f in enumerate(files, 1):
        dest    = os.path.join(local_dir, f["name"])
        size_mb = int(f.get("size", 0)) / 1_048_576
        print(f"   ⬇️  [{i}/{len(files)}] {f['name']} ({size_mb:.1f} MB)")
        fh = io.FileIO(dest, "wb")
        dl = MediaIoBaseDownload(
            fh, service.files().get_media(fileId=f["id"]),
            chunksize=50 * 1024 * 1024
        )
        done = False
        while not done:
            st, done = dl.next_chunk()
            if st:
                print(f"   {int(st.progress() * 100)}%", end="\r")
        fh.close()
        print(f"   ✅ Saved ({os.path.getsize(dest)/1_048_576:.1f} MB)")

    return True


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("FAMM — EE Export (All Ghana Regions, Personal Auth)")
    print(f"Date : {today}")
    print("=" * 60)

    # 1. Load credentials
    creds_data = load_creds()

    # 2. Authenticate EE
    initialize_ee(creds_data)

    # 3. Load all Ghana regions from FAO GAUL (same source as Rosemary)
    print(f"\n🗺️  Loading Ghana administrative regions...")
    regions     = (ee.FeatureCollection("FAO/GAUL/2015/level1")
                     .filter(ee.Filter.eq("ADM0_NAME", COUNTRY)))
    region_list = regions.toList(regions.size())
    num_regions = regions.size().getInfo()
    print(f"   Found {num_regions} regions")

    # 4. Start all region exports in parallel
    print(f"\n🚀 Starting exports for all {num_regions} regions...")
    tasks = []  # list of (export_name, task)

    for i in range(num_regions):
        region_feature = ee.Feature(region_list.get(i))
        region_name    = region_feature.get("ADM1_NAME").getInfo()
        roi            = region_feature.geometry()
        export_name    = f"{COUNTRY}_{region_name}_Composite_{today}"

        print(f"   [{i+1:02d}/{num_regions}] Starting: {region_name}")
        composite = build_composite(roi)
        task      = start_region_export(composite, roi, export_name)
        tasks.append((export_name, task))
        print(f"            Task ID: {task.id}")

    # 5. Wait for all tasks to complete
    # NOTE: Full Ghana takes several hours — Rosemary noted ~1hr per region
    completed_exports = wait_for_all_tasks(tasks, max_minutes=300)

    if not completed_exports:
        print("❌ No exports completed successfully.")
        sys.exit(1)

    # 6. Download all completed tiles
    print(f"\n📥 Downloading tiles for {len(completed_exports)} completed region(s)...")
    drive_svc = build_drive_service(creds_data)
    local_dir = "data/tif_input"
    os.makedirs(local_dir, exist_ok=True)

    download_failures = []
    for export_name in completed_exports:
        region_name = export_name.split("_")[1]  # extract from Ghana_REGION_Composite_...
        print(f"\n📂 {region_name}")
        if not download_region_tiles(drive_svc, export_name, local_dir):
            download_failures.append(export_name)

    if download_failures:
        print(f"\n⚠️  Download failed for: {download_failures}")
        print("   Continuing — inference will run on whatever was downloaded.")

    # 7. Summary
    tif_count = len([f for f in os.listdir(local_dir) if f.endswith(".tif")])
    print(f"\n✅ Download complete — {tif_count} .tif tile(s) in {local_dir}/")
    print(f"EXPORT_DATE={today}")
    print("=" * 60)
