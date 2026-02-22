#!/usr/bin/env python3
"""
FAMM Earth Engine Export — Google Drive (Personal Auth)
--------------------------------------------------------
AUTHENTICATION:
  One set of credentials handles everything: EE export + Drive download.
  Stored as EE_USER_CREDENTIALS GitHub Secret.

  The credentials MUST be generated with Drive scope included:

    gcloud auth application-default login \
      --scopes="https://www.googleapis.com/auth/drive,\
https://www.googleapis.com/auth/earthengine,\
https://www.googleapis.com/auth/devstorage.full_control,\
https://www.googleapis.com/auth/cloud-platform,\
openid,\
https://www.googleapis.com/auth/userinfo.email"

  Then: cat ~/.config/gcloud/application_default_credentials.json
  Paste that JSON as EE_USER_CREDENTIALS GitHub Secret.

  DO NOT use ~/.config/earthengine/credentials — those only have
  earthengine + devstorage scopes and will fail for Drive.

LOCAL USAGE:
  CI=false python ee_export_drive_wif.py

FUTURE:
  Migrate to GCS when billing is resolved — eliminates personal credentials
  from CI entirely.
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
SCALE        = 10
BANDS        = ["B2","B3","B4","B5","B6","B7","B8","B8A","B11","B12"]

today       = datetime.date.today()
EXPORT_NAME = f"Ghana_Composite_{today}"

ROI_COORDS = [
    [-2.1734693858250353, 4.984296847965302],
    [-1.5720532810091514, 5.121001314357858],
    [-0.9295708164086846, 5.421854403720199],
    [-1.436961333748421,  6.219931255643865],
    [-1.3346584712677179, 6.530992048541554],
    [-1.415359587336964,  6.779274372062422],
    [-1.6499660314807807, 6.831115397308761]
]

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

    IMPORTANT: must be generated with Drive scope — see module docstring.
    """
    in_ci = os.environ.get("CI", "false").lower() == "true"

    if in_ci:
        raw = os.environ.get("EE_USER_CREDENTIALS", "")
        if not raw:
            print("❌ EE_USER_CREDENTIALS secret is not set.")
            print("   Generate with Drive scope:")
            print('   gcloud auth application-default login \\')
            print('     --scopes="https://www.googleapis.com/auth/drive,\\')
            print('     https://www.googleapis.com/auth/earthengine,\\')
            print('     https://www.googleapis.com/auth/devstorage.full_control,\\')
            print('     https://www.googleapis.com/auth/cloud-platform,\\')
            print('     openid,https://www.googleapis.com/auth/userinfo.email"')
            print('   Then: cat ~/.config/gcloud/application_default_credentials.json')
            sys.exit(1)
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"❌ EE_USER_CREDENTIALS is not valid JSON: {e}")
            sys.exit(1)
    else:
        # Local: use gcloud ADC (has broader scopes than EE credentials)
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
    """Build a google.oauth2.credentials.Credentials object from creds_data."""
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
            quota_project_id = PROJECT_ID   # ← add this line
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


# ── Build composite ───────────────────────────────────────────────────────────
def build_composite(roi):
    end = ee.Date(str(today))
    s7  = ee.Date(str(today - datetime.timedelta(days=7)))
    s14 = ee.Date(str(today - datetime.timedelta(days=14)))
    s30 = ee.Date(str(today - datetime.timedelta(days=30)))

    def col(start):
        return (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                  .filterBounds(roi)
                  .filterDate(start, end)
                  .map(lambda img: mask_s2(img, roi))
                  .select(BANDS))

    composite = col(s7).median().unmask(col(s14).median()).unmask(col(s30).median())
    return composite.unmask(0).clip(roi)


# ── Start Drive export ────────────────────────────────────────────────────────
def start_drive_export(composite, roi):
    task = ee.batch.Export.image.toDrive(
        image          = composite,
        description    = EXPORT_NAME,
        folder         = DRIVE_FOLDER,
        fileNamePrefix = EXPORT_NAME,
        region         = roi,
        scale          = SCALE,
        maxPixels      = 1e13,
        fileFormat     = "GeoTIFF"
    )
    task.start()
    return task


# ── Poll until EE task completes ─────────────────────────────────────────────
def wait_for_task(task, max_minutes=55):
    print(f"⏳ Polling EE task every 60 s (max {max_minutes} min)...")
    for i in range(max_minutes):
        time.sleep(60)
        state = task.status().get("state", "UNKNOWN")
        print(f"   [{i+1:02d}/{max_minutes}] {state}")
        if state == "COMPLETED":
            print(f"✅ Export complete → Drive/{DRIVE_FOLDER}/{EXPORT_NAME}*.tif")
            return True
        if state in ("FAILED", "CANCELLED"):
            err = task.status().get("error_message", "no details")
            print(f"❌ Task {state}: {err}")
            return False
    print(f"❌ Timed out after {max_minutes} minutes.")
    return False


# ── Find and download tiles ───────────────────────────────────────────────────
def find_and_download_tiles(service, local_dir: str) -> bool:
    """
    Search personal Drive for today's tiles and download them.
    Personal account owns the files — always findable by name search
    regardless of which FAMM_EE_Exports folder EE chose.
    """
    from googleapiclient.http import MediaIoBaseDownload

    print(f"🔍 Searching Drive for: {EXPORT_NAME}*.tif")

    results = service.files().list(
        q        = f"name contains '{EXPORT_NAME}' and trashed=false",
        fields   = "files(id,name,size)",
        orderBy  = "name",
        pageSize = 20
    ).execute()

    files = results.get("files", [])

    if not files:
        print(f"❌ No tiles found matching '{EXPORT_NAME}' in Drive.")
        print(f"   Check drive.google.com → FAMM_EE_Exports for today's tiles.")
        return False

    os.makedirs(local_dir, exist_ok=True)
    total_mb = sum(int(f.get("size", 0)) for f in files) / 1_048_576
    print(f"   Found {len(files)} tile(s) (~{total_mb:.1f} MB total)")

    for i, f in enumerate(files, 1):
        dest    = os.path.join(local_dir, f["name"])
        size_mb = int(f.get("size", 0)) / 1_048_576

        print(f"\n⬇️  [{i}/{len(files)}] {f['name']} ({size_mb:.1f} MB)")
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

    print(f"\n✅ All {len(files)} tile(s) downloaded to {local_dir}/")
    return True


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("FAMM — EE Export (Drive, Personal Auth)")
    print(f"Date : {today}  |  Export: {EXPORT_NAME}")
    print("=" * 60)

    # 1. Load credentials (must have drive + earthengine scopes)
    creds_data = load_creds()

    # 2. Authenticate EE
    initialize_ee(creds_data)

    # 3. Build ROI (must be after ee.Initialize)
    ROI = ee.Geometry.Polygon([ROI_COORDS])

    # 4. Build composite and export to personal Drive
    print(f"\n🛰️  Building Sentinel-2 composite...")
    composite = build_composite(ROI)

    print(f"🚀 Starting Drive export → {DRIVE_FOLDER}/{EXPORT_NAME}*.tif")
    task = start_drive_export(composite, ROI)
    print(f"   Task ID: {task.id}")

    # 5. Wait for EE task
    if not wait_for_task(task):
        sys.exit(1)

    # 6. Download tiles using same personal credentials (file owner)
    print("\n📥 Downloading tiles from Drive...")
    drive_svc = build_drive_service(creds_data)
    if not find_and_download_tiles(drive_svc, "data/tif_input"):
        sys.exit(1)

    # 7. Signal export name to CI workflow
    print(f"\nEXPORT_FILENAME={EXPORT_NAME}.tif")
    print("=" * 60)
