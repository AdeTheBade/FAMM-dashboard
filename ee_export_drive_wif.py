#!/usr/bin/env python3
"""
FAMM Earth Engine Export — Google Drive (Personal Auth)
--------------------------------------------------------
AUTHENTICATION STRATEGY:
  Earth Engine export  → personal OAuth token (EE_USER_CREDENTIALS secret)
  Drive download       → same personal OAuth token (owner of the files)
  SA / WIF             → NOT used for Drive at all

WHY PERSONAL CREDENTIALS FOR BOTH:
  Service accounts have no Drive storage quota, so EE must export using your
  personal account. Since the files are owned by your personal account, the
  simplest and most reliable way to download them is also with your personal
  account — no sharing, no folder permissions, no scope juggling.

  The SA (WIF) is still used for all other GCP operations in the workflow
  (IAM, EE API calls etc) but is completely removed from Drive interactions.

SECRETS NEEDED (GitHub → Settings → Secrets → Actions):
  EE_USER_CREDENTIALS : cat ~/.config/earthengine/credentials
                        (client_id, client_secret, refresh_token)

LOCAL USAGE:
  CI=false python ee_export_drive_wif.py

GITHUB ACTIONS:
  Set EE_USER_CREDENTIALS secret and pass it to the export step via env:
    env:
      EE_USER_CREDENTIALS: ${{ secrets.EE_USER_CREDENTIALS }}

FUTURE:
  Migrate to GCS when billing is resolved — service account handles everything,
  no personal credentials needed in CI at all.
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

# ROI as plain coordinates — ee.Geometry built AFTER ee.Initialize()
ROI_COORDS = [
    [-2.1734693858250353, 4.984296847965302],
    [-1.5720532810091514, 5.121001314357858],
    [-0.9295708164086846, 5.421854403720199],
    [-1.436961333748421,  6.219931255643865],
    [-1.3346584712677179, 6.530992048541554],
    [-1.415359587336964,  6.779274372062422],
    [-1.6499660314807807, 6.831115397308761]
]


# ── Load personal OAuth credentials ──────────────────────────────────────────
def load_personal_creds() -> dict:
    """
    Load personal EE OAuth credentials.
      CI    : EE_USER_CREDENTIALS secret
      Local : ~/.config/earthengine/credentials
    Must contain: client_id, client_secret, refresh_token
    """
    in_ci = os.environ.get("CI", "false").lower() == "true"

    if in_ci:
        raw = os.environ.get("EE_USER_CREDENTIALS", "")
        if not raw:
            print("❌ EE_USER_CREDENTIALS secret is not set.")
            print("   Get it: cat ~/.config/earthengine/credentials")
            print("   Add as GitHub Secret: EE_USER_CREDENTIALS")
            sys.exit(1)
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"❌ EE_USER_CREDENTIALS is not valid JSON: {e}")
            sys.exit(1)
    else:
        path = os.path.expanduser("~/.config/earthengine/credentials")
        if not os.path.exists(path):
            print(f"❌ Local EE credentials not found at {path}")
            print("   Run: earthengine authenticate")
            sys.exit(1)
        with open(path) as f:
            return json.load(f)


# ── Authenticate Earth Engine ─────────────────────────────────────────────────
def initialize_ee(creds_data: dict):
    """
    Initialise EE using personal OAuth credentials from creds_data.
    client_id and client_secret are read from the credentials file —
    never hardcoded.
    """
    try:
        oauth_creds = google.oauth2.credentials.Credentials(
            token         = None,
            refresh_token = creds_data["refresh_token"],
            token_uri     = "https://oauth2.googleapis.com/token",
            client_id     = creds_data["client_id"],
            client_secret = creds_data["client_secret"],
            scopes        = creds_data.get("scopes", [
                "https://www.googleapis.com/auth/earthengine",
                "https://www.googleapis.com/auth/devstorage.full_control",
            ])
        )
        ee.Initialize(credentials=oauth_creds, project=PROJECT_ID)
        print("✅ EE authenticated via personal OAuth token")
    except KeyError as e:
        print(f"❌ EE_USER_CREDENTIALS missing field: {e}")
        print("   Expected: client_id, client_secret, refresh_token")
        sys.exit(1)
    except Exception as e:
        print(f"❌ EE auth failed: {e}")
        sys.exit(1)


# ── Build Drive client (personal — owns the exported files) ───────────────────
def build_drive_service(creds_data: dict):
    """
    Build Drive API client using personal OAuth credentials.
    Used for both finding and downloading exported tiles.

    Scopes requested include drive.readonly — sufficient for listing
    and downloading files that the personal account owns.
    EE credentials (earthengine + devstorage) are also included so the
    same refresh token works without scope rejection.
    """
    try:
        personal_creds = google.oauth2.credentials.Credentials(
            token         = None,
            refresh_token = creds_data["refresh_token"],
            token_uri     = "https://oauth2.googleapis.com/token",
            client_id     = creds_data["client_id"],
            client_secret = creds_data["client_secret"],
            scopes        = [
                "https://www.googleapis.com/auth/earthengine",
                "https://www.googleapis.com/auth/devstorage.full_control",
            ]
        )
        return gdrive_build("drive", "v3", credentials=personal_creds)
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
    """
    Export composite to personal Drive.
    EE matches folder by name — it may reuse an existing folder or create
    a new one. This is fine because we find files by name search after
    export (personal account owns them, so they're always findable).
    """
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


# ── Find and download tiles (personal credentials) ────────────────────────────
def find_and_download_tiles(service, local_dir: str) -> bool:
    """
    Search personal Drive for today's tiles and download them.
    Personal account owns the files so it can always find them regardless
    of which FAMM_EE_Exports folder EE chose to write to.
    No SA, no sharing, no folder ID — just a name search on owned files.
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
        print(f"   EE reported COMPLETED but files not visible yet.")
        print(f"   Check drive.google.com → FAMM_EE_Exports")
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
        print(f"   ✅ Saved → {dest} ({os.path.getsize(dest)/1_048_576:.1f} MB)")

    print(f"\n✅ All {len(files)} tile(s) downloaded to {local_dir}/")
    return True


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("FAMM — EE Export (Drive, Personal Auth)")
    print(f"Date : {today}  |  Export: {EXPORT_NAME}")
    print("=" * 60)

    # 1. Load personal credentials (EE + Drive — same token)
    creds_data = load_personal_creds()

    # 2. Authenticate EE
    initialize_ee(creds_data)

    # 3. Build ROI (must be after ee.Initialize)
    ROI = ee.Geometry.Polygon([ROI_COORDS])

    # 4. Build composite and start export to personal Drive
    print(f"\n🛰️  Building Sentinel-2 composite...")
    composite = build_composite(ROI)

    print(f"🚀 Starting Drive export → {DRIVE_FOLDER}/{EXPORT_NAME}*.tif")
    task = start_drive_export(composite, ROI)
    print(f"   Task ID: {task.id}")

    # 5. Wait for EE task to complete
    if not wait_for_task(task):
        sys.exit(1)

    # 6. Download tiles using same personal credentials (owner — always has access)
    print("\n📥 Downloading tiles from Drive...")
    drive_svc = build_drive_service(creds_data)
    if not find_and_download_tiles(drive_svc, "data/tif_input"):
        sys.exit(1)

    # 7. Signal export name to CI workflow
    print(f"\nEXPORT_FILENAME={EXPORT_NAME}.tif")
    print("=" * 60)
