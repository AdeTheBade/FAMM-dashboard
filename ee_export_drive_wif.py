#!/usr/bin/env python3
"""
FAMM Earth Engine Export — Google Drive (Hybrid Auth)
------------------------------------------------------
AUTHENTICATION STRATEGY:
  Earth Engine export  → your personal OAuth token (stored as GH secret)
  Drive download       → service account via WIF (already has folder Editor access)

WHY HYBRID:
  Service accounts have no Google Drive storage quota. Earth Engine cannot
  export to a Drive that has no storage quota. Your personal Google account
  has 15 GB free Drive storage, so EE can export there successfully.

  The service account CAN read/download files from a shared Drive folder
  (quota is only needed for writing, not reading). So the service account
  handles the download step cleanly via WIF.

PERSONAL TOKEN SETUP (one-time, in GitHub Secrets):
  1. Run locally: earthengine authenticate
  2. Find the token file: ~/.config/earthengine/credentials
  3. Copy the entire JSON content
  4. Add as GitHub Secret: EE_USER_CREDENTIALS

LOCAL USAGE:
  CI=false python ee_export_drive_wif.py
  (uses your local earthengine credentials automatically)

GITHUB ACTIONS:
  Reads EE_USER_CREDENTIALS secret for EE export.
  Reads GOOGLE_APPLICATION_CREDENTIALS (set by WIF action) for Drive download.

YOUR DRIVE FOLDER:
  https://drive.google.com/drive/folders/1aREXMO2k3BEg7aZWMcD6ALjqa2bZnD_7
  Folder ID: 1aREXMO2k3BEg7aZWMcD6ALjqa2bZnD_7
"""

import ee
import datetime
import io
import json
import os
import sys
import time
import tempfile

# ── Constants ─────────────────────────────────────────────────────────────────
PROJECT_ID      = "famm-472015"
DRIVE_FOLDER    = "FAMM_EE_Exports"
DRIVE_FOLDER_ID = "1aREXMO2k3BEg7aZWMcD6ALjqa2bZnD_7"  # your personal Drive folder — EE writes HERE
SCALE           = 10
BANDS           = ["B2","B3","B4","B5","B6","B7","B8","B8A","B11","B12"]

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


# ── Step 1: Authenticate Earth Engine ────────────────────────────────────────
def initialize_ee_for_export():
    """
    Authenticate EE using YOUR personal OAuth credentials.

    Local : reads from ~/.config/earthengine/credentials automatically
    CI    : reads EE_USER_CREDENTIALS secret, writes to a temp file,
            then initialises EE from that file.

    Why personal credentials and not the service account?
    Because Earth Engine exports go to the authenticated user's Google Drive,
    and service accounts have no Drive storage quota — the export would fail
    with "Service accounts do not have storage quota."
    """
    in_ci = os.environ.get("CI", "false").lower() == "true"

    if in_ci:
        ee_creds_json = os.environ.get("EE_USER_CREDENTIALS", "")
        if not ee_creds_json:
            print("❌ EE_USER_CREDENTIALS secret is not set.")
            print("   Add your personal EE credentials to GitHub Secrets.")
            print("   Run `cat ~/.config/earthengine/credentials` locally to get the value.")
            sys.exit(1)

        try:
            # Write credentials to a temp file — EE SDK reads from file path
            creds_data = json.loads(ee_creds_json)
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            )
            json.dump(creds_data, tmp)
            tmp.close()

            # Point EE to the temp credentials file
            os.environ["EARTHENGINE_TOKEN"] = tmp.name
            ee.Initialize(
                credentials=ee.ServiceAccountCredentials("", key_file=None),
                project=PROJECT_ID
            )
        except Exception:
            # Fallback: use the standard credentials path approach
            try:
                import google.oauth2.credentials
                creds_data = json.loads(ee_creds_json)
                creds = ee.ServiceAccountCredentials.__new__(ee.ServiceAccountCredentials)
                ee.Initialize(project=PROJECT_ID)
            except Exception:
                pass

        # Most reliable approach for personal OAuth token in CI:
        try:
            creds_data = json.loads(ee_creds_json)
            # Write to the standard EE credentials location
            ee_dir = os.path.expanduser("~/.config/earthengine")
            os.makedirs(ee_dir, exist_ok=True)
            creds_path = os.path.join(ee_dir, "credentials")
            with open(creds_path, "w") as f:
                json.dump(creds_data, f)
            ee.Initialize(project=PROJECT_ID)
            print("✅ EE authenticated via personal OAuth token (from secret)")
        except Exception as e:
            print(f"❌ EE personal auth failed: {e}")
            sys.exit(1)
    else:
        # Local: EE reads ~/.config/earthengine/credentials automatically
        ee.Initialize(project=PROJECT_ID)
        print("✅ EE authenticated via local personal credentials")


# ── Step 2: Build Drive client for DOWNLOAD (uses WIF service account) ────────
def build_drive_service_for_download():
    """
    Build Drive API client for downloading the exported .tif.

    In CI  : uses WIF credentials (GOOGLE_APPLICATION_CREDENTIALS set by
              google-github-actions/auth@v2). The service account has Editor
              access to your FAMM_EE_Exports folder so it can read files.
    Locally: uses your ADC credentials (gcloud auth application-default login).

    Why service account for download and not for export?
    Reading/downloading from a shared folder doesn't require storage quota —
    only writing does. So the service account can download freely.
    """
    try:
        from googleapiclient.discovery import build
        import google.auth

        in_ci = os.environ.get("CI", "false").lower() == "true"

        if in_ci:
            # WIF credentials set by google-github-actions/auth@v2
            creds, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/drive.readonly"]
            )
        else:
            # Local ADC credentials
            creds, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/drive.readonly"]
            )

        return build("drive", "v3", credentials=creds)

    except ImportError:
        print("❌ google-api-python-client not installed.")
        print("   Run: pip install google-api-python-client")
        sys.exit(1)
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
    end  = ee.Date(str(today))
    s7   = ee.Date(str(today - datetime.timedelta(days=7)))
    s14  = ee.Date(str(today - datetime.timedelta(days=14)))
    s30  = ee.Date(str(today - datetime.timedelta(days=30)))

    def col(start):
        return (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                  .filterBounds(roi)
                  .filterDate(start, end)
                  .map(lambda img: mask_s2(img, roi))
                  .select(BANDS))

    composite = col(s7).median().unmask(col(s14).median()).unmask(col(s30).median())
    return composite.unmask(0).clip(roi)


# ── Start Drive export (exports to YOUR personal Drive folder) ────────────────
def start_drive_export(composite, roi):
    """
    Export directly into your specific Drive folder by ID.

    WHY driveFolder instead of folder:
      The 'folder' parameter matches by NAME — if you have multiple folders
      named 'FAMM_EE_Exports', EE picks unpredictably and may create a new one.
      The 'driveFolder' parameter takes a FOLDER ID directly — EE writes into
      exactly that folder, no searching, no creating, no ambiguity.

    The folder ID is your personal FAMM_EE_Exports folder:
      https://drive.google.com/drive/folders/1PjFLEEg5fXIurCa_WHnVfXa5KMvdJ55N
    The service account has Editor access to this folder so it can download
    the tiles after EE finishes writing them.
    """
    task = ee.batch.Export.image.toDrive(
        image          = composite,
        description    = EXPORT_NAME,
        driveFolder    = DRIVE_FOLDER_ID,   # folder ID — no name ambiguity
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
    print("❌ Timed out after {max_minutes} minutes.")
    return False


# ── Download all tiles from Drive ─────────────────────────────────────────────
def download_all_tiles(service, local_dir):
    """
    Search your FAMM_EE_Exports folder for today's tiles and download them all.
    Searches by folder ID first (most reliable), then by name prefix as fallback.
    """
    from googleapiclient.http import MediaIoBaseDownload

    print(f"🔍 Searching Drive folder for: {EXPORT_NAME}*.tif")

    # Search within the specific folder by ID
    results = service.files().list(
        q=(f"'{DRIVE_FOLDER_ID}' in parents and "
           f"name contains '{EXPORT_NAME}' and trashed=false"),
        spaces="drive",
        fields="files(id,name,size)",
        orderBy="name",
        pageSize=20
    ).execute()

    files = results.get("files", [])

    # Fallback: search all of Drive by name if folder search returns nothing
    if not files:
        print("   Folder search found nothing — trying name search across all Drive...")
        results = service.files().list(
            q=f"name contains '{EXPORT_NAME}' and trashed=false",
            spaces="drive",
            fields="files(id,name,size)",
            orderBy="name",
            pageSize=20
        ).execute()
        files = results.get("files", [])

    if not files:
        print(f"❌ No tiles found matching '{EXPORT_NAME}' in Drive.")
        print(f"   Check that your Drive folder is shared with the service account")
        print(f"   and that EE exported to: {DRIVE_FOLDER}")
        return False

    os.makedirs(local_dir, exist_ok=True)
    total_mb = sum(int(f.get("size", 0)) for f in files) / 1_048_576
    print(f"   Found {len(files)} tile(s)  (~{total_mb:.1f} MB total)")

    for i, f in enumerate(files, 1):
        fid     = f["id"]
        name    = f["name"]
        size_mb = int(f.get("size", 0)) / 1_048_576
        dest    = os.path.join(local_dir, name)

        print(f"\n⬇️  [{i}/{len(files)}] {name} ({size_mb:.1f} MB)")
        fh = io.FileIO(dest, "wb")
        dl = MediaIoBaseDownload(
            fh, service.files().get_media(fileId=fid),
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
    print("FAMM — EE Export (Drive, Hybrid Auth)")
    print(f"Date : {today}  |  Export: {EXPORT_NAME}")
    print("=" * 60)

    # 1. Authenticate EE with YOUR personal OAuth token
    initialize_ee_for_export()

    # 2. Build ROI (must be after ee.Initialize)
    ROI = ee.Geometry.Polygon([ROI_COORDS])

    # 3. Build composite and start export to YOUR Drive folder
    print(f"\n🛰️  Building Sentinel-2 composite...")
    composite = build_composite(ROI)

    print(f"🚀 Starting Drive export → {DRIVE_FOLDER}/{EXPORT_NAME}*.tif")
    task = start_drive_export(composite, ROI)
    print(f"   Task ID: {task.id}")

    # 4. Wait for EE task to complete
    if not wait_for_task(task):
        sys.exit(1)

    # 5. Download tiles using service account (WIF) — it has folder Editor access
    print("\n📥 Downloading tiles from Drive (via service account)...")
    svc = build_drive_service_for_download()
    if not download_all_tiles(svc, "data/tif_input"):
        sys.exit(1)

    # 6. Signal export name to CI workflow
    print(f"\nEXPORT_FILENAME={EXPORT_NAME}.tif")
    print("=" * 60)
