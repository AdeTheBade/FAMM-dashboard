#!/usr/bin/env python3
"""
FAMM Earth Engine Export — Google Drive (Hybrid Auth)
------------------------------------------------------
AUTHENTICATION STRATEGY:
  Earth Engine export  → your personal OAuth token (EE_USER_CREDENTIALS secret)
  Drive sharing        → personal OAuth token shares each exported file with SA
  Drive download       → service account via WIF downloads the shared files

WHY HYBRID:
  Service accounts have no Google Drive storage quota. Earth Engine cannot
  export to a Drive that has no storage quota. Your personal Google account
  has 15 GB free Drive storage, so EE can export there successfully.

  After export, the personal account shares each exported file directly with
  the service account by file ID — no folder permission needed, no folder ID
  ambiguity, regardless of which folder EE creates.

SECRETS SETUP (one-time, in GitHub Secrets):
  EE_USER_CREDENTIALS   : cat ~/.config/earthengine/credentials
                          Used for EE authentication only.
  GCLOUD_USER_CREDENTIALS: cat ~/.config/gcloud/application_default_credentials.json
                          Used for Drive file sharing (has broader OAuth scopes).

LOCAL USAGE:
  CI=false python ee_export_drive_wif.py

GITHUB ACTIONS:
  EE_USER_CREDENTIALS    → EE export (earthengine scope)
  GCLOUD_USER_CREDENTIALS → Drive sharing (drive scope)
  GOOGLE_APPLICATION_CREDENTIALS (WIF) → Drive download (SA)
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

# Rosemary's service account — note: faam (not famm) is intentional
SA_EMAIL = "faam-github-actions-ee-runner@famm-472015.iam.gserviceaccount.com"

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


# ── Step 1: Load credentials ─────────────────────────────────────────────────
def load_personal_creds() -> dict:
    """
    Load EE OAuth credentials for Earth Engine authentication.
      CI    : EE_USER_CREDENTIALS secret (earthengine + devstorage scopes)
      Local : ~/.config/earthengine/credentials
    """
    in_ci = os.environ.get("CI", "false").lower() == "true"

    if in_ci:
        ee_creds_json = os.environ.get("EE_USER_CREDENTIALS", "")
        if not ee_creds_json:
            print("❌ EE_USER_CREDENTIALS secret is not set.")
            print("   Get it: cat ~/.config/earthengine/credentials")
            sys.exit(1)
        try:
            return json.loads(ee_creds_json)
        except json.JSONDecodeError as e:
            print(f"❌ EE_USER_CREDENTIALS is not valid JSON: {e}")
            sys.exit(1)
    else:
        creds_path = os.path.expanduser("~/.config/earthengine/credentials")
        if not os.path.exists(creds_path):
            print(f"❌ Local EE credentials not found at {creds_path}")
            print("   Run: earthengine authenticate")
            sys.exit(1)
        with open(creds_path) as f:
            return json.load(f)


def load_drive_creds() -> dict:
    """
    Load gcloud ADC credentials for Drive file sharing.
    These have broader OAuth scope than EE credentials and can access Drive.
      CI    : GCLOUD_USER_CREDENTIALS secret
      Local : ~/.config/gcloud/application_default_credentials.json
    """
    in_ci = os.environ.get("CI", "false").lower() == "true"

    if in_ci:
        gcloud_json = os.environ.get("GCLOUD_USER_CREDENTIALS", "")
        if not gcloud_json:
            print("❌ GCLOUD_USER_CREDENTIALS secret is not set.")
            print("   Get it: cat ~/.config/gcloud/application_default_credentials.json")
            print("   Add as GitHub Secret: GCLOUD_USER_CREDENTIALS")
            sys.exit(1)
        try:
            return json.loads(gcloud_json)
        except json.JSONDecodeError as e:
            print(f"❌ GCLOUD_USER_CREDENTIALS is not valid JSON: {e}")
            sys.exit(1)
    else:
        creds_path = os.path.expanduser(
            "~/.config/gcloud/application_default_credentials.json"
        )
        if not os.path.exists(creds_path):
            print(f"❌ gcloud ADC credentials not found at {creds_path}")
            print("   Run: gcloud auth application-default login")
            sys.exit(1)
        with open(creds_path) as f:
            return json.load(f)


# ── Step 2: Authenticate Earth Engine ────────────────────────────────────────
def initialize_ee_for_export(creds_data: dict):
    """
    Initialise EE using personal OAuth credentials.
    client_id and client_secret are read from creds_data — never hardcoded —
    so they always match the account that ran earthengine authenticate.
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
        print(f"❌ EE_USER_CREDENTIALS is missing required field: {e}")
        print("   Expected fields: client_id, client_secret, refresh_token")
        print("   Refresh: cat ~/.config/earthengine/credentials")
        sys.exit(1)
    except Exception as e:
        print(f"❌ EE personal auth failed: {e}")
        sys.exit(1)


# ── Step 3: Build personal Drive client (find + share exported files) ─────────
def build_personal_drive_service(drive_creds_data: dict):
    """
    Build Drive API client using gcloud ADC credentials.
    These have drive scope and can call files().list() and permissions().create().

    WHY gcloud creds and not EE creds?
    EE credentials (from earthengine authenticate) are issued with only
    earthengine + devstorage scopes. Requesting drive scope with those
    credentials fails with invalid_scope. gcloud ADC credentials
    (from gcloud auth application-default login) have broader scope
    and work with the Drive API.
    """
    try:
        personal_creds = google.oauth2.credentials.Credentials(
            token         = None,
            refresh_token = drive_creds_data["refresh_token"],
            token_uri     = "https://oauth2.googleapis.com/token",
            client_id     = drive_creds_data["client_id"],
            client_secret = drive_creds_data["client_secret"],
            scopes        = ["https://www.googleapis.com/auth/drive"]
        )
        return gdrive_build("drive", "v3", credentials=personal_creds)
    except Exception as e:
        print(f"❌ Personal Drive client build failed: {e}")
        sys.exit(1)


# ── Step 4: Build SA Drive client (WIF download) ─────────────────────────────
def build_sa_drive_service():
    """
    Build Drive API client using WIF service account credentials (CI)
    or local ADC credentials (local run).
    Used only to download files already shared with the SA by file ID.
    """
    try:
        import google.auth
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        return gdrive_build("drive", "v3", credentials=creds)
    except Exception as e:
        print(f"❌ SA Drive client build failed: {e}")
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
    Export composite to personal Drive using folder NAME.
    EE may create a new folder if multiple folders share the same name —
    this is fine because we locate files by name search after export and
    share them directly by file ID, so folder location is irrelevant.
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


# ── Share exported files with service account ─────────────────────────────────
def share_files_with_sa(personal_drive, sa_email: str) -> list:
    """
    Find today's exported tiles using the personal Drive client (owner can
    always find their own files) and share each one directly with the SA.

    Sharing by file ID is folder-agnostic — it doesn't matter which
    FAMM_EE_Exports folder EE wrote to. Once shared, the SA can download
    each tile by its file ID without any folder search.

    Returns list of file dicts for the SA download step.
    """
    print(f"🔍 Searching personal Drive for tiles: {EXPORT_NAME}*.tif")

    results = personal_drive.files().list(
        q        = f"name contains '{EXPORT_NAME}' and trashed=false",
        fields   = "files(id,name,size)",
        orderBy  = "name",
        pageSize = 20
    ).execute()

    files = results.get("files", [])

    if not files:
        print(f"❌ No tiles found matching '{EXPORT_NAME}' in personal Drive.")
        print(f"   EE reported COMPLETED — files may still be propagating.")
        print(f"   Check drive.google.com → FAMM_EE_Exports for today's tiles.")
        return []

    print(f"   Found {len(files)} tile(s) — sharing with SA ({sa_email})...")

    for f in files:
        try:
            personal_drive.permissions().create(
                fileId = f["id"],
                body   = {
                    "type":         "user",
                    "role":         "reader",
                    "emailAddress": sa_email,
                },
                fields = "id"
            ).execute()
            size_mb = int(f.get("size", 0)) / 1_048_576
            print(f"   ✅ Shared: {f['name']} ({size_mb:.1f} MB)")
        except Exception as e:
            print(f"   ⚠️  Could not share {f['name']}: {e}")

    return files


# ── Download files via service account ───────────────────────────────────────
def download_files(sa_drive, files: list, local_dir: str) -> bool:
    """
    Download tiles using SA credentials (WIF in CI, ADC locally).
    Uses file IDs directly — no folder search, no ambiguity.
    Files must already be shared with the SA before calling this.
    """
    from googleapiclient.http import MediaIoBaseDownload

    os.makedirs(local_dir, exist_ok=True)
    total_mb = sum(int(f.get("size", 0)) for f in files) / 1_048_576
    print(f"⬇️  Downloading {len(files)} tile(s) (~{total_mb:.1f} MB total)...")

    for i, f in enumerate(files, 1):
        fid     = f["id"]
        name    = f["name"]
        size_mb = int(f.get("size", 0)) / 1_048_576
        dest    = os.path.join(local_dir, name)

        print(f"\n   [{i}/{len(files)}] {name} ({size_mb:.1f} MB)")
        fh = io.FileIO(dest, "wb")
        dl = MediaIoBaseDownload(
            fh, sa_drive.files().get_media(fileId=fid),
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
    print("FAMM — EE Export (Drive, Hybrid Auth)")
    print(f"Date : {today}  |  Export: {EXPORT_NAME}")
    print("=" * 60)

    # 1. Load personal credentials once — used for EE auth and Drive sharing
    creds_data = load_personal_creds()

    # 2. Authenticate EE with personal OAuth (client_id/secret from creds_data)
    initialize_ee_for_export(creds_data)

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

    # 6. Find tiles in personal Drive and share each one with the SA
    print(f"\n🔗 Sharing exported tiles with service account...")
    drive_creds_data = load_drive_creds()
    personal_drive = build_personal_drive_service(drive_creds_data)
    files = share_files_with_sa(personal_drive, SA_EMAIL)

    if not files:
        sys.exit(1)

    # 7. Download tiles using SA credentials (WIF) — shared by file ID
    print("\n📥 Downloading tiles via service account (WIF)...")
    sa_drive = build_sa_drive_service()
    if not download_files(sa_drive, files, "data/tif_input"):
        sys.exit(1)

    # 8. Signal export name to CI workflow
    print(f"\nEXPORT_FILENAME={EXPORT_NAME}.tif")
    print("=" * 60)
