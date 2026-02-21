#!/usr/bin/env python3
"""
FAMM Earth Engine Export — Google Drive Version (Workload Identity Federation)
-------------------------------------------------------------------------------
LOCAL USAGE:
  earthengine authenticate   (run once)
  CI=false python ee_export_drive_wif.py

GITHUB ACTIONS:
  google-github-actions/auth@v2 sets GOOGLE_APPLICATION_CREDENTIALS automatically.
  Script detects CI=true and uses google.auth.default() (WIF keyless auth).
"""

import ee
import datetime
import io
import os
import sys
import time

# ── Constants (no EE calls here — safe before Initialize) ────────────────────
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


# ── Authentication ────────────────────────────────────────────────────────────
def initialize_ee():
    """
    Local  : uses personal credentials from `earthengine authenticate`
    CI/WIF : google-github-actions/auth@v2 has already set
             GOOGLE_APPLICATION_CREDENTIALS; google.auth.default() picks it up.
    Returns creds object (WIF) or None (local).
    """
    in_ci = os.environ.get("CI", "false").lower() == "true"

    if in_ci:
        try:
            import google.auth
            import google.auth.transport.requests

            creds, _ = google.auth.default(scopes=[
                "https://www.googleapis.com/auth/earthengine",
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/cloud-platform",
            ])
            creds.refresh(google.auth.transport.requests.Request())
            ee.Initialize(creds, project=PROJECT_ID)
            print("✅ Authenticated via Workload Identity Federation")
            return creds
        except Exception as e:
            print(f"❌ WIF authentication failed: {e}")
            sys.exit(1)
    else:
        ee.Initialize(project=PROJECT_ID)
        print("✅ Authenticated via local earthengine credentials")
        return None


# ── Build Drive API client ────────────────────────────────────────────────────
def build_drive_service(creds):
    try:
        from googleapiclient.discovery import build
        if creds:
            return build("drive", "v3", credentials=creds)
        # Local: request Drive-scoped credentials separately
        import google.auth
        local_creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        return build("drive", "v3", credentials=local_creds)
    except ImportError:
        print("❌ google-api-python-client not installed.")
        print("   Run: pip install google-api-python-client")
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
    end   = ee.Date(str(today))
    s7    = ee.Date(str(today - datetime.timedelta(days=7)))
    s14   = ee.Date(str(today - datetime.timedelta(days=14)))
    s30   = ee.Date(str(today - datetime.timedelta(days=30)))

    def col(start):
        mask_fn = lambda img: mask_s2(img, roi)
        return (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                  .filterBounds(roi)
                  .filterDate(start, end)
                  .map(mask_fn)
                  .select(BANDS))

    composite = col(s7).median().unmask(col(s14).median()).unmask(col(s30).median())
    return composite.unmask(0).clip(roi)


# ── Start Drive export task ───────────────────────────────────────────────────
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


# ── Poll until done ───────────────────────────────────────────────────────────
def wait_for_task(task, max_minutes=55):
    print(f"⏳ Polling EE task every 60 s (max {max_minutes} min)...")
    for i in range(max_minutes):
        time.sleep(60)
        state = task.status().get("state", "UNKNOWN")
        print(f"   [{i+1:02d}/{max_minutes}] {state}")
        if state == "COMPLETED":
            print(f"✅ Export complete: {EXPORT_NAME}.tif → Drive/{DRIVE_FOLDER}/")
            return True
        if state in ("FAILED", "CANCELLED"):
            print(f"❌ Task {state}: {task.status().get('error_message','no details')}")
            return False
    print("❌ Timed out.")
    return False


# ── Download from Drive ───────────────────────────────────────────────────────
def download_all_tiles(service, local_dir):
    """
    Download all .tif tiles for today's export from Drive.

    Earth Engine often splits large exports into multiple tiles named:
      Ghana_Composite_2026-02-21-0000000000-0000000000.tif
      Ghana_Composite_2026-02-21-0000000000-0000010496.tif
      ...
    We search by the export name prefix and download every matching file.
    run_inference.py processes all *.tif in data/tif_input/ so all tiles
    are used automatically.
    """
    from googleapiclient.http import MediaIoBaseDownload

    print(f"🔍 Searching Drive for tiles matching: {EXPORT_NAME}")
    results = service.files().list(
        q=f"name contains '{EXPORT_NAME}' and trashed=false",
        spaces="drive",
        fields="files(id,name,size)",
        orderBy="name",
        pageSize=20          # EE rarely produces more than 10 tiles
    ).execute()

    files = results.get("files", [])
    if not files:
        print(f"❌ No tiles found in Drive matching '{EXPORT_NAME}'.")
        print(f"   Ensure Drive API is enabled and EE exported successfully.")
        return False

    os.makedirs(local_dir, exist_ok=True)
    total_mb = sum(int(f.get("size", 0)) for f in files) / 1_048_576
    print(f"   Found {len(files)} tile(s)  (~{total_mb:.1f} MB total)")

    for i, f in enumerate(files, 1):
        fid      = f["id"]
        name     = f["name"]
        size_mb  = int(f.get("size", 0)) / 1_048_576
        dest     = os.path.join(local_dir, name)

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
    print("FAMM — EE Export (Drive + WIF)")
    print(f"Date : {today}  |  Export: {EXPORT_NAME}")
    print("=" * 60)

    # 1. Init EE — must happen before any ee.* calls
    creds = initialize_ee()

    # 2. Build ROI now that EE is initialised
    ROI = ee.Geometry.Polygon([ROI_COORDS])

    # 3. Composite + export
    print(f"\n🛰️  Building Sentinel-2 composite...")
    composite = build_composite(ROI)

    print(f"🚀 Starting Drive export → {DRIVE_FOLDER}/{EXPORT_NAME}*.tif")
    task = start_drive_export(composite, ROI)
    print(f"   Task ID: {task.id}")

    # 4. Wait for EE task
    if not wait_for_task(task):
        sys.exit(1)

    # 5. Download all tiles from Drive
    print("\n📥 Downloading tiles from Drive...")
    svc       = build_drive_service(creds)
    local_dir = "data/tif_input"
    if not download_all_tiles(svc, local_dir):
        sys.exit(1)

    # 6. Signal to CI workflow — report the prefix so workflow knows what ran
    print(f"\nEXPORT_FILENAME={EXPORT_NAME}.tif")
    print("=" * 60)