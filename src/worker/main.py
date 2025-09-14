from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
import json
import ast
import os
import io
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

from azure.servicebus import ServiceBusClient
from azure.storage.blob import BlobServiceClient, ContentSettings
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from PIL import Image, ImageFile, ImageOps, ImageFilter
ImageFile.LOAD_TRUNCATED_IMAGES = True

# ---- env ----
SB_CONN = os.environ["SERVICEBUS_CONNECTION"]
SB_QUEUE = os.environ.get("SERVICEBUS_QUEUE", "process")

ST_CONN = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
THUMBS = os.environ.get("THUMBS_CONTAINER", "thumbnails")

COSMOS_URI = os.environ["COSMOS_URI"]
COSMOS_DB = os.environ.get("COSMOS_DB", "mediaflow")
COSMOS_COLL = os.environ.get("COSMOS_COLL", "jobs")

# Targets (longest side). Set THUMB_MAX_2X=0 or DISABLE_2X=true to skip 2x.
THUMB_MAX_1X = int(os.environ.get("THUMB_MAX", "640"))
THUMB_MAX_2X = int(os.environ.get("THUMB_MAX_2X", "0"))
DISABLE_2X = os.environ.get(
    "DISABLE_2X", "true").lower() in ("1", "true", "yes")

# Quality & sharpening
THUMB_QUALITY = int(os.environ.get("THUMB_QUALITY", "90"))
THUMB_SHARPEN = max(0, min(3, int(os.environ.get("THUMB_SHARPEN", "2"))))

# ---- clients ----
blob_service = BlobServiceClient.from_connection_string(ST_CONN)
thumbs_cc = blob_service.get_container_client(THUMBS)
try:
    thumbs_cc.create_container()
    print(f"[init] created container '{THUMBS}'")
except ResourceExistsError:
    pass

mongo = MongoClient(COSMOS_URI, retryWrites=False, appname="mediaflow-worker")
jobs = mongo[COSMOS_DB][COSMOS_COLL]

# ---- imaging ----


def _encode_jpeg(im: Image.Image) -> bytes:
    if THUMB_SHARPEN:
        radius = [0, 1.0, 1.2, 1.5][THUMB_SHARPEN]
        percent = [0, 140, 170, 200][THUMB_SHARPEN]
        threshold = [0,   2,   2,   1][THUMB_SHARPEN]
        im = im.filter(ImageFilter.UnsharpMask(
            radius=radius, percent=percent, threshold=threshold))
    out = io.BytesIO()
    im.save(out, format="JPEG", quality=THUMB_QUALITY,
            optimize=True, progressive=True, subsampling=0)
    return out.getvalue()


def _prep(src_bytes: bytes) -> Image.Image:
    im = Image.open(io.BytesIO(src_bytes))
    im = ImageOps.exif_transpose(im)
    if im.mode not in ("RGB", "L"):
        im = im.convert("RGB")
    return im


def _cap_down(im: Image.Image, cap: int) -> Image.Image:
    if cap <= 0:  # "disabled" sentinel
        return None  # caller will skip
    if max(im.size) <= cap:
        return im.copy()
    out = im.copy()
    out.thumbnail((cap, cap), resample=Image.LANCZOS, reducing_gap=2.0)
    return out


def make_derivatives(src_bytes: bytes) -> tuple[bytes, Optional[bytes]]:
    """Return (b1x, b2x or None). Never exceeds caps. No upscaling."""
    base = _prep(src_bytes)

    # 1x
    im1 = _cap_down(base, THUMB_MAX_1X)
    if im1 is None:
        # if someone set THUMB_MAX=0 by mistake, fallback to 640
        im1 = _cap_down(base, 640)
    b1 = _encode_jpeg(im1)

    # 2x (optional)
    b2 = None
    if not DISABLE_2X:
        im2 = _cap_down(base, THUMB_MAX_2X)
        if im2 is not None:
            b2 = _encode_jpeg(im2)
    return b1, b2

# ---- db ----


def upsert_done(job_query: dict, thumb_url: str, thumb2x_url: Optional[str]):
    now = datetime.now(timezone.utc).isoformat()
    doc = {"status": "done", "thumbUrl": thumb_url, "finishedAt": now}
    if thumb2x_url:
        doc["thumb2xUrl"] = thumb2x_url
    try:
        jobs.update_one(job_query, {"$set": doc}, upsert=False)
    except PyMongoError as e:
        print(f"[warn] cosmos update failed: {e}", file=sys.stderr)

# ---- service bus helpers ----


def _loads_forgiving(s: str) -> dict:
    obj = json.loads(s)
    if isinstance(obj, str):
        try:
            obj = json.loads(obj)
        except Exception:
            pass
    if isinstance(obj, dict):
        return obj
    try:
        lit = ast.literal_eval(s)
        if isinstance(lit, dict):
            return lit
    except Exception:
        pass
    raise TypeError("Message body is not a JSON object")


def parse_body(body: Any) -> dict:
    if isinstance(body, dict):
        return body
    if isinstance(body, (bytes, bytearray, memoryview)):
        return _loads_forgiving(bytes(body).decode("utf-8", errors="replace"))
    if isinstance(body, str):
        return _loads_forgiving(body)
    try:
        parts = list(body)
        if parts:
            buf = bytearray()
            for p in parts:
                if isinstance(p, (bytes, bytearray, memoryview)):
                    buf.extend(p)
                elif isinstance(p, str):
                    buf.extend(p.encode("utf-8"))
                else:
                    buf.extend(str(p).encode("utf-8"))
            return _loads_forgiving(bytes(buf).decode("utf-8", errors="replace"))
    except TypeError:
        pass
    raise TypeError(f"Unsupported Service Bus body type: {type(body)}")

# ---- core ----


def process_payload(payload: dict):
    job_id = payload.get("id")
    blob_info = payload.get("blob") or {}
    container = (blob_info.get("container") or "uploads").strip().lower()
    name = (blob_info.get("name") or "").strip()
    url = payload.get("url")

    if not name:
        print("[skip] missing blob name", payload)
        return
    if container != "uploads":
        print(f"[skip] non-uploads container: {container}")
        return
    if name.endswith(".thumb.jpg") or name.endswith(".thumb@2x.jpg"):
        print(f"[skip] already a thumbnail: {name}")
        return

    src = blob_service.get_container_client(container).get_blob_client(name)

    # filenames
    b1_name = f"{name}.thumb.jpg"
    b2_name = f"{name}.thumb@2x.jpg"
    dst1 = thumbs_cc.get_blob_client(b1_name)
    dst2 = thumbs_cc.get_blob_client(b2_name)

    # download
    try:
        data = src.download_blob().readall()
    except ResourceNotFoundError:
        print(f"[warn] source blob not found: {name}")
        return

    # make & upload
    b1, b2 = make_derivatives(data)
    content = ContentSettings(
        content_type="image/jpeg", cache_control="public, max-age=31536000")
    dst1.upload_blob(b1, overwrite=True, content_settings=content)
    thumb_url = f"https://{blob_service.account_name}.blob.core.windows.net/{THUMBS}/{b1_name}"

    thumb2x_url = None
    if (not DISABLE_2X) and b2 is not None:
        dst2.upload_blob(b2, overwrite=True, content_settings=content)
        thumb2x_url = f"https://{blob_service.account_name}.blob.core.windows.net/{THUMBS}/{b2_name}"

    # update job
    q = {"id": job_id} if job_id else {"url": url}
    upsert_done(q, thumb_url, thumb2x_url)

    if thumb2x_url:
        print(f"[ok] {name} -> {b1_name} & {b2_name}")
    else:
        print(f"[ok] {name} -> {b1_name} (2x disabled)")

# ---- loop ----


def main():
    print("[start] worker listening on queue:", SB_QUEUE)
    with ServiceBusClient.from_connection_string(SB_CONN) as sb:
        with sb.get_queue_receiver(queue_name=SB_QUEUE, prefetch_count=5, max_wait_time=20) as receiver:
            while True:
                try:
                    for msg in receiver:
                        try:
                            payload = parse_body(msg.body)
                            process_payload(payload)
                            receiver.complete_message(msg)
                        except Exception as e:
                            print(
                                f"[err] processing failed: {e}", file=sys.stderr)
                            try:
                                receiver.dead_letter_message(
                                    msg, reason=str(e)[:250])
                            except Exception:
                                pass
                    time.sleep(0.5)
                except KeyboardInterrupt:
                    print("\n[stop] keyboard interrupt")
                    return
                except Exception as e:
                    print(f"[err] receiver loop: {e}", file=sys.stderr)
                    time.sleep(2)


if __name__ == "__main__":
    need = ["SERVICEBUS_CONNECTION",
            "AZURE_STORAGE_CONNECTION_STRING", "COSMOS_URI"]
    miss = [k for k in need if not os.environ.get(k)]
    if miss:
        print(f"[fatal] missing env: {', '.join(miss)}", file=sys.stderr)
        sys.exit(1)
    main()
