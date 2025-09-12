from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
import json
import ast
import os
import io
import sys
import time
from datetime import datetime, timezone
from typing import Any
from azure.servicebus import ServiceBusClient
from azure.storage.blob import BlobServiceClient, ContentSettings
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from PIL import Image, ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True

# ---- env ----
SB_CONN = os.environ["SERVICEBUS_CONNECTION"]
SB_QUEUE = os.environ.get("SERVICEBUS_QUEUE", "process")

ST_CONN = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
THUMBS = os.environ.get("THUMBS_CONTAINER", "thumbnails")

COSMOS_URI = os.environ["COSMOS_URI"]
COSMOS_DB = os.environ.get("COSMOS_DB", "mediaflow")
COSMOS_COLL = os.environ.get("COSMOS_COLL", "jobs")

# ---- clients ----
blob_service = BlobServiceClient.from_connection_string(ST_CONN)
uploads_cc = blob_service.get_container_client("uploads")
thumbs_cc = blob_service.get_container_client(THUMBS)
try:
    thumbs_cc.create_container()
    print(f"[init] created container '{THUMBS}'")
except ResourceExistsError:
    pass

mongo = MongoClient(COSMOS_URI, retryWrites=False, appname="mediaflow-worker")
jobs = mongo[COSMOS_DB][COSMOS_COLL]


def make_thumb(data: bytes, max_w=256) -> bytes:
    im = Image.open(io.BytesIO(data))
    # normalize mode so JPEG saves consistently
    if im.mode not in ("RGB", "L"):
        im = im.convert("RGB")
    w, h = im.size
    if w > max_w:
        im = im.resize((max_w, int(h * (max_w/float(w)))))
    out = io.BytesIO()
    im.save(out, format="JPEG", quality=85, optimize=True)
    return out.getvalue()


def upsert_done(job_query: dict, thumb_url: str):
    now = datetime.now(timezone.utc).isoformat()
    try:
        jobs.update_one(job_query, {"$set": {
            "status": "done",
            "thumbUrl": thumb_url,
            "finishedAt": now
        }}, upsert=False)
    except PyMongoError as e:
        print(f"[warn] cosmos update failed: {e}", file=sys.stderr)


def process_payload(payload: dict):
    """
    payload shape from Function:
      { id: <blob name or null>, url: <https url>, blob: { container: "uploads", name: "<file>" } }
    """
    # prefer id when present (your API uses blob name for id)
    job_id = payload.get("id")
    blob_info = payload.get("blob") or {}
    container = blob_info.get("container") or "uploads"
    name = blob_info.get("name")
    url = payload.get("url")

    if not name:
        print("[skip] missing blob name in payload:", payload)
        return

    src_cc = blob_service.get_container_client(container)
    src = src_cc.get_blob_client(name)
    dst_name = f"{name}.thumb.jpg"
    dst = thumbs_cc.get_blob_client(dst_name)

    # idempotency: if thumbnail already exists, just update the job and return
    try:
        if dst.exists():
            thumb_url = f"https://{blob_service.account_name}.blob.core.windows.net/{THUMBS}/{dst_name}"
            print(
                f"[idempotent] thumbnail exists for {name}, updating job only")
            q = {"id": job_id} if job_id else {"url": url}
            upsert_done(q, thumb_url)
            return
    except Exception:
        pass

    # download original
    try:
        data = src.download_blob().readall()
    except ResourceNotFoundError:
        print(f"[warn] source blob not found: {name}")
        return

    # transform
    thumb_bytes = make_thumb(data)

    # upload thumbnail
    content = ContentSettings(
        content_type="image/jpeg", cache_control="public, max-age=31536000")
    dst.upload_blob(thumb_bytes, overwrite=True, content_settings=content)
    thumb_url = f"https://{blob_service.account_name}.blob.core.windows.net/{THUMBS}/{dst_name}"

    # update job
    q = {"id": job_id} if job_id else {"url": url}
    upsert_done(q, thumb_url)

    print(f"[ok] {name} -> {dst_name}")


def _loads_forgiving(s: str) -> dict:
    """
    Load JSON; if result is a string containing JSON, load again.
    Falls back to ast.literal_eval for rare cases.
    """
    obj = json.loads(s)
    if isinstance(obj, str):
        try:
            obj2 = json.loads(obj)
            obj = obj2
        except Exception:
            pass
    if isinstance(obj, dict):
        return obj
    # last resort
    try:
        lit = ast.literal_eval(s)
        if isinstance(lit, dict):
            return lit
    except Exception:
        pass
    raise TypeError("Message body is not a JSON object")


def parse_body(body: Any) -> dict:
    # Already a dict?
    if isinstance(body, dict):
        return body

    # Bytes-like -> decode JSON
    if isinstance(body, (bytes, bytearray, memoryview)):
        return _loads_forgiving(bytes(body).decode("utf-8", errors="replace"))

    # String -> parse JSON (possibly twice)
    if isinstance(body, str):
        return _loads_forgiving(body)

    # Iterables (generator / DataBody sections) -> join and decode
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


def main():
    print("[start] worker listening on queue:", SB_QUEUE)
    # prefetch helps a bit if you upload multiple files
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
                            # dead-letter with reason so you can inspect in Portal
                            print(
                                f"[err] processing failed: {e}", file=sys.stderr)
                            try:
                                receiver.dead_letter_message(
                                    msg, reason=str(e)[:250])
                            except Exception:
                                pass
                    # idle wait
                    time.sleep(0.5)
                except KeyboardInterrupt:
                    print("\n[stop] keyboard interrupt")
                    return
                except Exception as e:
                    print(f"[err] receiver loop: {e}", file=sys.stderr)
                    time.sleep(2)


if __name__ == "__main__":
    # basic env sanity before starting
    needed = ["SERVICEBUS_CONNECTION",
              "AZURE_STORAGE_CONNECTION_STRING", "COSMOS_URI"]
    missing = [k for k in needed if not os.environ.get(k)]
    if missing:
        print(f"[fatal] missing env: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    main()
