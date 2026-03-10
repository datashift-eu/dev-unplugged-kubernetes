"""
unzip_to_minio.py
-----------------
Unzips a ZIP archive and uploads its contents to MinIO (S3-compatible).
Generates a JSON control file summarising the operation.

Environment variables (or pass via CLI args):
  MINIO_ENDPOINT      e.g. http://minio:9000
  MINIO_ACCESS_KEY
  MINIO_SECRET_KEY
  MINIO_BUCKET        target bucket name
  MINIO_PREFIX        optional key prefix / "folder" inside the bucket
  ZIP_SOURCE          path to the local zip file  -OR-  an s3://bucket/key URI
  CONTROL_FILE_KEY    key under which the control JSON is written (default: _control/manifest.json)
"""

import argparse
import hashlib
import io
import json
import logging
import os
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.client import Config


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_env(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.environ.get(name, default)
    if required and not value:
        log.error("Required environment variable '%s' is not set.", name)
        sys.exit(1)
    return value


def build_s3_client(endpoint: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",          # MinIO ignores this but boto3 needs it
    )


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
        log.info("Bucket '%s' already exists.", bucket)
    except client.exceptions.ClientError:
        client.create_bucket(Bucket=bucket)
        log.info("Bucket '%s' created.", bucket)


def md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def s3_key(prefix: str, member_name: str) -> str:
    """Combine an optional prefix with a zip member path."""
    prefix = prefix.rstrip("/") + "/" if prefix else ""
    return f"{prefix}{member_name}"


def download_zip_from_s3(client, uri: str) -> bytes:
    """Download a zip stored in S3/MinIO and return raw bytes."""
    # uri format: s3://bucket/key
    without_scheme = uri[len("s3://"):]
    bucket, _, key = without_scheme.partition("/")
    log.info("Downloading zip from s3://%s/%s …", bucket, key)
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def process_zip(
    client,
    zip_bytes: bytes,
    zip_source_label: str,
    target_bucket: str,
    prefix: str,
    control_file_key: str,
    dry_run: bool = False,
) -> dict:
    """
    Extract every file in the zip archive and upload to MinIO.
    Returns the control/manifest dictionary.
    """
    log.info(f"Unzipping {zip_source_label}")
    started_at = datetime.now(timezone.utc)

    split_label = zip_source_label.split("/")[4:9]

    extracted_prefix = f"{prefix}{'/'.join(split_label[0:4])}/{split_label[-1].split('_')[1].replace('.zip','')}"
    log.info("Extracting to s3://%s/%s", target_bucket, extracted_prefix)

    manifest = {
        "schema_version": "1.0",
        "source": zip_source_label,
        "target_bucket": target_bucket,
        "prefix": extracted_prefix,
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "dry_run": dry_run,
        "total_files": 0,
        "total_bytes_uncompressed": 0,
        "files": [],
    }

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        members = [m for m in zf.infolist() if not m.is_dir()]
        log.info("Archive contains %d file(s).", len(members))

        for member in members:
            data = zf.read(member.filename)
            key = s3_key(extracted_prefix, member.filename)
            checksum = md5_hex(data)
            size = len(data)

            file_entry = {
                "zip_path": member.filename,
                "s3_key": key,
                "size_bytes": size,
                "md5": checksum,
                "compress_type": member.compress_type,
                "date_time": "{}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}".format(*member.date_time),
                "status": None,
                "error": None,
            }

            if dry_run:
                log.info("[DRY-RUN] Would upload %s → s3://%s/%s (%d bytes)", member.filename, target_bucket, key, size)
                file_entry["status"] = "dry_run"
            else:
                try:
                    client.put_object(
                        Bucket=target_bucket,
                        Key=key,
                        Body=data,
                        ContentLength=size,
                        Metadata={"source-zip": zip_source_label, "original-path": member.filename},
                    )
                    log.info("Uploaded %s → s3://%s/%s (%d bytes)", member.filename, target_bucket, key, size)
                    file_entry["status"] = "uploaded"
                except Exception as exc:  # noqa: BLE001
                    log.error("Failed to upload %s: %s", member.filename, exc)
                    file_entry["status"] = "error"
                    file_entry["error"] = str(exc)

            manifest["files"].append(file_entry)
            manifest["total_bytes_uncompressed"] += size

    manifest["total_files"] = len(manifest["files"])
    manifest["finished_at"] = datetime.now(timezone.utc).isoformat()

    # --- write control file ---
    control_json = json.dumps(manifest, indent=2).encode()
    control_file_key = s3_key(extracted_prefix, control_file_key)
    if dry_run:
        log.info("[DRY-RUN] Would write control file → s3://%s/%s", target_bucket, control_file_key)
    else:
        client.put_object(
            Bucket=target_bucket,
            Key=control_file_key,
            Body=control_json,
            ContentType="application/json",
        )
        log.info("Control file written → s3://%s/%s", target_bucket, control_file_key)

    return manifest


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Unzip a file and upload contents to MinIO/S3.")
    p.add_argument("--endpoint",        default=None, help="MinIO endpoint URL")
    p.add_argument("--access-key",      default=None, dest="access_key")
    p.add_argument("--secret-key",      default=None, dest="secret_key")
    p.add_argument("--bucket",          default=None)
    p.add_argument("--prefix",          default="",   help="Key prefix inside the bucket")
    p.add_argument("--zip-source",      default=None, dest="zip_source",
                   help="Local path or s3://bucket/key URI of the zip file")
    p.add_argument("--control-file-key", default=None, dest="control_file_key",
                   help="S3 key for the JSON control/manifest file")
    p.add_argument("--dry-run",         action="store_true", dest="dry_run",
                   help="Parse zip and print plan without uploading anything")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    endpoint        = args.endpoint        or get_env("AWS_ENDPOINT_URL",    required=True)
    access_key      = args.access_key      or get_env("AWS_ACCESS_KEY_ID",  required=True)
    secret_key      = args.secret_key      or get_env("AWS_SECRET_ACCESS_KEY",  required=True)

    bucket          = args.bucket          or get_env("MINIO_BUCKET",      required=True)
    prefix          = args.prefix          or get_env("MINIO_PREFIX",      default="")
    zip_source      = args.zip_source      or get_env("ZIP_SOURCE",        required=True)
    control_key     = args.control_file_key or get_env("CONTROL_FILE_KEY", default="_control/manifest.json")
    dry_run         = args.dry_run

    client = build_s3_client(endpoint, access_key, secret_key)

    if not dry_run:
        ensure_bucket(client, bucket)

    # Load zip bytes --------------------------------------------------------
    if zip_source.startswith("s3://"):
        zip_bytes = download_zip_from_s3(client, zip_source)
        label = zip_source
    else:
        path = Path(zip_source)
        if not path.exists():
            log.error("Local zip file not found: %s", zip_source)
            sys.exit(1)
        zip_bytes = path.read_bytes()
        label = path.name

    # Process ---------------------------------------------------------------
    manifest = process_zip(
        client=client,
        zip_bytes=zip_bytes,
        zip_source_label=label,
        target_bucket=bucket,
        prefix=prefix,
        control_file_key=control_key,
        dry_run=dry_run,
    )

    # Summary ---------------------------------------------------------------
    uploaded  = sum(1 for f in manifest["files"] if f["status"] == "uploaded")
    errors    = sum(1 for f in manifest["files"] if f["status"] == "error")
    total_mb  = manifest["total_bytes_uncompressed"] / (1024 ** 2)

    log.info("─" * 60)
    log.info("Done. Total files: %d | Uploaded: %d | Errors: %d | %.2f MB",
             manifest["total_files"], uploaded, errors, total_mb)

    if errors:
        sys.exit(2)   # non-zero so Argo marks the step as failed


if __name__ == "__main__":
    main()