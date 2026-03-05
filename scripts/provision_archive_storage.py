#!/usr/bin/env python3
from __future__ import annotations

import argparse
import random
import string
import subprocess
import sys
from datetime import datetime


def _suffix(n: int = 8) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(n))


def _create_s3_bucket(*, bucket: str, region: str) -> tuple[bool, str]:
    cmd = [
        "aws",
        "s3api",
        "create-bucket",
        "--bucket",
        bucket,
        "--region",
        region,
    ]
    if region != "us-east-1":
        cmd.extend(
            [
                "--create-bucket-configuration",
                f"LocationConstraint={region}",
            ]
        )

    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, proc.stdout.strip()
    return False, (proc.stderr.strip() or proc.stdout.strip())


def _create_vercel_blob_store(*, name: str, scope: str) -> tuple[bool, str]:
    cmd = ["vercel", "blob", "store", "add", name, "--scope", scope]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, proc.stdout.strip()
    return False, (proc.stderr.strip() or proc.stdout.strip())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Provision archive object storage with S3 primary and Vercel Blob fallback.",
    )
    parser.add_argument("--region", default="eu-west-2")
    parser.add_argument("--bucket-prefix", default="chutes-research-optin-archive")
    parser.add_argument("--vercel-scope", default="chutesai")
    args = parser.parse_args()

    date_part = datetime.utcnow().strftime("%Y%m%d")
    bucket_name = f"{args.bucket_prefix}-{date_part}-{_suffix()}"

    ok, detail = _create_s3_bucket(bucket=bucket_name, region=args.region)
    if ok:
        print("S3 bucket created.")
        print("Set these env vars:")
        print("ARCHIVE_STORAGE_PROVIDER=s3")
        print(f"ARCHIVE_S3_BUCKET={bucket_name}")
        print(f"ARCHIVE_S3_REGION={args.region}")
        return 0

    print("S3 bucket creation failed; trying Vercel Blob fallback.", file=sys.stderr)
    print(detail, file=sys.stderr)

    store_name = f"research-data-optin-archive-{date_part}-{_suffix(5)}"
    ok, blob_detail = _create_vercel_blob_store(name=store_name, scope=args.vercel_scope)
    if ok:
        print("Vercel Blob store created.")
        print(blob_detail)
        print("Set these env vars:")
        print("ARCHIVE_STORAGE_PROVIDER=vercel_blob")
        print("VERCEL_BLOB_READ_WRITE_TOKEN=<token-from-vercel-store-output-or-dashboard>")
        print("VERCEL_BLOB_ACCESS=private")
        return 0

    print("Vercel Blob fallback failed too.", file=sys.stderr)
    print(blob_detail, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
