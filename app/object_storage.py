from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx

from app.config import Settings


@dataclass(slots=True)
class StoredObject:
    key: str
    url: str
    size_bytes: int
    sha256: str


class ObjectStorageError(RuntimeError):
    pass


class ObjectStorage:
    async def put_bytes(self, *, key: str, payload: bytes, content_type: str) -> StoredObject:
        raise NotImplementedError

    async def get_bytes(self, *, key: str | None = None, url: str | None = None) -> bytes:
        raise NotImplementedError


class S3ObjectStorage(ObjectStorage):
    def __init__(self, *, bucket: str, region: str):
        try:
            import boto3  # pylint: disable=import-outside-toplevel
        except ImportError as exc:  # pragma: no cover - dependency path
            raise ObjectStorageError("boto3 is required for S3 object storage") from exc

        self.bucket = bucket
        self.region = region
        self.client = boto3.client("s3", region_name=region or None)

    async def put_bytes(self, *, key: str, payload: bytes, content_type: str) -> StoredObject:
        sha256 = hashlib.sha256(payload).hexdigest()
        await asyncio.to_thread(
            self.client.put_object,
            Bucket=self.bucket,
            Key=key,
            Body=payload,
            ContentType=content_type,
            Metadata={"sha256": sha256},
        )
        return StoredObject(
            key=key,
            url=f"s3://{self.bucket}/{key}",
            size_bytes=len(payload),
            sha256=sha256,
        )

    async def get_bytes(self, *, key: str | None = None, url: str | None = None) -> bytes:
        resolved_key = key or _s3_key_from_url(url)
        if not resolved_key:
            raise ObjectStorageError("S3 key is required")
        response = await asyncio.to_thread(
            self.client.get_object,
            Bucket=self.bucket,
            Key=resolved_key,
        )
        body = response.get("Body")
        if body is None:
            raise ObjectStorageError(f"Empty S3 object body for key={resolved_key}")
        return await asyncio.to_thread(body.read)


def _s3_key_from_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    if parsed.scheme != "s3":
        return None
    return parsed.path.lstrip("/")


class VercelBlobStorage(ObjectStorage):
    def __init__(self, *, read_write_token: str, access: str = "private"):
        if not read_write_token:
            raise ObjectStorageError("VERCEL_BLOB_READ_WRITE_TOKEN is required")
        if access not in {"public", "private"}:
            raise ObjectStorageError("VERCEL_BLOB_ACCESS must be one of: public, private")
        self.read_write_token = read_write_token
        self.access = access

        try:
            from vercel.blob import put as blob_put  # pylint: disable=import-outside-toplevel
        except ImportError as exc:  # pragma: no cover - dependency path
            raise ObjectStorageError(
                "vercel SDK (python) is required for Vercel Blob object storage"
            ) from exc
        self._blob_put = blob_put

    async def put_bytes(self, *, key: str, payload: bytes, content_type: str) -> StoredObject:
        sha256 = hashlib.sha256(payload).hexdigest()

        def _upload() -> Any:
            return self._blob_put(
                key,
                payload,
                {
                    "token": self.read_write_token,
                    "access": self.access,
                    "allowOverwrite": True,
                    "contentType": content_type,
                },
            )

        result = await asyncio.to_thread(_upload)
        url = _blob_result_value(result, "url")
        pathname = _blob_result_value(result, "pathname") or key
        return StoredObject(
            key=pathname,
            url=url,
            size_bytes=len(payload),
            sha256=sha256,
        )

    async def get_bytes(self, *, key: str | None = None, url: str | None = None) -> bytes:
        target_url = url
        if not target_url and key:
            raise ObjectStorageError(
                "Vercel Blob export resolution requires a persisted URL"
            )
        if not target_url:
            raise ObjectStorageError("Vercel Blob URL is required")

        headers = {"Authorization": f"Bearer {self.read_write_token}"}
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(target_url, headers=headers)
            response.raise_for_status()
            return response.content


def _blob_result_value(result: Any, key: str) -> str:
    if isinstance(result, dict):
        value = result.get(key)
        if isinstance(value, str) and value:
            return value
        raise ObjectStorageError(f"Vercel Blob put result missing key: {key}")

    value = getattr(result, key, None)
    if isinstance(value, str) and value:
        return value
    raise ObjectStorageError(f"Vercel Blob put result missing attribute: {key}")


def create_object_storage(settings: Settings) -> ObjectStorage:
    provider = settings.archive_provider

    if provider in {"auto", "s3"} and settings.archive_s3_bucket:
        return S3ObjectStorage(
            bucket=settings.archive_s3_bucket,
            region=settings.archive_s3_region,
        )

    if provider in {"auto", "vercel_blob"} and settings.vercel_blob_read_write_token:
        return VercelBlobStorage(
            read_write_token=settings.vercel_blob_read_write_token,
            access=settings.vercel_blob_access,
        )

    raise ObjectStorageError(
        "No object storage backend configured. Set S3 or Vercel Blob env vars."
    )
