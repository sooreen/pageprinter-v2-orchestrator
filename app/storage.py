"""MinIO storage helpers for the orchestrator."""

import io
import json
from minio import Minio
from app.config import settings


def get_minio_client() -> Minio:
    return Minio(
        settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=settings.MINIO_USE_SSL,
    )


def read_file(path: str) -> bytes:
    """Read raw bytes from MinIO."""
    client = get_minio_client()
    response = client.get_object(settings.MINIO_BUCKET, path)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def read_text(path: str) -> str:
    """Read a text file from MinIO."""
    return read_file(path).decode("utf-8")


def write_text(path: str, content: str) -> str:
    """Write a text file to MinIO. Returns the path."""
    client = get_minio_client()
    data = content.encode("utf-8")
    client.put_object(
        settings.MINIO_BUCKET,
        path,
        io.BytesIO(data),
        length=len(data),
        content_type="text/plain; charset=utf-8",
    )
    return path


def write_json(path: str, data: dict) -> str:
    """Write a JSON file to MinIO. Returns the path."""
    client = get_minio_client()
    content = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        settings.MINIO_BUCKET,
        path,
        io.BytesIO(content),
        length=len(content),
        content_type="application/json",
    )
    return path


def file_exists(path: str) -> bool:
    """Check if a file exists in MinIO."""
    client = get_minio_client()
    try:
        client.stat_object(settings.MINIO_BUCKET, path)
        return True
    except Exception:
        return False


def ensure_bucket():
    """Create bucket if it doesn't exist."""
    client = get_minio_client()
    if not client.bucket_exists(settings.MINIO_BUCKET):
        client.make_bucket(settings.MINIO_BUCKET)
