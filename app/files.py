"""File API — read/write project files in MinIO."""

import posixpath
import re

from fastapi import APIRouter, HTTPException, Path
from pydantic import BaseModel

from app import storage

router = APIRouter()

_PROJECT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def _safe_path(project_id: str, path: str) -> str:
    """Build a safe MinIO path, preventing traversal outside the project scope."""
    if not _PROJECT_ID_RE.match(project_id):
        raise HTTPException(status_code=400, detail="Invalid project_id")
    normalized = posixpath.normpath(f"{project_id}/{path}")
    if not normalized.startswith(f"{project_id}/"):
        raise HTTPException(status_code=400, detail="Path traversal not allowed")
    return normalized


class FileContent(BaseModel):
    content: str


@router.get("/projects/{project_id}/files/{path:path}")
def read_file(
    project_id: str = Path(pattern=r"^[a-z0-9][a-z0-9_-]*$"),
    path: str = Path(),
):
    """Read a file from MinIO for the given project."""
    full_path = _safe_path(project_id, path)
    try:
        content = storage.read_text(full_path)
        return {"path": full_path, "content": content}
    except Exception as e:
        error_str = str(e)
        if "NoSuchKey" in error_str or "NoSuchBucket" in error_str:
            raise HTTPException(status_code=404, detail=f"File not found: {full_path}")
        raise HTTPException(status_code=502, detail=f"Storage error: {error_str}")


@router.put("/projects/{project_id}/files/{path:path}")
def write_file(
    body: FileContent,
    project_id: str = Path(pattern=r"^[a-z0-9][a-z0-9_-]*$"),
    path: str = Path(),
):
    """Write a file to MinIO for the given project."""
    full_path = _safe_path(project_id, path)
    try:
        storage.ensure_bucket()
        storage.write_text(full_path, body.content)
        return {"path": full_path, "saved": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to write file: {e}")
