"""Parser for project_info.md -> project_input.json."""

import json
import re
import logging
from pathlib import Path

import jsonschema

from app import storage

logger = logging.getLogger(__name__)

# Mapping from H2 section title keywords to JSON field names.
# Each key is matched if the normalized section title contains it.
_SECTION_KEYWORDS = [
    ("назван", "project_name"),
    ("тематик", "topic"),
    ("аудитори", "target_audience"),  # matches "целевая аудитория", "ца", "аудитория"
    ("нюанс", "niche_notes"),        # matches "нюансы ниши"
    ("нишы", "niche_notes"),         # alternate
    ("конкурент", "competitor_urls"),
    ("seed", "seed_queries"),
    ("сид", "seed_queries"),         # cyrillic variant
    ("запрос", "seed_queries"),      # matches "seed-запросы", "запросы"
]

# Exact match fallback (original strict mapping)
_SECTION_MAP_EXACT = {
    "название": "project_name",
    "тематика": "topic",
    "целевая аудитория": "target_audience",
    "ца": "target_audience",
    "нюансы ниши": "niche_notes",
    "конкуренты": "competitor_urls",
    "seed-запросы": "seed_queries",
    "seed запросы": "seed_queries",
}

_REQUIRED_FIELDS = {"project_name", "topic", "target_audience"}

_URL_PATTERN = re.compile(r"https?://\S+")


def _parse_markdown_sections(text: str) -> dict[str, str]:
    """Split markdown into {section_title_lowercase: content} by H2 headers."""
    sections = {}
    current_title = None
    current_lines = []

    for line in text.split("\n"):
        if line.startswith("## "):
            if current_title is not None:
                sections[current_title] = "\n".join(current_lines).strip()
            current_title = line[3:].strip().lower()
            # Remove "(опционально)" suffix
            current_title = re.sub(r"\s*\(опционально\)\s*$", "", current_title)
            current_lines = []
        elif current_title is not None:
            current_lines.append(line)

    if current_title is not None:
        sections[current_title] = "\n".join(current_lines).strip()

    return sections


def _extract_list_items(text: str) -> list[str]:
    """Extract bullet list items from text."""
    items = []
    for line in text.split("\n"):
        line = line.strip()
        if line.startswith("- ") or line.startswith("* "):
            items.append(line[2:].strip())
    return items


def _extract_urls(text: str) -> list[str]:
    """Extract URLs from text."""
    urls = []
    for line in text.split("\n"):
        line = line.strip()
        if line.startswith("- ") or line.startswith("* "):
            line = line[2:].strip()
        match = _URL_PATTERN.match(line)
        if match:
            urls.append(match.group())
    return urls


def parse_project_info(project_id: str) -> dict:
    """Parse project_info.md from MinIO and return project_input dict.

    Raises ValueError if required fields are missing.
    """
    path = f"{project_id}/project_info.md"
    text = storage.read_text(path)
    sections = _parse_markdown_sections(text)

    result = {}
    for section_title, content in sections.items():
        if not content:
            continue
        # Resolve field name: try exact match first, then keyword match
        field_name = _SECTION_MAP_EXACT.get(section_title)
        if not field_name:
            for keyword, fname in _SECTION_KEYWORDS:
                if keyword in section_title:
                    field_name = fname
                    break
        if not field_name:
            logger.debug(f"Unmapped section: '{section_title}'")
            continue

        if field_name == "competitor_urls":
            result[field_name] = _extract_urls(content)
        elif field_name == "seed_queries":
            result[field_name] = _extract_list_items(content)
        else:
            result[field_name] = content

    # Validate required fields
    missing = _REQUIRED_FIELDS - set(result.keys())
    if missing:
        raise ValueError(f"Missing required fields in project_info.md: {missing}")

    return result


def _load_schema() -> dict:
    """Load the project_input JSON schema from the contracts directory."""
    # Check multiple locations: Docker image, monorepo
    candidates = [
        Path("/app/contracts/project_input.schema.json"),
        Path(__file__).resolve().parents[3] / "contracts" / "project_input.schema.json",
    ]
    for schema_path in candidates:
        if schema_path.exists():
            with open(schema_path) as f:
                return json.load(f)
    logger.warning("project_input.schema.json not found, skipping validation")
    return {}


def _validate_against_schema(data: dict) -> None:
    """Validate data against project_input.schema.json. Raises ValueError on failure."""
    schema = _load_schema()
    if not schema:
        return
    try:
        jsonschema.validate(instance=data, schema=schema)
    except jsonschema.ValidationError as e:
        raise ValueError(f"project_input.json does not match schema: {e.message}")


def parse_and_save(project_id: str) -> str:
    """Parse project_info.md and save project_input.json to MinIO.

    Returns the MinIO path to the saved JSON.
    """
    data = parse_project_info(project_id)
    _validate_against_schema(data)
    output_path = f"{project_id}/data-input/output/project_input.json"
    storage.write_json(output_path, data)
    logger.info(f"Saved project_input.json for {project_id} at {output_path}")
    return output_path
