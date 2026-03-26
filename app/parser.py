"""Parser for project_info.md -> project_input.json."""

import re
import logging
from app import storage

logger = logging.getLogger(__name__)

# Mapping from H2 section titles to JSON field names
_SECTION_MAP = {
    "название": "project_name",
    "тематика": "topic",
    "целевая аудитория": "target_audience",
    "нюансы ниши": "niche_notes",
    "конкуренты": "competitor_urls",
    "seed-запросы": "seed_queries",
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
    for section_title, field_name in _SECTION_MAP.items():
        content = sections.get(section_title, "")
        if not content:
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


def parse_and_save(project_id: str) -> str:
    """Parse project_info.md and save project_input.json to MinIO.

    Returns the MinIO path to the saved JSON.
    """
    data = parse_project_info(project_id)
    output_path = f"{project_id}/data-input/output/project_input.json"
    storage.write_json(output_path, data)
    logger.info(f"Saved project_input.json for {project_id} at {output_path}")
    return output_path
