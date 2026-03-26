"""Pipeline execution engine."""

import json
import logging
import time

import httpx
import psycopg2
from psycopg2.extras import RealDictCursor

from app.config import settings

logger = logging.getLogger(__name__)


def _get_agent_url(agent_name: str) -> str | None:
    """Get the base URL for an agent from the registry."""
    urls = json.loads(settings.AGENT_URLS)
    return urls.get(agent_name)


def run_agent(agent_name: str, project_id: str, params: dict | None = None) -> dict:
    """Call POST /run on an agent and poll until completion.

    Raises on unrecoverable network errors so the caller can mark the task as 'error'.
    """
    base_url = _get_agent_url(agent_name)
    if not base_url:
        return {"status": "error", "error": f"Agent {agent_name} not found in AGENT_URLS"}

    # Start the agent
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(
                f"{base_url}/run",
                json={"project_id": project_id, "params": params or {}},
            )
            if resp.status_code == 409:
                return {"status": "error", "error": f"Agent {agent_name} is already running"}
            resp.raise_for_status()
    except httpx.HTTPError as e:
        return {"status": "error", "error": f"Failed to start agent {agent_name}: {e}"}

    # Poll for completion
    max_polls = 3600  # 1 hour at 1s interval
    consecutive_errors = 0
    for _ in range(max_polls):
        time.sleep(1)
        try:
            with httpx.Client(timeout=10.0) as client:
                status_resp = client.get(f"{base_url}/status")
                status_resp.raise_for_status()
                status_data = status_resp.json()
                consecutive_errors = 0
                if status_data["status"] in ("done", "error"):
                    return status_data
        except (httpx.HTTPError, ValueError, KeyError) as e:
            consecutive_errors += 1
            logger.warning(f"Poll error for {agent_name} ({consecutive_errors}/10): {e}")
            if consecutive_errors >= 10:
                return {"status": "error", "error": f"Agent {agent_name} unreachable after 10 consecutive poll failures"}

    return {"status": "error", "error": f"Agent {agent_name} timed out after {max_polls}s"}


def reflect_agent(
    agent_name: str,
    project_id: str,
    score: float,
    verdict: str,
    feedback: str | None = None,
) -> dict:
    """Call POST /reflect on an agent after gate."""
    base_url = _get_agent_url(agent_name)
    if not base_url:
        return {}
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(
                f"{base_url}/reflect",
                json={
                    "project_id": project_id,
                    "gate_feedback": feedback,
                    "gate_verdict": verdict,
                    "score": score,
                },
            )
            return resp.json() if resp.status_code == 200 else {}
    except Exception as e:
        logger.warning(f"Reflect failed for {agent_name}: {e}")
        return {}


def get_agent_knowledge(agent_name: str) -> dict:
    """Fetch agent's cross-project knowledge via direct DB query."""
    result = {"patterns": [], "prompts": [], "benchmarks": [], "task_count": 0}
    try:
        with psycopg2.connect(settings.postgres_dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM agent_knowledge.universal_patterns WHERE agent_name = %s AND is_active = TRUE",
                    (agent_name,),
                )
                result["patterns"] = [dict(r) for r in cur.fetchall()]

                cur.execute(
                    "SELECT agent_name, prompt_name, version, performance_score, is_active, created_at FROM agent_knowledge.prompt_versions WHERE agent_name = %s ORDER BY prompt_name, version DESC",
                    (agent_name,),
                )
                result["prompts"] = [dict(r) for r in cur.fetchall()]

                cur.execute(
                    "SELECT * FROM agent_knowledge.quality_benchmarks WHERE agent_name = %s AND is_active = TRUE",
                    (agent_name,),
                )
                result["benchmarks"] = [dict(r) for r in cur.fetchall()]

                cur.execute(
                    "SELECT COUNT(*) as count FROM agent_knowledge.task_history WHERE agent_name = %s",
                    (agent_name,),
                )
                result["task_count"] = cur.fetchone()["count"]
    except Exception as e:
        logger.warning(f"Failed to fetch knowledge for {agent_name}: {e}")

    return result


def get_agent_metrics(agent_name: str) -> dict:
    """Fetch agent's performance metrics across all projects."""
    result = {"total_tasks": 0, "avg_score": None, "by_project": []}
    try:
        with psycopg2.connect(settings.postgres_dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """SELECT COUNT(*) as total, AVG(score) as avg_score
                       FROM agent_knowledge.task_history
                       WHERE agent_name = %s AND score IS NOT NULL""",
                    (agent_name,),
                )
                row = cur.fetchone()
                result["total_tasks"] = row["total"]
                result["avg_score"] = float(row["avg_score"]) if row["avg_score"] else None

                cur.execute(
                    """SELECT project_id, COUNT(*) as tasks, AVG(score) as avg_score,
                              SUM(tokens_used) as total_tokens, SUM(cost_usd) as total_cost
                       FROM agent_knowledge.task_history
                       WHERE agent_name = %s
                       GROUP BY project_id ORDER BY MAX(started_at) DESC""",
                    (agent_name,),
                )
                result["by_project"] = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.warning(f"Failed to fetch metrics for {agent_name}: {e}")

    return result
