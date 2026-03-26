import json

import psycopg2
from psycopg2.extras import Json, RealDictCursor

from app.config import settings


def _connect():
    return psycopg2.connect(settings.postgres_dsn)


# --- Projects ---


def create_project(project_id: str, name: str, topic: str | None = None) -> dict:
    """Create a new project in the registry."""
    with _connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """INSERT INTO projects.project (id, name, topic)
                   VALUES (%s, %s, %s) RETURNING *""",
                (project_id, name, topic),
            )
            conn.commit()
            return dict(cur.fetchone())


def get_project(project_id: str) -> dict | None:
    with _connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM projects.project WHERE id = %s", (project_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def list_projects() -> list[dict]:
    with _connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM projects.project ORDER BY created_at DESC")
            return [dict(row) for row in cur.fetchall()]


def update_project_status(project_id: str, status: str) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE projects.project SET status = %s, updated_at = NOW() WHERE id = %s",
                (status, project_id),
            )
            conn.commit()


# --- Task Queue ---


def enqueue_tasks(project_id: str, pipeline: list[dict]) -> list[dict]:
    """Enqueue all pipeline steps as tasks for a project."""
    with _connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            tasks = []
            for i, step in enumerate(pipeline):
                cur.execute(
                    """INSERT INTO projects.task_queue
                       (project_id, agent_name, priority, params)
                       VALUES (%s, %s, %s, %s) RETURNING *""",
                    (project_id, step["agent"], i, Json(step.get("params", {}))),
                )
                tasks.append(dict(cur.fetchone()))
            conn.commit()
            return tasks


def has_blocking_task(project_id: str) -> dict | None:
    """Check if pipeline is blocked by a waiting_gate, running, or error task."""
    with _connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT id, agent_name, status FROM projects.task_queue
                   WHERE project_id = %s AND status IN ('waiting_gate', 'running', 'error')
                   ORDER BY priority ASC LIMIT 1""",
                (project_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def claim_next_task(project_id: str) -> dict | None:
    """Atomically find the next pending task and set it to running.

    Uses SELECT ... FOR UPDATE SKIP LOCKED + UPDATE in a single transaction
    to prevent double-claiming under concurrent calls.
    """
    with _connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT id FROM projects.task_queue
                   WHERE project_id = %s AND status = 'pending'
                   ORDER BY priority ASC LIMIT 1
                   FOR UPDATE SKIP LOCKED""",
                (project_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            task_id = row["id"]
            cur.execute(
                """UPDATE projects.task_queue
                   SET status = 'running', started_at = NOW()
                   WHERE id = %s
                   RETURNING *""",
                (task_id,),
            )
            conn.commit()
            return dict(cur.fetchone())


def update_task_status(task_id: int, status: str, result: dict | None = None) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            if status == "running":
                cur.execute(
                    "UPDATE projects.task_queue SET status = %s, started_at = NOW() WHERE id = %s",
                    (status, task_id),
                )
            elif status in ("done", "error"):
                cur.execute(
                    "UPDATE projects.task_queue SET status = %s, finished_at = NOW(), result = %s WHERE id = %s",
                    (status, Json(result) if result else None, task_id),
                )
            elif status == "waiting_gate":
                cur.execute(
                    "UPDATE projects.task_queue SET status = %s, finished_at = NOW() WHERE id = %s",
                    (status, task_id),
                )
            else:
                cur.execute(
                    "UPDATE projects.task_queue SET status = %s WHERE id = %s",
                    (status, task_id),
                )
            conn.commit()


def update_gate(task_id: int, gate_status: str, feedback: str | None = None) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            if gate_status == "approved":
                cur.execute(
                    """UPDATE projects.task_queue
                       SET gate_status = %s, gate_feedback = %s, status = 'done'
                       WHERE id = %s""",
                    (gate_status, feedback, task_id),
                )
            else:
                # rejected or revised — reset to pending for retry
                cur.execute(
                    """UPDATE projects.task_queue
                       SET gate_status = %s, gate_feedback = %s, status = 'pending', started_at = NULL, finished_at = NULL
                       WHERE id = %s""",
                    (gate_status, feedback, task_id),
                )
            conn.commit()


def get_project_tasks(project_id: str) -> list[dict]:
    """Get all tasks for a project ordered by priority."""
    with _connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT * FROM projects.task_queue
                   WHERE project_id = %s ORDER BY priority ASC""",
                (project_id,),
            )
            return [dict(row) for row in cur.fetchall()]


def get_waiting_gate_task(project_id: str) -> dict | None:
    """Get task currently waiting for gate approval."""
    with _connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT * FROM projects.task_queue
                   WHERE project_id = %s AND status = 'waiting_gate'
                   LIMIT 1""",
                (project_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
