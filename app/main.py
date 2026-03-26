"""Orchestrator service — manages project pipelines and agent coordination."""

import logging

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app import db, engine
from app.pipeline import get_pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="PagePrinterV2 Orchestrator")


# --- Request/Response Models ---


class CreateProjectRequest(BaseModel):
    project_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9_-]*$")
    name: str = Field(min_length=1)
    topic: str | None = None


class GateRequest(BaseModel):
    feedback: str | None = None
    score: float = Field(default=1.0, ge=0.0, le=1.0)


# --- Project Endpoints ---


@app.post("/projects", status_code=201)
def create_project(request: CreateProjectRequest):
    existing = db.get_project(request.project_id)
    if existing:
        raise HTTPException(status_code=409, detail="Project already exists")
    project = db.create_project(request.project_id, request.name, request.topic)
    return project


@app.get("/projects")
def list_projects():
    return db.list_projects()


@app.get("/projects/{project_id}/status")
def project_status(project_id: str):
    project = db.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    tasks = db.get_project_tasks(project_id)
    return {
        "project": project,
        "tasks": tasks,
        "completed": sum(1 for t in tasks if t["status"] == "done"),
        "total": len(tasks),
        "current": next((t for t in tasks if t["status"] in ("running", "waiting_gate")), None),
    }


# --- Pipeline Endpoints ---


@app.post("/projects/{project_id}/step")
def execute_next_step(project_id: str):
    """Execute the next pending task in the pipeline. Returns when the task completes or reaches a gate."""
    project = db.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    # Check for blocking tasks (waiting_gate, running, error)
    blocker = db.has_blocking_task(project_id)
    if blocker:
        raise HTTPException(
            status_code=409,
            detail=f"Pipeline blocked: task {blocker['agent_name']} is in '{blocker['status']}' state",
        )

    # Atomically claim the next pending task (SELECT + UPDATE in one transaction)
    task = db.claim_next_task(project_id)
    if not task:
        return {"message": "No pending tasks", "pipeline_complete": True}

    # Execute agent — catch all errors so task never stays stuck in 'running'
    try:
        result = engine.run_agent(task["agent_name"], project_id, task.get("params", {}))
    except Exception as e:
        logger.error(f"Agent {task['agent_name']} raised exception: {e}")
        result = {"status": "error", "error": str(e)}

    if result.get("status") == "error":
        db.update_task_status(task["id"], "error", result)
        return {"message": f"Agent {task['agent_name']} failed", "error": result.get("error"), "task_id": task["id"]}

    # Check if this step has a gate
    pipeline = get_pipeline()
    step = next((s for s in pipeline if s["agent"] == task["agent_name"]), None)

    if step and step.get("gate"):
        db.update_task_status(task["id"], "waiting_gate")
        return {
            "message": f"Agent {task['agent_name']} done, waiting for gate '{step['gate']}'",
            "gate": step["gate"],
            "task_id": task["id"],
        }
    else:
        db.update_task_status(task["id"], "done", result)
        # Auto-reflect with score=1.0 for non-gated steps
        engine.reflect_agent(task["agent_name"], project_id, score=1.0, verdict="approved")
        return {"message": f"Agent {task['agent_name']} completed", "task_id": task["id"]}


@app.post("/projects/{project_id}/start")
def start_pipeline(project_id: str):
    project = db.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    existing_tasks = db.get_project_tasks(project_id)
    if existing_tasks:
        raise HTTPException(status_code=409, detail="Pipeline already started")

    pipeline = get_pipeline()
    tasks = db.enqueue_tasks(project_id, pipeline)
    db.update_project_status(project_id, "active")
    return {"message": "Pipeline started", "tasks_enqueued": len(tasks)}


# --- Gate Endpoints ---


@app.post("/projects/{project_id}/gates/{gate_name}/approve")
def approve_gate(project_id: str, gate_name: str, request: GateRequest):
    task = db.get_waiting_gate_task(project_id)
    if not task:
        raise HTTPException(status_code=404, detail="No task waiting for gate approval")

    # Verify gate name matches
    pipeline = get_pipeline()
    step = next((s for s in pipeline if s["agent"] == task["agent_name"]), None)
    if not step or step.get("gate") != gate_name:
        raise HTTPException(
            status_code=400,
            detail=f"Gate '{gate_name}' does not match current agent '{task['agent_name']}'",
        )

    db.update_gate(task["id"], "approved", request.feedback)

    # Trigger reflection on the agent
    engine.reflect_agent(
        task["agent_name"],
        project_id,
        score=request.score,
        verdict="approved",
        feedback=request.feedback,
    )

    return {"message": f"Gate '{gate_name}' approved", "agent": task["agent_name"]}


@app.post("/projects/{project_id}/gates/{gate_name}/reject")
def reject_gate(project_id: str, gate_name: str, request: GateRequest):
    task = db.get_waiting_gate_task(project_id)
    if not task:
        raise HTTPException(status_code=404, detail="No task waiting for gate approval")

    pipeline = get_pipeline()
    step = next((s for s in pipeline if s["agent"] == task["agent_name"]), None)
    if not step or step.get("gate") != gate_name:
        raise HTTPException(
            status_code=400,
            detail=f"Gate '{gate_name}' does not match current agent '{task['agent_name']}'",
        )

    db.update_gate(task["id"], "rejected", request.feedback)

    engine.reflect_agent(
        task["agent_name"],
        project_id,
        score=request.score,
        verdict="rejected",
        feedback=request.feedback,
    )

    return {"message": f"Gate '{gate_name}' rejected — task reset to pending", "agent": task["agent_name"]}


# --- Agent Knowledge Endpoints ---


@app.get("/agents/{agent_name}/knowledge")
def agent_knowledge(agent_name: str):
    return engine.get_agent_knowledge(agent_name)


@app.get("/agents/{agent_name}/metrics")
def agent_metrics(agent_name: str):
    return engine.get_agent_metrics(agent_name)


# --- Health ---


@app.get("/health")
def health():
    return {"status": "ok", "service": "orchestrator"}
