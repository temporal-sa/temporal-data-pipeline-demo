import hashlib
import os
import secrets
import uuid
from datetime import timedelta

from flask import Flask, g, jsonify, render_template, request
from flask.wrappers import Response
from temporalio.client import Client, WorkflowExecutionStatus

from client import TASK_QUEUE, get_client
from dataobjects import DataPipelineParams

app = Flask(__name__)

# Each browser gets an unguessable session ID in this cookie. Workflow IDs are
# built from a hash of it, so a browser only ever sees and drives its own jobs,
# and a workflow ID seen in the Temporal UI does not reveal the cookie.
SESSION_COOKIE = "pipeline_session"

# Bounds a run nobody finishes (RecoverableFailure retries its bug until the
# code is fixed; HumanInLoop waits for a click).
WORKFLOW_EXECUTION_TIMEOUT = timedelta(minutes=30)

TEMPORAL_UI_URL = os.getenv("TEMPORAL_UI_URL", "http://localhost:8233").rstrip("/")
TEMPORAL_UI_NAMESPACE = os.getenv("TEMPORAL_UI_NAMESPACE", os.getenv("TEMPORAL_NAMESPACE", "default"))

FAILED_STATUSES = {
    WorkflowExecutionStatus.FAILED,
    WorkflowExecutionStatus.TIMED_OUT,
    WorkflowExecutionStatus.TERMINATED,
    WorkflowExecutionStatus.CANCELED,
}

# Scenario choices dropdown
scenarios = [
    "HappyPath",
    "AdvancedVisibility",
    "HumanInLoopSignal",
    "HumanInLoopUpdate",
    "Idempotency",
    "APIFailure",
    "RecoverableFailure",
    "NonRecoverableFailure",
]

data = {
    "input_filename": "info.json",
    "foldername": "./demodata",
    "poll_or_wait": "poll",
    "validation": "orange",
}

_client: Client | None = None


async def temporal_client() -> Client:
    """One Temporal client for the whole process, connected on first use."""
    global _client
    if _client is None:
        _client = await get_client()
    return _client


@app.before_request
def load_session() -> None:
    session_id = request.cookies.get(SESSION_COOKIE, "")
    g.new_session = not (len(session_id) == 32 and all(c in "0123456789abcdef" for c in session_id))
    g.session_id = secrets.token_hex(16) if g.new_session else session_id


@app.after_request
def save_session(response: Response) -> Response:
    if g.get("new_session"):
        response.set_cookie(SESSION_COOKIE, g.session_id, httponly=True, samesite="Lax", max_age=7 * 24 * 3600)
    return response


def workflow_id(job_id: str) -> str:
    session_key = hashlib.sha256(g.session_id.encode()).hexdigest()[:16]
    return f"pipeline-{session_key}-{job_id}"


def temporal_ui_link(job_id: str) -> str:
    return f"{TEMPORAL_UI_URL}/namespaces/{TEMPORAL_UI_NAMESPACE}/workflows/{workflow_id(job_id)}"


@app.route("/healthz")
def healthz() -> str:
    return "ok"


@app.route("/", methods=["GET", "POST"])
async def main_order_page() -> str:
    job_id = str(uuid.uuid4().int)[:6]

    return render_template("index.html", data=data, scenarios=scenarios, job_id=job_id)


@app.route("/run_job")
async def run_job() -> str | tuple[str, int]:
    selected_scenario = request.args.get("scenario")
    job_id = request.args.get("job_id")

    if not selected_scenario or selected_scenario not in scenarios:
        return "Error: a valid scenario parameter is required", 400
    if not job_id or not job_id.isdigit():
        return "Error: a numeric job_id parameter is required", 400

    client = await temporal_client()

    input = DataPipelineParams(
        input_filename=data["input_filename"],
        foldername=data["foldername"],
        poll_or_wait=data["poll_or_wait"],
        validation=data["validation"],
        scenario=selected_scenario,
        key=workflow_id(job_id),
    )

    await client.start_workflow(
        "DataPipeline" + selected_scenario if selected_scenario != "HappyPath" else "DataPipelineWorkflowHappyPath",
        input,
        id=workflow_id(job_id),
        task_queue=TASK_QUEUE,
        execution_timeout=WORKFLOW_EXECUTION_TIMEOUT,
    )

    return render_template(
        "job_progress.html",
        selected_scenario=selected_scenario,
        job_id=job_id,
        temporal_ui_link=temporal_ui_link(job_id),
    )


@app.route("/confirmation")
async def order_confirmation() -> str | tuple[str, int]:
    job_id = request.args.get("job_id")

    if not job_id:
        return "Error: job_id parameter is required", 400

    client = await temporal_client()
    pipeline_workflow = client.get_workflow_handle(workflow_id(job_id))
    await pipeline_workflow.result()

    return render_template("confirmation.html", job_id=job_id, temporal_ui_link=temporal_ui_link(job_id))


@app.route("/get_progress")
async def get_progress() -> Response | tuple[Response, int]:
    job_id = request.args.get("job_id")

    if not job_id:
        return jsonify({"error": "job_id parameter is required"}), 400

    progress_percent = 0
    try:
        client = await temporal_client()
        pipeline_workflow = client.get_workflow_handle(workflow_id(job_id))

        try:
            progress_percent = await pipeline_workflow.query("progress")
        except Exception as e:
            print(e)

        desc = await pipeline_workflow.describe()
        if desc.status in FAILED_STATUSES:
            error_message = f"Workflow {desc.status.name.lower().replace('_', ' ')}: job-{job_id}"
            print(f"Error in get_progress route: {error_message}")
            return jsonify({"error": error_message}), 500

        return jsonify({"progress": progress_percent})
    except Exception:
        return jsonify({"progress": progress_percent})


@app.route("/signal", methods=["POST"])
async def signal() -> tuple[str, int] | tuple[Response, int]:
    job_id = request.args.get("job_id")

    if not job_id:
        return jsonify({"error": "job_id parameter is required"}), 400

    try:
        client = await temporal_client()
        pipeline_workflow = client.get_workflow_handle(workflow_id(job_id))
        await pipeline_workflow.signal("load_complete_signal", "complete")
    except Exception as e:
        print(f"Error sending signal: {str(e)}")
        return jsonify({"error": str(e)}), 500

    return "Signal received successfully", 200


@app.route("/update", methods=["POST"])
async def update() -> Response:
    job_id = request.args.get("job_id")

    if not job_id:
        return jsonify({"error": "job_id parameter is required"})

    update_result = None
    try:
        client = await temporal_client()
        pipeline_workflow = client.get_workflow_handle(workflow_id(job_id))
        update_result = await pipeline_workflow.execute_update(
            update="load_complete_update",
            arg="complete",
        )
    except Exception as e:
        result = f"Update for job_id {job_id} rejected, not valid! {str(e)}"
        return jsonify(result=result)

    result = f"Update for job_id {job_id} accepted: {update_result}"

    return jsonify(result=result)
