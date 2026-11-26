# Temporal Data Pipeline Justfile
# Comprehensive development and operational toolkit

# Show available commands (default recipe)
default:
    @just --list

# Code Quality & Development Recipes

# Run ruff linting checks
lint:
    uv run ruff check .

# Format code with ruff
format:
    uv run ruff format .

# Run mypy type checking
typecheck:
    uv run mypy --strict .

# Run all code quality checks
check-all: lint format typecheck

# Environment & Dependencies

# Install base dependencies
install:
    uv sync

# Install with development dependencies
install-dev:
    uv sync --extra dev

# Copy .env.example to .env if it doesn't exist
env-setup:
    @if [ ! -f .env ]; then cp .env.example .env && echo "Created .env from .env.example"; else echo ".env already exists"; fi

# Temporal Services

# Start local Temporal dev server
temporal-start:
    temporal server start-dev

# Stop local Temporal dev server
temporal-stop:
    @echo "Stopping Temporal server..."
    @pkill -f "temporal server start-dev" || echo "No Temporal server process found"

# Application Services

# Start the Temporal worker
worker:
    uv run worker.py

# Start the Flask UI application
ui:
    uv run gunicorn -b 0.0.0.0:8080 --threads 8 ui.app:app

# Workflow Operations

# Start a workflow with specific scenario and job ID
start-workflow SCENARIO JOB_ID:
    @echo "Starting workflow: {{SCENARIO}} with job ID: {{JOB_ID}}"
    temporal workflow start \
        --workflow-id "job-{{JOB_ID}}" \
        --type "DataPipeline{{SCENARIO}}" \
        --task-queue "${TEMPORAL_TASK_QUEUE:-data-pipeline}" \
        --input '{"input_filename": "info.json", "foldername": "./demodata", "poll_or_wait": "poll", "validation": "orange", "scenario": "{{SCENARIO}}", "key": "{{JOB_ID}}"}'

# Query workflow progress by job ID
query-workflow JOB_ID:
    @echo "Querying workflow progress for job ID: {{JOB_ID}}"
    temporal workflow query \
        --workflow-id "job-{{JOB_ID}}" \
        --type "progress"

# Send completion signal to workflow
signal-workflow JOB_ID:
    @echo "Sending load_complete_signal to workflow: {{JOB_ID}}"
    temporal workflow signal \
        --workflow-id "job-{{JOB_ID}}" \
        --name "load_complete_signal" \
        --input '"completed"'

# Get workflow execution status
workflow-status JOB_ID:
    @echo "Getting status for workflow: {{JOB_ID}}"
    temporal workflow describe \
        --workflow-id "job-{{JOB_ID}}"

# Utility Commands

# Clean up temporary files and idempotent keys
clean:
    @echo "Cleaning up temporary files..."
    @if [ -f "idempotent_keys.txt" ]; then rm idempotent_keys.txt && echo "Removed idempotent_keys.txt"; fi
    @echo "Cleanup complete"

# Stop all services and clean up
reset: temporal-stop clean
    @echo "Reset complete - all services stopped and cleaned up"