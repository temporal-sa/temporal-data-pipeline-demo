import asyncio
import json
import os
import random
import shutil
from pathlib import Path

from temporalio import activity
from temporalio.exceptions import ApplicationError

from dataobjects import IDEMPOTENT_FILE, DataPipelineParams

ErrorAPIUnavailable = "DataPipelineAPIFailure"


@activity.defn
async def get_available_task_queue() -> str:
    """Just a stub for typedworkflow invocation."""
    raise NotImplementedError


@activity.defn
async def validate(input: DataPipelineParams) -> bool:
    if input.validation == "blue":
        return False
    else:
        return True


@activity.defn
async def extract(input: DataPipelineParams) -> str:
    try:
        initialize(input.foldername, input.key)
    except OSError as e:
        raise ApplicationError(f"Initialization failed: {e}", non_retryable=True) from e

    shutil.copy(
        input.foldername + "/source/" + input.input_filename,
        job_dir(input.foldername, "working", input.key) + input.input_filename,
    )

    # Simulate random sleep
    await asyncio.sleep(random.randint(1, 3))
    activity.heartbeat(input.input_filename)

    return "success"


@activity.defn
async def transform(input: DataPipelineParams) -> str:
    try:
        namespaces = get_namespaces(input.foldername, input.key, input.input_filename)
    except (OSError, json.JSONDecodeError, KeyError) as e:
        raise ApplicationError(f"Failed to load namespaces from json file: {e}", non_retryable=True) from e

    workingfilename = job_dir(input.foldername, "working", input.key) + Path(input.input_filename).stem + ".csv"
    namespacesCSVFile = open(workingfilename, "w+")
    namespacesCSVFile.write("Namespace,\n")
    for i in namespaces:
        namespacesCSVFile.write(f"{i},\n")
        # Simulate sleep
        await asyncio.sleep(1)
        activity.heartbeat(input.input_filename)

    namespacesCSVFile.close()

    return "success"


@activity.defn
async def load(input: DataPipelineParams) -> str:
    try:
        keyExists = is_idempotent(input.key)
    except OSError as e:
        raise ApplicationError(f"Failed to read idempotency key: {e}", non_retryable=True) from e

    if keyExists:
        return "idempotency key " + input.key + " found, skipping... "

    shutil.copy(
        job_dir(input.foldername, "working", input.key) + Path(input.input_filename).stem + ".csv",
        job_dir(input.foldername, "output", input.key) + Path(input.input_filename).stem + ".csv",
    )

    # Simulate random sleep
    await asyncio.sleep(random.randint(1, 3))
    activity.heartbeat(input.input_filename)

    try:
        cleanup(input.foldername, input.key)
    except OSError as e:
        raise ApplicationError(f"Cleanup failed: {e}", non_retryable=True) from e

    try:
        write_idempotent_key(input.key)
    except OSError as e:
        raise ApplicationError(f"Failed to create idempotency key: {e}", non_retryable=True) from e

    return "success"


# this activity simulates polling for demo purposes
# see https://community.temporal.io/t/what-is-the-best-practice-for-a-polling-activity/328/2
# it throws an exception 90% of the time (simulating "not found")
# 10% of the time it simulates "found" and returns
@activity.defn
async def poll(input: DataPipelineParams, workflow_type: str) -> str:
    if ErrorAPIUnavailable == workflow_type:
        if activity.info().attempt < 10:
            raise Exception("Poll failed: not found")
        return "polled successfully: found"
    else:
        # Simulate delay
        await asyncio.sleep(5)
        return "polled successfully: found"


def job_dir(datafolder: str, stage: str, key: str) -> str:
    """The job's own directory under working/ or output/, so concurrent jobs never share files."""
    return f"{datafolder}/{stage}/{key}/"


def initialize(datafolder: str, key: str) -> None:
    """Create the job's working and output directories, removing what an earlier attempt left."""
    shutil.rmtree(job_dir(datafolder, "working", key), ignore_errors=True)
    shutil.rmtree(job_dir(datafolder, "output", key), ignore_errors=True)

    os.makedirs(job_dir(datafolder, "working", key), exist_ok=True)
    os.makedirs(job_dir(datafolder, "output", key), exist_ok=True)


def cleanup(datafolder: str, key: str) -> None:
    """Remove the job's working files."""
    shutil.rmtree(job_dir(datafolder, "working", key), ignore_errors=True)


def get_namespaces(datafolder: str, key: str, filename: str) -> list[str]:
    """Load namespaces from JSON file."""
    namespaces = []
    with open(job_dir(datafolder, "working", key) + filename) as namespacesJSONFile:
        namespacesDict = json.load(namespacesJSONFile)
        for i in namespacesDict["namespaces"]:
            namespaces.append(i)
    return namespaces


def is_idempotent(key: str) -> bool:
    """Check if a key has already been processed (idempotency check)."""
    if not os.path.exists(IDEMPOTENT_FILE):
        return False
    with open(IDEMPOTENT_FILE) as file:
        keys = file.read().splitlines()
        return key in keys


def write_idempotent_key(key: str) -> None:
    """Write a key to the idempotency file."""
    with open(IDEMPOTENT_FILE, "a") as file:
        file.write(f"{key}\n")
