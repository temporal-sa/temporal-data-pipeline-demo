import asyncio
import logging
import os
import random
from uuid import UUID

from temporalio import activity
from temporalio.api.enums.v1 import IndexedValueType
from temporalio.api.operatorservice.v1 import AddSearchAttributesRequest, ListSearchAttributesRequest
from temporalio.client import Client
from temporalio.service import RPCError
from temporalio.worker import Worker

from activities import extract, load, poll, transform, validate
from client import TASK_QUEUE, get_client
from dataobjects import IDEMPOTENT_FILE
from DataPipelineWorkflowHappyPath import DataPipelineWorkflowHappyPath
from DataPipelineWorkflowScenarios import DataPipelineWorkflowScenarios

interrupt_event = asyncio.Event()


async def ensure_step_search_attribute(client: Client) -> None:
    """Register the Step keyword search attribute the AdvancedVisibility scenario upserts.

    A local dev server starts without it. Where the namespace's search attributes are
    managed elsewhere (Temporal Cloud), adding it is refused: log that and carry on.
    """
    try:
        existing = await client.operator_service.list_search_attributes(
            ListSearchAttributesRequest(namespace=client.namespace)
        )
        if "Step" in existing.custom_attributes:
            return
        await client.operator_service.add_search_attributes(
            AddSearchAttributesRequest(
                namespace=client.namespace,
                search_attributes={"Step": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD},
            )
        )
        print("Registered the Step search attribute")
    except RPCError as e:
        print(f"Could not register the Step search attribute: {e.message}")


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    # Delete idempotent keys when worker starts
    if os.path.exists(IDEMPOTENT_FILE):
        os.remove(IDEMPOTENT_FILE)

    # Comment line to see non-deterministic functionality
    random.seed(667)

    # Create the host-specific task queue and build task queue selection function
    task_queue: str = f"{TASK_QUEUE}-host-{UUID(int=random.getrandbits(128))}"

    @activity.defn(name="get_available_task_queue")
    async def select_task_queue() -> str:
        """Randomly assign the job to a queue"""
        return task_queue

    # Start client
    client = await get_client()
    await ensure_step_search_attribute(client)

    # Run a worker to distribute the workflows
    run_futures = []
    handle = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[DataPipelineWorkflowHappyPath, DataPipelineWorkflowScenarios],
        activities=[select_task_queue, validate],
    )
    run_futures.append(handle.run())
    print("Base worker started")

    # Run unique task queue for this particular host
    handle = Worker(
        client,
        task_queue=task_queue,
        activities=[
            extract,
            transform,
            load,
            poll,
        ],
    )
    run_futures.append(handle.run())
    # Wait until interrupted
    print(f"Worker {task_queue} started")

    print("All workers started, ctrl+c to exit")
    await asyncio.gather(*run_futures)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        interrupt_event.set()
        loop.run_until_complete(loop.shutdown_asyncgens())
        print("\nShutting down workers")
