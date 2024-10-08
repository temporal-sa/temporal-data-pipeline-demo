import asyncio
from datetime import timedelta
from typing import Sequence, Any
from temporalio import workflow
from temporalio.workflow import info
import temporalio
from temporalio.common import RawValue, RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from activities import extract, validate, transform, load, get_available_task_queue, poll
    from dataobjects import DataPipelineParams, CustomException

@workflow.defn(dynamic=True)
class DataPipelineWorkflowScenarios:
    BUG        = "DataPipelineRecoverableFailure"
    FAILURE    = "DataPipelineNonRecoverableFailure"
    SIGNAL     = "DataPipelineHumanInLoopSignal"
    UPDATE     = "DataPipelineHumanInLoopUpdate"
    VISIBILITY = "DataPipelineAdvancedVisibility"    
    IDEMPOTENCY = "DataPipelineIdempotency"

    def __init__(self) -> None:
        self.load_complete_signal = False
        self.load_complete_update = False
        self._progress = 0

    @workflow.run
    async def run(self, args: Sequence[RawValue]) -> Any:
        input = workflow.payload_converter().from_payload(args[0].payload, DataPipelineParams)
        workflow_type = workflow.info().workflow_type
        workflow.logger.info("Dynamic Data Pipeline workflow started, " + workflow_type)

        workflow.logger.info("Searching for available worker")
        unique_worker_task_queue = await workflow.execute_activity(
            activity=get_available_task_queue,
            start_to_close_timeout=timedelta(seconds=10),
        )
        workflow.logger.info(f"Matching workflow to worker {unique_worker_task_queue}")

        # Set progress to 10%
        self._progress = 10

        # Sleep 2 seconds
        await asyncio.sleep(2)

        # Advanced Visibility scenario    
        if self.VISIBILITY == workflow_type:
            workflow.upsert_search_attributes({"Step": ["validation"]})

        if self.FAILURE == workflow_type:
            input.validation = "blue"

        validation = await workflow.execute_activity(
            validate, 
            input,  
            start_to_close_timeout=timedelta(seconds=300), 
            heartbeat_timeout=timedelta(seconds=20)
        )

        if validation == False:
            workflow.logger.info(f"Validation rejected for: {input.input_filename}") 
            raise ApplicationError(f"Workflow failed due to validation") from CustomException("Validation Failed") 

        # Set progress to 20%
        self._progress = 20

        # Advanced Visibility scenario    
        if self.VISIBILITY == workflow_type:
            workflow.upsert_search_attributes({"Step": ["extract"]})

        activity_output = await workflow.execute_activity(
            extract, 
            input, 
            task_queue=unique_worker_task_queue, 
            start_to_close_timeout=timedelta(seconds=300), 
            heartbeat_timeout=timedelta(seconds=20)
        )
        workflow.logger.info(f"Extract status: {input.input_filename}: {activity_output}")

        # Set progress to 40%
        self._progress = 40

        # Advanced Visibility scenario    
        if self.VISIBILITY == workflow_type:
            workflow.upsert_search_attributes({"Step": ["transform"]})

        activity_output = await workflow.execute_activity(
            transform, 
            input,
            task_queue=unique_worker_task_queue, 
            start_to_close_timeout=timedelta(seconds=300), 
            heartbeat_timeout=timedelta(seconds=20)
        )
        workflow.logger.info(f"Transform status: {input.input_filename}: {activity_output}")

        # Non-Recoverable (bug) Scenario
        if self.BUG == workflow_type:
            # Comment out to fix recoverable scenario
            raise Exception("Workflow bug!")

        # Set progress to 60%
        self._progress = 60

        # Advanced Visibility scenario    
        if self.VISIBILITY == workflow_type:
            workflow.upsert_search_attributes({"Step": ["load"]})

        activity_output = await workflow.execute_activity(
            load, 
            input, 
            task_queue=unique_worker_task_queue, 
            start_to_close_timeout=timedelta(seconds=300), 
            heartbeat_timeout=timedelta(seconds=20)
        )
        workflow.logger.info(f"Load status: {input.input_filename}: {activity_output}")
        
        # Idempotency Scenario
        if self.IDEMPOTENCY == workflow_type:
            print("here")
            activity_output = await workflow.execute_activity(
                load, 
                input, 
                task_queue=unique_worker_task_queue, 
                start_to_close_timeout=timedelta(seconds=300), 
                heartbeat_timeout=timedelta(seconds=20)
            )
            workflow.logger.info(f"Load status: {input.input_filename}: {activity_output}")

        # Set progress to 80%
        self._progress = 80

        # Human In the Loop (signal) scenario
        if self.SIGNAL == workflow_type:
            try:
                await workflow.wait_condition(lambda: self.load_complete_signal, timeout=60)
                workflow.logger.info(f"Received signal that load completed: {input.input_filename} load complete: {self.load_complete_signal}")
            except asyncio.TimeoutError:
             # could return "Load did not complete before timeout."
                raise temporalio.exceptions.ApplicationError("Load did not complete before timeout")

        # Human In the Loop (update) scenario
        elif self.UPDATE == workflow_type:
            try:
                await workflow.wait_condition(lambda: self.load_complete_update, timeout=60)
                workflow.logger.info(f"Received update that load completed: {input.input_filename} load complete: {self.load_complete_update}")
            except asyncio.TimeoutError:
                # could return "Load did not complete before timeout."
                raise temporalio.exceptions.ApplicationError("Load did not complete before timeout")
        else:


            activity_output = await workflow.execute_activity(
                poll, 
                args=[input, workflow_type],
                task_queue=unique_worker_task_queue, 
                start_to_close_timeout=timedelta(seconds=3000), 
                heartbeat_timeout=timedelta(seconds=20), 
                retry_policy=RetryPolicy(initial_interval=timedelta(seconds=2), backoff_coefficient=1)
            )
            workflow.logger.info(f"Poll status: {input.input_filename}: {activity_output}")

        # Advanced Visibility scenario    
        if self.VISIBILITY == workflow_type:
            workflow.upsert_search_attributes({"Step": ["complete"]})        

        # Set progress to 100%
        self._progress = 100        

        return f"Successfully processed: {input.input_filename}!"

    @workflow.signal
    async def load_complete_signal(self, complete: str) -> None:
        self.load_complete_signal = True

    @workflow.update
    async def load_complete_update(self, complete: str) -> None:
        self.load_complete_update = True
        return "Workflow update successful"

    @workflow.query
    def progress(self) -> int:
        return self._progress