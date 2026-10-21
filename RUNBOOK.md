# Runbook: Data Pipeline demo

This runbook is for presenting the demo. The [README](README.md) explains what it is and how it's built.

## What it shows

A file-based ETL job (validate, extract, transform, load, then poll a downstream API) runs as one Temporal Workflow. When a step fails, a Worker dies, or the code has a bug, the job picks up at the step that failed instead of starting over. When a load needs a person's sign-off, the job waits for it.

The demo makes three problems visible:

- **A failure partway through means rerunning the whole pipeline.** The downstream API is down for the first nine calls, or the Worker is killed mid-run. Beats 2 and 3 show the job retrying only the step that failed and finishing.
- **Retries are only safe if the steps can run twice, and some failures shouldn't be retried at all.** Beat 4 runs the load twice and the second run skips. Beat 5 fails for good on bad input. Beat 6 hits a bug in the pipeline code, waits while you fix it, and carries on from where it stopped.
- **Some loads need a person, and operators need to know where each job is.** Beat 7 pauses the job until someone clicks a button, with a 60-second deadline. Beat 8 finds jobs by the step they're on.

## Before you start

- Install [uv](https://docs.astral.sh/uv/), the [Temporal CLI](https://docs.temporal.io/cli), and optionally [just](https://github.com/casey/just) for the shortcuts in the `justfile`.
- Clone the repo and run `uv sync`. If it fails building `cffi`, run `UV_PYTHON=3.12 uv sync`; the tmprl-demo.cloud build pins Python 3.12 for that reason.
- Locally you need no `.env`. The Worker and the web app connect to `localhost:7233`, Namespace `default`.
- **Run one job at a time.** Every job reads and writes the same files under `./demodata` on the Worker, so two jobs running at once delete each other's working files and fail. This holds on tmprl-demo.cloud too, where every presenter shares one Worker.
- To run against Temporal Cloud yourself, the client supports mTLS only (no API keys). Set `TEMPORAL_HOST_URL`, `TEMPORAL_NAMESPACE`, `TEMPORAL_MTLS_TLS_CERT` and `TEMPORAL_MTLS_TLS_KEY` in `.env`, and add the `Step` (Keyword) Search Attribute to the Namespace. `ENCRYPT_PAYLOADS=true` only takes effect with mTLS set. `TEMPORAL_TASK_QUEUE` in `.env.example` isn't read by the code.

### On tmprl-demo.cloud

The demo runs at https://data-pipeline.tmprl-demo.cloud against the project's Temporal Cloud Namespace, whose name starts with `tmprl-dem-cld-data-pipeline`. The `Step` Search Attribute is already there. It is not a crashable demo: you can't kill or restart its Worker, and you can't edit its code, so beats 2 and 6 only work locally. On the hosted copy, show beat 6's stuck Workflow and explain the fix, or skip it.

## Start it

In three terminals, from the repository root:

1. `temporal server start-dev` (or `just temporal-start`)
2. `uv run worker.py` (or `just worker`)
3. `uv run ui/app.py` (or `just ui`)

Then create the Search Attribute once per dev server: `just add-search-attribute`, or `temporal operator search-attribute create --name Step --type Keyword`. The dev server keeps its data in memory, so do this again after each restart.

Open http://localhost:8080. A healthy start shows **Temporal Data Pipeline**, a **Data Format** table (`info.json`, `./demodata`, `poll`, `orange`), a **Choose Scenario** dropdown and a **Run Job** button. The Worker terminal prints `Base worker started`, `Worker worker_specific_task_queue-host-<uuid> started` and `All workers started, ctrl+c to exit`. The Temporal UI is at http://localhost:8233.

## The walkthrough

Each job takes about 20 seconds when nothing fails: a 2-second timer, extract and load at 1–3 seconds each, transform at 1 second per namespace in `info.json` (8 of them), and a 5-second poll. Every beat starts the same way: pick a scenario in **Choose Scenario**, press **Run Job**, and watch the green bar on **Job Processing**. The bar reads a Query, so it moves in steps (10, 20, 40, 60, 80, 100) and can lag a step behind. A finished job goes to **Job Completed** with a **New Job** button; a failed one turns the bar red with `Failed to fetch progress. Status: 500`.

The Job ID is generated for you and is not shown on the form. Find it in the address bar (`job_id=` in the URL). The Workflow ID is `job-<Job ID>`; search for it in the Temporal UI.

### 1. The pipeline runs as one Workflow

1. Choose **HappyPath** and press **Run Job**.
2. The bar fills in about 20 seconds and lands on **Job Completed**.
3. Open `job-<Job ID>` in the Temporal UI. `get_available_task_queue` and `validate` ran on `worker_specific_task_queue-distribution-queue`; extract, transform, load and poll ran on `worker_specific_task_queue-host-<uuid>`, the queue only this Worker polls.
4. Say: "Each step is an Activity. The Workflow first asks which Worker should do the file work, then sends every step that touches those files to that one Worker's queue, because the files are on its disk."

### 2. Kill the Worker mid-run (local only)

1. Choose **HappyPath**, press **Run Job**, and when the bar is partway, press `Ctrl-C` in the Worker terminal. Wait for `Shutting down workers`. If the Worker prints a `KeyboardInterrupt` traceback from inside an Activity and keeps running, the interrupt landed in that Activity's `time.sleep` and failed just that attempt; press `Ctrl-C` again.
2. The bar stops moving and drops to empty: with no Worker, the progress Query fails and the page shows 0.
3. Start the Worker again with `uv run worker.py`. The job finishes and lands on **Job Completed**.
4. Say: "The process running the pipeline just died. Temporal kept the job's state, so the new process carried on from the step it was on. Nothing before that step ran again."
5. For proof, open the Workflow's history. The steps before the kill each appear once. The step that was running when the Worker died shows attempt 2. Its first attempt failed either with `activity Heartbeat timeout`, up to 20 seconds after the Worker died, or with the `KeyboardInterrupt` if the interrupt landed inside it.

This works because the restarted Worker polls the same host queue name; see [Send file work to the Worker that has the files](#send-file-work-to-the-worker-that-has-the-files).

### 3. The downstream API is down, and the job keeps polling

1. Choose **APIFailure** and press **Run Job**.
2. The bar sits at 80 for about 18 seconds, then finishes.
3. While it waits, open the Workflow in the Temporal UI. The pending `poll` Activity shows its attempt count climbing every 2 seconds and the last failure `Poll failed: not found`. It succeeds on attempt 10.
4. Say: "The API was down for the first nine calls. Temporal retried just that call, every two seconds, and the four steps before it didn't run again. Nobody wrote a retry loop."
5. Point out that the history has one `ActivityTaskStarted` for `poll`, not ten. Retries don't add events; the attempt count and last failure live on the pending Activity until it finishes.

### 4. The load is safe to run twice

1. Choose **Idempotency** and press **Run Job**. It finishes like HappyPath.
2. In the Temporal UI, open the history. `load` appears twice. The first result is `"success"`; the second is `"idempotency key <Job ID> found, skipping... "`.
3. Say: "Temporal retries Activities, so an Activity can run more than once. The load checks a key before it writes, so a second run changes nothing."

Temporal runs an Activity at least once, not exactly once; the idempotency key in the Activity is what makes the repeat harmless. Don't repeat the README's "exactly-once" line.

### 5. Bad input fails the job for good

1. Choose **NonRecoverableFailure** and press **Run Job**.
2. A few seconds in, the bar turns red with `Failed to fetch progress. Status: 500`.
3. In the Temporal UI the Workflow is **Failed** with `Workflow failed due to validation`, caused by `Validation Failed`. Extract never ran.
4. Say: "Validation rejected the file. Retrying wouldn't change the answer, so the Workflow fails right away and says why."

### 6. A bug in the pipeline code, fixed without losing the job (local only)

1. Choose **RecoverableFailure** and press **Run Job**.
2. After transform, the bar drops to empty and stays there.
3. In the Temporal UI the Workflow is still **Running**. Its history ends in `WorkflowTaskFailed` with `Workflow bug!`, and the Worker terminal logs the same exception on every retry.
4. Say: "That's a bug in the Workflow code, not bad data. Temporal doesn't fail the job; it holds it at the last good step and keeps retrying until the code is fixed."
5. Fix it: in `DataPipelineWorkflowScenarios.py` around line 108, comment out `raise Exception("Workflow bug!")` and uncomment `pass`. Restart the Worker with `Ctrl-C` and `uv run worker.py`.
6. The job finishes and the page moves to **Job Completed**.
7. For proof, the history lists `extract` and `transform` once each, before the failures, and `load` and `poll` after them. The fixed Worker replayed the history and carried on.
8. Undo the edit afterwards (`git checkout DataPipelineWorkflowScenarios.py`) and restart the Worker, so the scenario fails again next time.

### 7. Wait for a person before finishing

1. Choose **HumanInLoopSignal** and press **Run Job**.
2. At 80, a **Signal Complete Load** button appears with a countdown from 59 seconds.
3. Press **Signal Complete Load** before the countdown ends. The job finishes.
4. Say: "The pipeline stopped and waited for a sign-off. It can wait seconds or weeks, and nothing runs while it waits."
5. Run **HumanInLoopUpdate** next. At 80, press **Update Complete Load**. The page prints `Update for job_id <Job ID> accepted: Workflow update successful`.
6. Say: "A Signal is fire-and-forget. An Update waits for the Workflow to handle it and returns an answer, so the person clicking knows it landed."
7. To show the deadline, run **HumanInLoopSignal** again and don't press the button. After 60 seconds the bar turns red, and the Workflow fails with `Load did not complete before timeout`.
8. For proof, the history shows `TimerStarted`, then `WorkflowExecutionSignaled` (or `WorkflowExecutionUpdateAccepted` and `WorkflowExecutionUpdateCompleted`), then `TimerCanceled`.

You can also send the Signal from a terminal: `just signal-workflow <Job ID>`.

### 8. Find jobs by the step they're on

1. Choose **AdvancedVisibility** and press **Run Job**.
2. While it runs, in the Temporal UI's Workflow list, filter with `Step = "transform"` (or `"load"`). The job appears while it's on that step. When it finishes, `Step = "complete"` finds it.
3. Say: "The Workflow tags itself with the step it's on. Operations can list every job stuck in load, across thousands of runs, without a separate status table."

The README's `Step:validation` example isn't valid List Filter syntax; use `Step = "validation"`.

## Show it in code

All links point at commit [`8394c9c`](https://github.com/temporal-sa/temporal-data-pipeline-demo/commit/8394c9c820c3b7506ce1a6670322c859b6015342). Open a file locally with `code -g <path>:<line>`.

The UI starts `DataPipelineWorkflowHappyPath` for HappyPath and a Workflow type named `DataPipeline<Scenario>` for everything else. A dynamic Workflow, `DataPipelineWorkflowScenarios`, handles all of those types and switches behavior on the type name. The snippets below come from it; the HappyPath Workflow has the same steps without the scenario branches.

### Send file work to the Worker that has the files

**When to use it:** after beat 1, or when someone asks how the steps end up on the machine that has the files.

**Where it is:** [`DataPipelineWorkflowScenarios.py` lines 42–46](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/DataPipelineWorkflowScenarios.py#L42-L46) and [lines 80–86](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/DataPipelineWorkflowScenarios.py#L80-L86) (`code -g DataPipelineWorkflowScenarios.py:42`); the Worker side is [`worker.py` lines 26–35](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/worker.py#L26-L35) (`code -g worker.py:26`)

```python
workflow.logger.info("Searching for available worker")
unique_worker_task_queue = await workflow.execute_activity(
    activity=get_available_task_queue,
    start_to_close_timeout=timedelta(seconds=10),
)
# ...
activity_output = await workflow.execute_activity(
    extract,
    input,
    task_queue=unique_worker_task_queue,
    start_to_close_timeout=timedelta(seconds=300),
    heartbeat_timeout=timedelta(seconds=20),
)
```

```python
# Comment line to see non-deterministic functionality
random.seed(667)

# Create random task queues and build task queue selection function
task_queue: str = f"worker_specific_task_queue-host-{UUID(int=random.getrandbits(128))}"

@activity.defn(name="get_available_task_queue")
async def select_task_queue() -> str:
    """Randomly assign the job to a queue"""
    return task_queue
```

**What you need to understand:**

- This is the [Worker-specific Task Queues](https://docs.temporal.io/design-patterns/worker-specific-taskqueue) pattern. Each Worker process runs two Workers: one on the shared `worker_specific_task_queue-distribution-queue`, which runs the Workflows, `get_available_task_queue` and `validate`, and one on a queue named for this host, which runs extract, transform, load and poll.
- `get_available_task_queue` runs on whichever Worker picks it up and returns that Worker's own queue name. Every later file step passes `task_queue=unique_worker_task_queue`, so it can only run there.
- The seed makes the host queue name the same on every start (`...-host-a93bb0c8-...` with this seed). That's why a restarted Worker in beat 2 picks up the work left on its queue. The code comment says "non-deterministic"; what removing the seed changes is that each start gets a new queue name, and work already sent to the old one waits for a Worker that never comes back.
- In production, each host would get a stable name of its own, and the host-queue Activities would set a `schedule_to_start_timeout` so the Workflow can notice a host that's gone and start the sequence again elsewhere. This demo sets neither.

**Docs:** [Worker-specific Task Queues pattern](https://docs.temporal.io/design-patterns/worker-specific-taskqueue), [Task routing](https://docs.temporal.io/task-routing)

### Retry a flaky API on a fixed interval

**When to use it:** during beat 3, or when someone asks how many retries that took to write.

**Where it is:** [`DataPipelineWorkflowScenarios.py` lines 164–171](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/DataPipelineWorkflowScenarios.py#L164-L171) (`code -g DataPipelineWorkflowScenarios.py:164`); the failure is simulated in [`activities.py` lines 106–115](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/activities.py#L106-L115) (`code -g activities.py:106`)

```python
activity_output = await workflow.execute_activity(
    poll,
    args=[input, workflow_type],
    task_queue=unique_worker_task_queue,
    start_to_close_timeout=timedelta(seconds=3000),
    heartbeat_timeout=timedelta(seconds=20),
    retry_policy=RetryPolicy(initial_interval=timedelta(seconds=2), backoff_coefficient=1),
)
```

```python
@activity.defn
async def poll(input: DataPipelineParams, workflow_type: str) -> str:
    if ErrorAPIUnavailable == workflow_type:
        if activity.info().attempt < 10:
            raise Exception("Poll failed: not found")
        return "polled successfully: found"
    else:
        # Simulate delay
        time.sleep(5)
        return "polled successfully: found"
```

**What you need to understand:**

- The retry policy is the whole retry implementation. `backoff_coefficient=1` keeps every retry 2 seconds apart, which suits polling: "not found yet" isn't an outage that needs backing off. Maximum attempts is unset, so it retries until it succeeds.
- The other Activities set no retry policy and get the default: first retry after 1 second, doubling each time, capped at 100 seconds, unlimited attempts.
- The Activity raises a plain `Exception`; the SDK turns it into a retryable Application Error. To stop retrying on a particular error, raise `ApplicationError(..., non_retryable=True)`, as `extract` and `load` do for file errors, or list the error type in `non_retryable_error_types`.
- Using an Activity failure as "not found yet" is the [polling pattern](https://community.temporal.io/t/what-is-the-best-practice-for-a-polling-activity/328/2) the code comment links. Each attempt is a new Activity Task on the queue; the history records only the final attempt, and the Workflow sees only the success.
- `start_to_close_timeout` is per attempt. To cap the total time spent retrying, a customer would add `schedule_to_close_timeout`.

**Docs:** [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies), [Python error handling](https://docs.temporal.io/develop/python/best-practices/error-handling#configure-custom-retry-policies)

### Notice a dead Worker in 20 seconds

**When to use it:** after beat 2, or when someone asks how Temporal knew the Worker was gone.

**Where it is:** [`DataPipelineWorkflowScenarios.py` lines 96–102](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/DataPipelineWorkflowScenarios.py#L96-L102) (`code -g DataPipelineWorkflowScenarios.py:96`) and [`activities.py` lines 56–65](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/activities.py#L56-L65) (`code -g activities.py:56`)

```python
activity_output = await workflow.execute_activity(
    transform,
    input,
    task_queue=unique_worker_task_queue,
    start_to_close_timeout=timedelta(seconds=300),
    heartbeat_timeout=timedelta(seconds=20),
)
```

```python
workingfilename = input.foldername + "/working/" + Path(input.input_filename).stem + ".csv"
namespacesCSVFile = open(workingfilename, "w+")
namespacesCSVFile.write("Namespace,\n")
for i in namespaces:
    namespacesCSVFile.write(f"{i},\n")
    # Simulate sleep
    time.sleep(1)
    activity.heartbeat(input.input_filename)

namespacesCSVFile.close()
```

**What you need to understand:**

- Each step may take up to 300 seconds (`start_to_close_timeout`), but must heartbeat at least every 20 seconds (`heartbeat_timeout`). When a Worker dies, Temporal sees the heartbeats stop and retries the step after 20 seconds instead of waiting out the 300.
- Activities that finish without heartbeating, such as `poll`'s 5-second sleep, are also covered: the attempt times out 20 seconds after it started.
- The retry goes back on the same host queue, so it runs when that Worker returns. Transform starts its file over; a long-running step could pass progress in `activity.heartbeat(...)` and read it back from `activity.info().heartbeat_details` on the retry to resume mid-file. This demo doesn't.

**Docs:** [Activity heartbeats and timeouts](https://docs.temporal.io/encyclopedia/detecting-activity-failures)

### Make the load safe to run twice

**When to use it:** during beat 4, or after any retry beat when someone asks about duplicate writes.

**Where it is:** [`activities.py` lines 70–99](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/activities.py#L70-L99) (`code -g activities.py:70`)

```python
@activity.defn
async def load(input: DataPipelineParams) -> str:
    try:
        keyExists = is_idempotent(input.key)
    except OSError as e:
        raise ApplicationError(f"Failed to read idempotency key: {e}", non_retryable=True) from e

    if keyExists:
        return "idempotency key " + input.key + " found, skipping... "

    shutil.copy(
        input.foldername + "/working/" + Path(input.input_filename).stem + ".csv",
        input.foldername + "/output/" + Path(input.input_filename).stem + ".csv",
    )
    # ...
    try:
        write_idempotent_key(input.key)
    except OSError as e:
        raise ApplicationError(f"Failed to create idempotency key: {e}", non_retryable=True) from e

    return "success"
```

**What you need to understand:**

- Temporal guarantees an Activity runs at least once. A Worker can finish the write and die before reporting success, and the retry runs it again. The Activity has to make that safe.
- The key is the Job ID, which the UI passes in the Workflow input. The Idempotency scenario simply calls `load` a second time to show the skip ([lines 127–137](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/DataPipelineWorkflowScenarios.py#L127-L137)).
- The keys live in `idempotent_keys.txt` next to the Worker, and the Worker deletes the file when it starts. A real load would use the destination's own mechanism: an upsert, a unique constraint, or a write keyed by the job ID.
- File errors here raise `non_retryable=True` errors, because retrying a missing file won't help.

**Docs:** [Idempotency in Activities](https://docs.temporal.io/activity-definition#idempotency)

### Fail for good on bad input

**When to use it:** during beat 5, or when someone asks how to stop Temporal retrying.

**Where it is:** [`DataPipelineWorkflowScenarios.py` lines 62–71](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/DataPipelineWorkflowScenarios.py#L62-L71) (`code -g DataPipelineWorkflowScenarios.py:62`)

```python
validation = await workflow.execute_activity(
    validate,
    input,
    start_to_close_timeout=timedelta(seconds=300),
    heartbeat_timeout=timedelta(seconds=20),
)

if not validation:
    workflow.logger.info(f"Validation rejected for: {input.input_filename}")
    raise ApplicationError("Workflow failed due to validation") from CustomException("Validation Failed")
```

**What you need to understand:**

- `validate` returns `False` for this input; it doesn't raise. The Workflow turns that answer into a failure by raising `ApplicationError`.
- In the Python SDK, raising `ApplicationError` (or any other Temporal failure) fails the Workflow Execution. Workflows have no retry policy by default, so it stays failed.
- The `from CustomException(...)` becomes the failure's cause, which is the `Validation Failed` line in the Temporal UI.

**Docs:** [Python: raise exceptions from Workflows](https://docs.temporal.io/develop/python/best-practices/error-handling#raise-exceptions-from-workflows), [Errors in Workflows](https://docs.temporal.io/references/failures#errors-in-workflows)

### Suspend on a bug instead of failing

**When to use it:** during beat 6, or when someone asks what happens when a deploy ships a bug.

**Where it is:** [`DataPipelineWorkflowScenarios.py` lines 105–109](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/DataPipelineWorkflowScenarios.py#L105-L109) (`code -g DataPipelineWorkflowScenarios.py:105`)

```python
# Non-Recoverable (bug) Scenario
if self.BUG == workflow_type:
    # Comment out to fix recoverable scenario and uncomment pass
    raise Exception("Workflow bug!")
    # pass
```

**What you need to understand:**

- A plain Python exception in Workflow code fails the Workflow Task, not the Workflow. The Workflow stays Running and the Task retries until the code stops raising. Compare beat 5, where `ApplicationError` fails the Workflow.
- After the first couple of failures, the history stops adding a `WorkflowTaskFailed` event per retry; the pending Workflow Task's attempt count keeps rising instead.
- When the fixed Worker starts, it replays the history: the completed Activities return their recorded results instead of running again, and execution continues past the old `raise`.
- While the Task is failing, Queries fail with `Unable to query workflow due to Workflow Task in failed state`. That's why the progress bar drops to empty.
- The fix here doesn't change which commands the Workflow had already issued, so replay accepts it. A fix that changed earlier steps would need versioning. The section's comment says "Non-Recoverable"; this is the recoverable scenario.
- To make a given exception type fail the Workflow instead, list it in `workflow_failure_exception_types` on the Worker or `failure_exception_types` on `@workflow.defn`.

**Docs:** [Python: trigger a Workflow Task retry](https://docs.temporal.io/develop/python/best-practices/error-handling#raise-exceptions-from-workflows), [Workflow Task failures](https://docs.temporal.io/references/failures#errors-in-workflows)

### Wait for a sign-off with a Signal or an Update

**When to use it:** during beat 7, or when someone asks how approvals work.

**Where it is:** [`DataPipelineWorkflowScenarios.py` lines 142–162](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/DataPipelineWorkflowScenarios.py#L142-L162) and [lines 183–190](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/DataPipelineWorkflowScenarios.py#L183-L190) (`code -g DataPipelineWorkflowScenarios.py:142`)

```python
# Human In the Loop (signal) scenario
if self.SIGNAL == workflow_type:
    try:
        await workflow.wait_condition(lambda: self.signal_received, timeout=60)
        # ...
    except asyncio.TimeoutError as e:
        # could return "Load did not complete before timeout."
        raise temporalio.exceptions.ApplicationError("Load did not complete before timeout") from e

# Human In the Loop (update) scenario
elif self.UPDATE == workflow_type:
    try:
        await workflow.wait_condition(lambda: self.update_received, timeout=60)
        # ...
```

```python
@workflow.signal
async def load_complete_signal(self, complete: str) -> None:
    self.signal_received = True

@workflow.update
async def load_complete_update(self, complete: str) -> str:
    self.update_received = True
    return "Workflow update successful"
```

**What you need to understand:**

- `wait_condition(..., timeout=60)` is a durable wait with a deadline. It shows up as a Timer in the history; the handler sets the flag, the condition becomes true, and the Timer is canceled. A Worker restart during the wait doesn't lose it.
- A Signal is recorded and acknowledged by the server; the sender gets no answer from the Workflow. An Update waits until the Workflow has handled it and returns the handler's value, which is the `accepted: Workflow update successful` text.
- This Update has no validator, so it never rejects a request. The page's "rejected" message appears when the call itself errors, for example after the Workflow has finished (`workflow execution already completed`). A real approval would add a `@load_complete_update.validator` to reject, say, an approval from the wrong person before it reaches the history.
- The 60 seconds is a demo value. An approval in production could wait days; the wait costs no Worker time.

**Docs:** [Python message passing](https://docs.temporal.io/develop/python/workflows/message-passing), [Signals, Queries and Updates](https://docs.temporal.io/encyclopedia/workflow-message-passing)

### Tag each job with the step it's on

**When to use it:** during beat 8, or when someone asks how they'd build a jobs dashboard.

**Where it is:** [`DataPipelineWorkflowScenarios.py` lines 55–57](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/DataPipelineWorkflowScenarios.py#L55-L57) (`code -g DataPipelineWorkflowScenarios.py:55`), repeated before each step

```python
# Advanced Visibility scenario
if self.VISIBILITY == workflow_type:
    workflow.upsert_search_attributes({"Step": ["validation"]})
```

**What you need to understand:**

- `Step` is a custom Keyword Search Attribute. It has to exist in the Namespace before a Workflow sets it.
- Each upsert is recorded in the history and indexed, so a List Filter such as `Step = "load"` finds every job on that step, in the Temporal UI, the CLI (`temporal workflow list --query 'Step = "load"'`), or code.
- The visibility index updates shortly after the event, not instantly.
- This dictionary form is deprecated in the pinned SDK. New code uses a typed key: `workflow.upsert_search_attributes([SearchAttributeKey.for_keyword("Step").value_set("load")])`.
- The progress bar uses a Query instead ([lines 192–194](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/DataPipelineWorkflowScenarios.py#L192-L194)). A Query reads one Workflow and adds nothing to its history; a Search Attribute is for finding many Workflows.

**Docs:** [Python: Search Attributes](https://docs.temporal.io/develop/python/platform/observability#visibility), [Search Attributes](https://docs.temporal.io/search-attribute)

### What's demo-only

When someone asks what they'd change for their own system, these are the parts to leave out:

- **Failure injection by Workflow type.** [`DataPipelineWorkflowScenarios.py` lines 22–29](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/DataPipelineWorkflowScenarios.py#L22-L29): each scenario is a Workflow type name that switches on a branch, such as forcing `validation = "blue"` or raising the bug. `poll` fails its first nine attempts only for `DataPipelineAPIFailure`. A real pipeline is the HappyPath Workflow with the Signal or Update wait added where it needs one.
- **The data.** One small JSON file of namespace names, copied between folders under `./demodata`, with `time.sleep` standing in for work. Every job uses the same files, which is why jobs can't overlap.
- **Blocking sleeps in `async` Activities.** `time.sleep` inside `async def` blocks the Worker's event loop, which is part of why the progress bar lags. Real blocking work would use `def` Activities with an `activity_executor`, or `await asyncio.sleep`.
- **The fixed seed and the key file.** [`worker.py` lines 22–27](https://github.com/temporal-sa/temporal-data-pipeline-demo/blob/8394c9c820c3b7506ce1a6670322c859b6015342/worker.py#L22-L27): the Worker deletes `idempotent_keys.txt` on start and seeds the random host queue name.
- **The web app.** It opens a new Temporal client per request and polls the Query once a second.

## Reset and tear down

- **Between jobs:** press **New Job** on **Job Completed**, or go back to http://localhost:8080. A failed job needs nothing; each run gets a new Job ID.
- **After beat 6:** run `git checkout DataPipelineWorkflowScenarios.py` and restart the Worker, so the bug is back for the next run. Terminate any RecoverableFailure Workflow you didn't fix (`temporal workflow terminate --workflow-id job-<Job ID>`); otherwise its Workflow Task keeps retrying.
- **Clean up files:** `just clean` deletes `idempotent_keys.txt`. Restarting the Worker does the same.
- **To stop:** `Ctrl-C` the web app, the Worker and the dev server. `just reset` stops the dev server and deletes the key file. The dev server's history is in memory and goes with it.
- **On tmprl-demo.cloud:** there's nothing to stop. Make sure you didn't leave a RecoverableFailure Workflow retrying in the shared Namespace; terminate it in the Temporal Cloud UI if you can.

## When something goes wrong

| What you see | Likely cause | Fix |
|---|---|---|
| A job fails in `transform` or `load` with `No such file or directory: './demodata/working/info...'` | Two jobs ran at once and shared the working files, or someone else is presenting on tmprl-demo.cloud | Run one job at a time; start a new one after the first finishes |
| **Run Job** shows an error page | The web app can't reach Temporal on `localhost:7233` | Start the dev server first, then the web app |
| The bar never moves past 0 | The Worker isn't running | Start `uv run worker.py` |
| An AdvancedVisibility job sits at an empty bar, and the Worker logs `BadSearchAttributes: Namespace default has no mapping defined for search attribute Step` | The `Step` Search Attribute doesn't exist; the dev server forgets it on restart | Run `just add-search-attribute`. The stuck job carries on at its next Workflow Task retry |
| `Ctrl-C` prints a `KeyboardInterrupt` traceback but the Worker keeps running | The interrupt landed inside an Activity's blocking sleep, and the SDK recorded it as that attempt's failure | Press `Ctrl-C` again |
| `just add-search-attribute` or `just signal-workflow` reports an authentication error or a Cloud address | Your Temporal CLI profile or `TEMPORAL_ADDRESS` points at another server | Add `--address localhost:7233 --namespace default` to the `temporal` command, or clear the profile |
| The progress bar lags a step behind, or briefly drops | The Query waits behind blocking Activity sleeps, or fails while the Worker restarts | Expected; check the Temporal UI for the real state |
| The Signal or Update button never appears | The page shows buttons only when the progress reads exactly 80 | Send it from a terminal: `just signal-workflow <Job ID>` |
| After a Worker restart, a job waits at the step it was on | Its Activity is waiting for the 20-second heartbeat timeout before retrying | Wait up to 20 seconds |
| `uv run ui/app.py` fails with "address already in use" | Something else is on port 8080 | Stop it; the port is fixed in `ui/app.py` |

## Keeping this runbook current

The code links point at a fixed commit. When a linked file changes, update its link, line numbers, and snippet together. Before an event, list the linked files that have changed:

```sh
grep -oE 'blob/[0-9a-f]{40}/[^#)]+' RUNBOOK.md | sort -u | while IFS=/ read -r _ sha file; do
  git diff --quiet "$sha" HEAD -- "$file" || echo "changed since ${sha:0:7}: $file"
done
```
