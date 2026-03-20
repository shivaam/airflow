# Issue #56149: DockerOperator on_kill Does Not Respect auto_remove='force'

## Problem Description

When a DockerOperator task configured with `auto_remove='force'` is killed via the Airflow UI
(e.g., "Mark as Failed" or "Clear"), the `on_kill()` method only **stops** the container but does
not **remove** it. This leaves orphaned containers on the Docker host.

### Symptoms

1. Container remains in `Exited (137)` state after task kill
2. Subsequent DAG runs with the same `container_name` fail with **409 Conflict** because the
   stopped container still exists
3. Manual `docker rm` is required to clean up

### Reproduction

```python
from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG("docker_test", ...) as dag:
    task = DockerOperator(
        task_id="long_running",
        image="alpine:latest",
        command=["sleep", "300"],
        container_name="my_container",
        auto_remove="force",      # Should force-remove on any exit
        docker_url="unix:///var/run/docker.sock",
    )
```

Steps:
1. Trigger the DAG and wait for the task to enter `running` state
2. In the Airflow UI, select "Clear" with "Mark as Failed" checked
3. Run `docker ps -a` on the host
4. **Expected**: Container is removed
5. **Actual**: Container shows `Exited (137)` and persists

---

## Root Cause Analysis

### The Broken Code Path: `on_kill()`

**File**: `providers/docker/src/airflow/providers/docker/operators/docker.py:525-531`

```python
def on_kill(self) -> None:
    if self.hook.client_created:
        self.log.info("Stopping docker container")
        if self.container is None:
            self.log.info("Not attempting to kill container as it was not created")
            return
        self.cli.stop(self.container["Id"])
        # ← Missing: no auto_remove check, no container removal
```

The `on_kill()` method only calls `self.cli.stop()`. It completely ignores the `auto_remove`
setting.

### The Working Code Path: Normal Execution `finally` Block

**File**: `providers/docker/src/airflow/providers/docker/operators/docker.py:457-461`

```python
finally:
    if self.auto_remove == "success":
        self.cli.remove_container(self.container["Id"])
    elif self.auto_remove == "force":
        self.cli.remove_container(self.container["Id"], force=True)
```

During normal execution (success or failure), the `_run_image_with_mounts()` method's `finally`
block properly handles container removal based on the `auto_remove` setting.

### Why the `finally` Block Doesn't Save Us

When `on_kill()` is triggered:

1. The task runner sends a kill signal to the running task
2. `on_kill()` is called, which stops the container
3. The `_run_image_with_mounts()` method may still be running (waiting on `self.cli.wait()`)
4. The `wait()` call may return/raise after the container is stopped
5. The `finally` block **may** execute, but by this point the container is already stopped with
   exit code 137
6. The `finally` block tries to remove the container, but depending on timing and the
   `auto_remove` value:
   - `"success"`: Won't remove (task didn't succeed)
   - `"force"`: Would remove, but **race condition** — the `finally` block may not execute
     if the process is killed before reaching it

In practice, the `finally` block execution is **not guaranteed** when the task is killed
externally, making `on_kill()` the correct place to handle cleanup.

---

## Architecture of the DockerOperator

### Container Lifecycle

```
DockerOperator.execute()
    │
    ▼
_run_image()
    │
    ▼
_run_image_with_mounts()
    │
    ├─→ self.cli.create_container(...)       # auto_remove=False (line 405)
    │       └─→ Docker daemon does NOT auto-remove
    │       └─→ Airflow manages container lifecycle
    │
    ├─→ self.cli.start(container)
    │
    ├─→ try:
    │       └─→ self.cli.wait(container, timeout)
    │       └─→ Check exit code
    │       └─→ Return result
    │
    └─→ finally:                              # Normal cleanup (line 457)
            ├─→ auto_remove == "success" → remove_container()
            └─→ auto_remove == "force"   → remove_container(force=True)


Kill signal received:
    │
    ▼
on_kill()                                     # Signal-based cleanup (line 525)
    │
    └─→ self.cli.stop(container)              # Only stops, doesn't remove
         └─→ Container enters "Exited (137)"
         └─→ ⚠️ Container persists on disk
```

### Key Design Decisions

1. **`auto_remove=False` at container creation** (line 405): The Docker daemon's auto-remove
   feature is intentionally disabled. Airflow manages the container lifecycle to control when
   removal happens (based on success/failure/force settings).

2. **Three `auto_remove` modes** (line 255):
   - `"never"` (default): Container persists after execution
   - `"success"`: Remove only on successful completion
   - `"force"`: Always remove, regardless of exit status

3. **`on_kill()` callback**: Invoked by the task runner (supervisor) when the task receives a
   kill signal (SIGTERM/SIGKILL). It's the last chance to clean up resources.

---

## Proposed Solution

### Fix: Add `auto_remove` Handling to `on_kill()`

```python
def on_kill(self) -> None:
    if self.hook.client_created:
        self.log.info("Stopping docker container")
        if self.container is None:
            self.log.info("Not attempting to kill container as it was not created")
            return
        self.cli.stop(self.container["Id"])
        if self.auto_remove == "force":
            try:
                self.cli.remove_container(self.container["Id"], force=True)
            except APIError:
                self.log.warning("Failed to remove container %s", self.container["Id"])
```

**Key decisions**:
- Only `"force"` triggers removal in `on_kill()` — `"success"` should not remove on kill since
  the task did not succeed
- Wrap in `try/except APIError` to handle race conditions (container may already be removed by
  the `finally` block or Docker daemon)

**Effort**: Tiny — 5 lines added to `on_kill()`.

**File to modify**:
- `providers/docker/src/airflow/providers/docker/operators/docker.py` — `on_kill()` at line 525

**Existing PR**: Draft PR #63737 addresses this issue.

---

## Blind Spots and Potential Problems

1. **Race condition with `finally` block**: If both `on_kill()` and the `finally` block execute,
   there will be a double-remove attempt. The `try/except APIError` handles this, but we should
   verify that `APIError` (from `docker.errors`) is the correct exception for "container not
   found" scenarios. `docker.errors.NotFound` (a subclass of `APIError`) is more precise.

2. **`DockerSwarmOperator`**: This is a separate operator at
   `providers/docker/src/airflow/providers/docker/operators/docker_swarm.py`. It may have its
   own `on_kill()` with the same issue. Should be checked.

3. **Timing of stop vs. remove**: `self.cli.stop()` sends SIGTERM and waits for the container to
   exit (with a timeout). If the container hasn't fully stopped when `remove_container(force=True)`
   is called, the `force=True` parameter ensures removal regardless. This should be safe.

4. **Container not yet created**: The `self.container is None` check already handles the case
   where `on_kill()` is called before the container is created.

5. **Network cleanup**: Stopping/removing a container should clean up associated networks, but
   if the container is part of a custom network, there may be edge cases. This is unlikely to be
   affected by this fix.

6. **Test coverage**: Tests should cover:
   - `on_kill()` with `auto_remove="force"` — container should be removed
   - `on_kill()` with `auto_remove="success"` — container should NOT be removed
   - `on_kill()` with `auto_remove="never"` — container should NOT be removed
   - `on_kill()` when container is already removed (race condition) — no error
   - `on_kill()` when container was never created — no error (existing behavior)
