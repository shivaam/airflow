"""
Fork FD Tracer: Proves whether inspect() causes forked children to inherit
parent Redis sockets, and whether that breaks task dispatch.

This script:
1. Creates a Celery app mimicking Airflow's exact construction (config_source=)
2. Registers a task
3. Optionally calls inspect() (controlled by --inspect flag)
4. Dumps all socket FDs BEFORE fork
5. Calls worker_main() which forks prefork children
6. Uses os.register_at_fork() to dump FDs in the child AFTER fork
7. Uses celery signals to trace worker lifecycle events
8. Sends a task from a separate thread and checks if it completes

Run on EC2:
    source /opt/airflow-scripts/env.sh

    # Test A: WITH inspect (should show inherited FDs, task MAY get stuck)
    python3 dev/fork_fd_tracer.py --inspect --hostname "tracer@%h"

    # Test B: WITHOUT inspect (clean FDs, task should complete)
    python3 dev/fork_fd_tracer.py --hostname "tracer@%h"

    # Test C: WITH inspect + kombu.pools.reset (should be clean like B)
    python3 dev/fork_fd_tracer.py --inspect --reset --hostname "tracer@%h"

    # Test D: WITHOUT hostname (baseline, should always work)
    python3 dev/fork_fd_tracer.py
"""
import argparse
import os
import sys
import threading
import time

# Must register fork hooks BEFORE celery imports (which may fork)
PARENT_SOCKET_FDS = {}
CHILD_SOCKET_FDS = {}


def _get_socket_fds():
    """Get all socket file descriptors for the current process."""
    fds = {}
    fd_dir = f"/proc/{os.getpid()}/fd"
    if not os.path.exists(fd_dir):
        return fds
    for name in os.listdir(fd_dir):
        try:
            target = os.readlink(f"{fd_dir}/{int(name)}")
            if "socket" in target:
                fds[int(name)] = target
        except (ValueError, OSError):
            pass
    return fds


def _before_fork():
    global PARENT_SOCKET_FDS
    PARENT_SOCKET_FDS = _get_socket_fds()
    _log("FORK", f"PARENT (pid={os.getpid()}) BEFORE FORK: {len(PARENT_SOCKET_FDS)} socket FDs: {PARENT_SOCKET_FDS}")


def _after_fork_in_child():
    global CHILD_SOCKET_FDS
    CHILD_SOCKET_FDS = _get_socket_fds()
    _log("FORK", f"CHILD (pid={os.getpid()}, ppid={os.getppid()}) AFTER FORK: {len(CHILD_SOCKET_FDS)} socket FDs: {CHILD_SOCKET_FDS}")

    # Check which FDs were inherited from parent
    inherited = set(PARENT_SOCKET_FDS.keys()) & set(CHILD_SOCKET_FDS.keys())
    new_fds = set(CHILD_SOCKET_FDS.keys()) - set(PARENT_SOCKET_FDS.keys())
    _log("FORK", f"CHILD inherited {len(inherited)} parent FDs: {sorted(inherited)}")
    _log("FORK", f"CHILD new FDs (opened after fork): {sorted(new_fds)}")

    if inherited:
        _log("FORK", "*** SHARED SOCKET FDs DETECTED — parent and child share the same underlying sockets ***")
        for fd in sorted(inherited):
            _log("FORK", f"  FD {fd}: parent={PARENT_SOCKET_FDS[fd]} child={CHILD_SOCKET_FDS[fd]}")


os.register_at_fork(
    before=_before_fork,
    after_in_child=_after_fork_in_child,
)


def _log(tag, msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [{tag}] (pid={os.getpid()}) {msg}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Fork FD tracer for Celery hostname bug")
    parser.add_argument("--inspect", action="store_true", help="Call inspect() before worker_main()")
    parser.add_argument("--reset", action="store_true", help="Call kombu.pools.reset() after inspect()")
    parser.add_argument("--hostname", type=str, default=None, help="Worker hostname (e.g., 'tracer@%%h')")
    parser.add_argument("--broker", type=str, default="redis://localhost:6379/0")
    parser.add_argument("--backend", type=str, default="redis://localhost:6379/1")
    args = parser.parse_args()

    _log("MAIN", f"PID={os.getpid()}")
    _log("MAIN", f"Options: inspect={args.inspect}, reset={args.reset}, hostname={args.hostname}")

    import kombu.pools
    from celery import Celery
    from celery.signals import (
        worker_process_init,
        worker_ready,
        task_prerun,
        task_postrun,
        task_received,
    )

    # ── Create app mimicking Airflow's construction ──
    config = {
        "broker_url": args.broker,
        "result_backend": args.backend,
        "accept_content": ["json"],
        "worker_prefetch_multiplier": 1,
        "task_acks_late": True,
        "task_track_started": True,
    }
    app = Celery("fork_tracer", config_source=config)

    @app.task(name="tracer_task")
    def tracer_task(msg):
        _log("TASK", f"EXECUTING: {msg}")
        return f"done: {msg}"

    # ── Celery signals for lifecycle tracing ──
    @worker_process_init.connect
    def on_worker_process_init(**kwargs):
        fds = _get_socket_fds()
        _log("SIGNAL", f"worker_process_init: pid={os.getpid()}, socket_fds={fds}")

    @worker_ready.connect
    def on_worker_ready(**kwargs):
        _log("SIGNAL", f"worker_ready: pid={os.getpid()}")
        # Send a task 5 seconds after worker is ready
        threading.Timer(5.0, _send_task, args=(app,)).start()

    @task_received.connect
    def on_task_received(sender=None, request=None, **kwargs):
        _log("SIGNAL", f"task_received: {request.id if request else 'unknown'}")

    @task_prerun.connect
    def on_task_prerun(sender=None, task_id=None, **kwargs):
        _log("SIGNAL", f"task_prerun: {task_id} in pid={os.getpid()}")

    @task_postrun.connect
    def on_task_postrun(sender=None, task_id=None, state=None, **kwargs):
        _log("SIGNAL", f"task_postrun: {task_id} state={state} in pid={os.getpid()}")

    # ── State dump ──
    _log("STATE", f"INITIAL: app.amqp._producer_pool = {app.amqp._producer_pool}")
    _log("STATE", f"INITIAL: kombu.pools.connections has {len(kombu.pools.connections)} entries")
    _log("STATE", f"INITIAL: socket FDs = {_get_socket_fds()}")

    # ── Optional inspect() call (mimics the duplicate hostname check) ──
    if args.inspect:
        _log("INSPECT", "Calling app.control.inspect().active_queues()...")
        result = app.control.inspect().active_queues()
        _log("INSPECT", f"Result: {result}")
        _log("STATE", f"AFTER INSPECT: app.amqp._producer_pool = {app.amqp._producer_pool}")
        _log("STATE", f"AFTER INSPECT: kombu.pools.connections has {len(kombu.pools.connections)} entries")
        _log("STATE", f"AFTER INSPECT: socket FDs = {_get_socket_fds()}")

        if args.reset:
            _log("RESET", "Calling kombu.pools.reset()...")
            kombu.pools.reset()
            _log("STATE", f"AFTER RESET: app.amqp._producer_pool = {app.amqp._producer_pool}")
            _log("STATE", f"AFTER RESET: kombu.pools.connections has {len(kombu.pools.connections)} entries")
            _log("STATE", f"AFTER RESET: socket FDs = {_get_socket_fds()}")
    else:
        _log("INSPECT", "SKIPPING inspect() (--inspect not set)")

    # ── Start worker ──
    _log("STATE", f"BEFORE worker_main: app.amqp._producer_pool = {app.amqp._producer_pool}")
    _log("STATE", f"BEFORE worker_main: socket FDs = {_get_socket_fds()}")

    options = [
        "worker",
        "-O", "fair",
        "--queues", "default",
        "--concurrency", "1",
        "--loglevel", "INFO",
    ]
    if args.hostname:
        options.extend(["--hostname", args.hostname])

    _log("MAIN", f"Starting worker_main with options: {options}")
    app.worker_main(options)


def _send_task(app):
    """Send a test task after worker is ready."""
    _log("SENDER", "Sending tracer_task...")
    try:
        result = app.send_task("tracer_task", args=["hello from fork tracer"], queue="default")
        _log("SENDER", f"Task sent: id={result.id}")

        # Wait for result with timeout
        try:
            value = result.get(timeout=30)
            _log("SENDER", f"*** TASK COMPLETED: {value} ***")
        except Exception as e:
            _log("SENDER", f"*** TASK FAILED/TIMEOUT: {e} ***")
            _log("SENDER", f"Task state: {result.state}")
    except Exception as e:
        _log("SENDER", f"Error sending task: {e}")


if __name__ == "__main__":
    main()
