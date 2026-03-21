"""
Deep diagnostic for Celery hostname bug #59707.

Instruments fork behavior, Celery signals, kombu pool lifecycle, and
pool dispatch to trace exactly why inspect() before worker_main() breaks
task dispatch — and why it only happens in Airflow's context.

Run on EC2:
  source /opt/airflow-scripts/env.sh
  # Test A: Airflow context (uses Airflow's module-level app + config)
  python3 ~/airflow/dev/deep_celery_debug.py airflow
  # Test B: Standalone context (plain Celery app, same broker)
  python3 ~/airflow/dev/deep_celery_debug.py standalone
  # Test C: Standalone but with Airflow's exact celery config
  python3 ~/airflow/dev/deep_celery_debug.py standalone-airflow-config
"""
import os
import sys
import threading
import time

# ── Helpers ──────────────────────────────────────────────────────────

def get_socket_fds():
    fds = {}
    fd_dir = f"/proc/{os.getpid()}/fd"
    if os.path.exists(fd_dir):
        for name in os.listdir(fd_dir):
            try:
                target = os.readlink(f"{fd_dir}/{int(name)}")
                if "socket" in target:
                    fds[int(name)] = target
            except (ValueError, OSError):
                pass
    return fds


def log(msg, *args):
    ts = time.strftime("%H:%M:%S")
    pid = os.getpid()
    formatted = msg % args if args else msg
    print(f"[{ts}] [pid={pid}] {formatted}", flush=True)


def dump_kombu_pools(label):
    import kombu.pools
    log("=== %s: kombu.pools ===", label)
    log("  connections: %s", dict(kombu.pools.connections))
    log("  producers: %s", dict(kombu.pools.producers))
    for key, pool in kombu.pools.connections.items():
        log("  conn pool key=%s closed=%s", key, getattr(pool, '_closed', '?'))
        resource = getattr(pool, '_resource', None)
        if resource:
            for i, item in enumerate(list(getattr(resource, 'queue', []))):
                connected = getattr(item, 'connected', '?')
                transport = getattr(item, 'transport', None)
                log("    [%d] %s connected=%s transport=%s", i, type(item).__name__, connected, transport)


def dump_app_state(label, app):
    log("=== %s: app state ===", label)
    log("  app: %s", app)
    log("  app.main: %s", app.main)
    log("  app.conf.broker_url: %s", app.conf.broker_url)
    log("  app.conf.result_backend: %s", str(app.conf.result_backend)[:80])
    log("  app.amqp._producer_pool: %s", app.amqp._producer_pool)
    pool = app.pool
    log("  app.pool: %s (limit=%s, closed=%s)", pool, getattr(pool, 'limit', '?'), getattr(pool, '_closed', '?'))
    resource = getattr(pool, '_resource', None)
    if resource:
        qsize = resource.qsize() if hasattr(resource, 'qsize') else '?'
        log("  app.pool._resource qsize=%s", qsize)
        for i, item in enumerate(list(getattr(resource, 'queue', []))):
            connected = getattr(item, 'connected', '?') if hasattr(item, 'connected') else 'lazy'
            log("    [%d] %s connected=%s id=%s", i, type(item).__name__, connected, id(item))
    log("  socket FDs: %s", get_socket_fds())


# ── Fork tracing ─────────────────────────────────────────────────────

def install_fork_hooks():
    """Register at-fork hooks to trace what children inherit."""
    import kombu.pools

    def before_fork():
        log(">>> BEFORE FORK: pid=%s socket_fds=%s", os.getpid(), get_socket_fds())
        log(">>> BEFORE FORK: kombu.pools.connections keys=%s", list(kombu.pools.connections.keys()))

    def after_in_child():
        log("<<< CHILD AFTER FORK: pid=%s ppid=%s socket_fds=%s", os.getpid(), os.getppid(), get_socket_fds())
        log("<<< CHILD: kombu.pools.connections keys=%s", list(kombu.pools.connections.keys()))

    def after_in_parent():
        log("--- PARENT AFTER FORK: pid=%s socket_fds=%s", os.getpid(), get_socket_fds())

    os.register_at_fork(
        before=before_fork,
        after_in_child=after_in_child,
        after_in_parent=after_in_parent,
    )
    log("Fork hooks installed")


# ── Celery signal tracing ────────────────────────────────────────────

def install_celery_signals():
    """Hook into Celery worker lifecycle signals."""
    from celery.signals import (
        worker_init,
        worker_process_init,
        worker_ready,
        task_received,
        task_prerun,
        task_postrun,
        task_retry,
        task_failure,
        task_revoked,
    )

    @worker_init.connect
    def on_worker_init(sender, **kwargs):
        log("SIGNAL worker_init: sender=%s pid=%s", sender, os.getpid())

    @worker_process_init.connect
    def on_worker_process_init(sender, **kwargs):
        log("SIGNAL worker_process_init: pid=%s ppid=%s socket_fds=%s",
            os.getpid(), os.getppid(), get_socket_fds())
        import kombu.pools
        log("SIGNAL worker_process_init: kombu.pools.connections=%s",
            list(kombu.pools.connections.keys()))

    @worker_ready.connect
    def on_worker_ready(sender, **kwargs):
        log("SIGNAL worker_ready: sender=%s pid=%s", sender, os.getpid())

    @task_received.connect
    def on_task_received(sender, request, **kwargs):
        log("SIGNAL task_received: task=%s id=%s pid=%s", sender, request.id, os.getpid())

    @task_prerun.connect
    def on_task_prerun(sender, task_id, task, **kwargs):
        log("SIGNAL task_prerun: task=%s id=%s pid=%s", task, task_id, os.getpid())

    @task_postrun.connect
    def on_task_postrun(sender, task_id, task, retval, state, **kwargs):
        log("SIGNAL task_postrun: task=%s id=%s state=%s pid=%s", task, task_id, state, os.getpid())

    @task_failure.connect
    def on_task_failure(sender, task_id, exception, **kwargs):
        log("SIGNAL task_failure: id=%s exception=%s pid=%s", task_id, exception, os.getpid())

    @task_revoked.connect
    def on_task_revoked(sender, request, terminated, signum, expired, **kwargs):
        log("SIGNAL task_revoked: id=%s terminated=%s expired=%s pid=%s",
            request.id, terminated, expired, os.getpid())

    log("Celery signals installed")


# ── Main ─────────────────────────────────────────────────────────────

def run_airflow_context(do_inspect=True):
    """Test with Airflow's actual module-level Celery app."""
    log("Loading Airflow celery app...")
    from airflow.providers.celery.executors.celery_executor import app as celery_app

    install_fork_hooks()
    install_celery_signals()

    dump_app_state("AIRFLOW BEFORE inspect", celery_app)
    dump_kombu_pools("AIRFLOW BEFORE inspect")

    if do_inspect:
        log("Calling celery_app.control.inspect().active_queues()...")
        result = celery_app.control.inspect().active_queues()
        log("inspect result: %s", result)

        dump_app_state("AIRFLOW AFTER inspect", celery_app)
        dump_kombu_pools("AIRFLOW AFTER inspect")
    else:
        log("Skipping inspect (control test)")

    log("Starting worker_main with --hostname...")
    celery_app.worker_main([
        "worker", "-O", "fair",
        "--queues", "default",
        "--concurrency", "1",
        "--loglevel", "INFO",
        "--hostname", "debug@%h",
    ])


def run_standalone(use_airflow_config=False, use_visibility_timeout=False):
    """Test with a fresh Celery app (not Airflow's)."""
    from celery import Celery

    broker = "redis://localhost:6379/0"

    if use_airflow_config:
        log("Creating standalone app WITH Airflow's celery config...")
        app = Celery("standalone_airflow", broker=broker)
        app.conf.update(
            accept_content=["json"],
            event_serializer="json",
            worker_prefetch_multiplier=1,
            task_acks_late=True,
            task_default_queue="default",
            task_track_started=True,
            result_backend="redis://localhost:6379/1",  # Use Redis not PG for standalone
            worker_concurrency=1,
            broker_transport_options={"visibility_timeout": 86400},
        )
    elif use_visibility_timeout:
        log("Creating standalone app WITH visibility_timeout (Airflow's key difference)...")
        app = Celery("standalone_vt", broker=broker, backend="redis://localhost:6379/1")
        app.conf.update(
            broker_transport_options={"visibility_timeout": 86400},
        )
    else:
        log("Creating standalone app with minimal config...")
        app = Celery("standalone_minimal", broker=broker, backend="redis://localhost:6379/1")

    @app.task(name="test_task")
    def test_task(msg):
        log("TASK EXECUTED: %s", msg)
        return f"done: {msg}"

    install_fork_hooks()
    install_celery_signals()

    dump_app_state("STANDALONE BEFORE inspect", app)
    dump_kombu_pools("STANDALONE BEFORE inspect")

    log("Calling app.control.inspect().active_queues()...")
    result = app.control.inspect().active_queues()
    log("inspect result: %s", result)

    dump_app_state("STANDALONE AFTER inspect", app)
    dump_kombu_pools("STANDALONE AFTER inspect")

    log("Starting worker_main with --hostname...")
    app.worker_main([
        "worker", "-O", "fair",
        "--queues", "default",
        "--concurrency", "1",
        "--loglevel", "INFO",
        "--hostname", "debug@%h",
    ])


def run_standalone_config_source():
    """Mimic Airflow's exact app construction: config_source= + task registration."""
    from celery import Celery
    import gc

    log("Creating standalone app mimicking Airflow's _get_celery_app()...")
    config = {
        "accept_content": ["json"],
        "event_serializer": "json",
        "worker_prefetch_multiplier": 1,
        "task_acks_late": True,
        "task_default_queue": "default",
        "task_track_started": True,
        "broker_url": "redis://localhost:6379/0",
        "result_backend": "redis://localhost:6379/1",
        "worker_concurrency": 1,
        "broker_transport_options": {"visibility_timeout": 86400},
    }

    # This is how Airflow creates the app — config_source= instead of conf.update()
    app = Celery("airflow.providers.celery.executors.celery_executor", config_source=config)

    @app.task(name="execute_workload")
    def test_task(msg):
        log("TASK EXECUTED: %s", msg)
        return f"done: {msg}"

    # Mimic Airflow's celery_import_modules signal
    from celery.signals import import_modules as celery_import_modules
    from celery.signals import worker_ready

    @celery_import_modules.connect
    def on_import(*args, **kwargs):
        log("celery_import_modules signal fired — calling gc.freeze()")
        gc.freeze()

    @worker_ready.connect
    def on_ready(*args, **kwargs):
        log("worker_ready signal fired — calling gc.unfreeze()")
        gc.unfreeze()

    install_fork_hooks()
    install_celery_signals()

    dump_app_state("CONFIG-SOURCE BEFORE inspect", app)
    dump_kombu_pools("CONFIG-SOURCE BEFORE inspect")

    log("Calling app.control.inspect().active_queues()...")
    result = app.control.inspect().active_queues()
    log("inspect result: %s", result)

    dump_app_state("CONFIG-SOURCE AFTER inspect", app)
    dump_kombu_pools("CONFIG-SOURCE AFTER inspect")

    log("Starting worker_main with --hostname...")
    app.worker_main([
        "worker", "-O", "fair",
        "--queues", "default",
        "--concurrency", "1",
        "--loglevel", "INFO",
        "--hostname", "debug@%h",
    ])


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "help"

    if mode == "airflow":
        run_airflow_context(do_inspect=True)
    elif mode == "airflow-no-inspect":
        run_airflow_context(do_inspect=False)
    elif mode == "standalone":
        run_standalone(use_airflow_config=False)
    elif mode == "standalone-airflow-config":
        run_standalone(use_airflow_config=True)
    elif mode == "standalone-vt":
        run_standalone(use_visibility_timeout=True)
    elif mode == "standalone-config-source":
        run_standalone_config_source()
    else:
        print("Usage: python3 deep_celery_debug.py <mode>")
        print()
        print("  airflow                  - Airflow's app + inspect before worker_main (BROKEN)")
        print("  airflow-no-inspect       - Airflow's app, no inspect (WORKS - control)")
        print("  standalone               - Fresh Celery app + inspect (WORKS)")
        print("  standalone-airflow-config - Fresh app with full Airflow config + inspect")
        print("  standalone-vt            - Fresh app with ONLY visibility_timeout + inspect")
        print("  standalone-config-source - Mimics Airflow's exact app construction + gc.freeze")
        sys.exit(1)
