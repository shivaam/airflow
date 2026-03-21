"""
Compare Celery app internal state between config_source= and broker= constructors.

Dumps every attribute, pool, connection, and kombu.pools entry to find the exact
difference that makes config_source= vulnerable to inspect() tainting.

Run on EC2: source /opt/airflow-scripts/env.sh && python3 ~/airflow/dev/compare_constructors.py
"""
import os
import sys
import time

import kombu.pools
from celery import Celery


def log(msg, *args):
    ts = time.strftime("%H:%M:%S")
    formatted = msg % args if args else msg
    print(f"[{ts}] {formatted}", flush=True)


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


def deep_dump(label, app):
    """Dump everything about the app's connection state."""
    log("\n========== %s ==========", label)

    # 1. Basic app info
    log("app.main = %s", app.main)
    log("app.conf.broker_url = %s", app.conf.broker_url)
    log("app.conf.result_backend = %s", str(app.conf.result_backend)[:80])
    log("app.conf.broker_transport_options = %s", app.conf.broker_transport_options)

    # 2. The connection used by the app
    log("app.connection_for_read() = %s", app.connection_for_read())
    log("app.connection_for_write() = %s", app.connection_for_write())

    # Compare the two connection objects
    r = app.connection_for_read()
    w = app.connection_for_write()
    log("read == write: %s", r is w)
    log("read.as_uri(): %s", r.as_uri())
    log("write.as_uri(): %s", w.as_uri())
    log("read.transport_options: %s", getattr(r, 'transport_options', 'N/A'))
    log("write.transport_options: %s", getattr(w, 'transport_options', 'N/A'))

    # 3. app.pool — the ConnectionPool
    pool = app.pool
    log("app.pool = %s (id=%s)", pool, id(pool))
    log("app.pool.limit = %s", getattr(pool, 'limit', 'N/A'))
    log("app.pool._closed = %s", getattr(pool, '_closed', 'N/A'))
    log("app.pool.connection = %s", getattr(pool, 'connection', 'N/A'))
    pool_conn = getattr(pool, 'connection', None)
    if pool_conn:
        log("app.pool.connection.as_uri() = %s", pool_conn.as_uri())
        log("app.pool.connection.transport_options = %s", getattr(pool_conn, 'transport_options', 'N/A'))

    # 4. app.amqp
    amqp = app.amqp
    log("app.amqp = %s (id=%s)", amqp, id(amqp))
    log("app.amqp._producer_pool = %s", amqp._producer_pool)
    log("app.amqp.producer_pool (property) = %s", amqp.producer_pool)
    log("app.amqp.producer_pool is app.amqp._producer_pool: %s",
        amqp.producer_pool is amqp._producer_pool)

    # 5. kombu.pools global state
    log("kombu.pools.connections._data keys = %s", list(kombu.pools.connections._data.keys()))
    log("kombu.pools.producers._data keys = %s", list(kombu.pools.producers._data.keys()))
    for key, val in kombu.pools.connections._data.items():
        log("  conn pool key=%s id=%s", list(key), id(val))
        log("    is app.pool: %s", val is pool)
    for key, val in kombu.pools.producers._data.items():
        log("  prod pool key=%s id=%s", list(key), id(val))

    # 7. app.control
    ctrl = app.control
    mb = ctrl.mailbox
    log("app.control.mailbox = %s", mb)
    log("app.control.mailbox.connection = %s", getattr(mb, 'connection', 'N/A'))
    mb_pp = getattr(mb, 'producer_pool', None)
    log("app.control.mailbox.producer_pool = %s (id=%s)", mb_pp, id(mb_pp) if mb_pp else None)
    log("mailbox.producer_pool is app.amqp.producer_pool: %s",
        mb_pp is amqp.producer_pool if mb_pp else 'N/A')

    # 8. Socket FDs
    log("socket FDs = %s", get_socket_fds())


def compare():
    log("PID: %s", os.getpid())

    # ── Pattern A: broker= constructor (WORKS) ──
    log("\n\n>>> CREATING APP A: Celery(broker=...) + conf.update()")
    kombu.pools.reset()
    app_a = Celery("pattern_a", broker="redis://localhost:6379/0", backend="redis://localhost:6379/1")
    app_a.conf.update(
        broker_transport_options={"visibility_timeout": 86400},
        worker_prefetch_multiplier=1,
        task_acks_late=True,
    )

    @app_a.task(name="test_task")
    def task_a(msg):
        return msg

    deep_dump("PATTERN A: BEFORE inspect", app_a)

    app_a.control.inspect().active_queues()
    deep_dump("PATTERN A: AFTER inspect", app_a)

    app_a.close()
    kombu.pools.reset()

    # ── Pattern B: config_source= constructor (BROKEN) ──
    log("\n\n>>> CREATING APP B: Celery(name, config_source=config)")
    config = {
        "broker_url": "redis://localhost:6379/0",
        "result_backend": "redis://localhost:6379/1",
        "broker_transport_options": {"visibility_timeout": 86400},
        "worker_prefetch_multiplier": 1,
        "task_acks_late": True,
    }
    app_b = Celery("pattern_b", config_source=config)

    @app_b.task(name="test_task")
    def task_b(msg):
        return msg

    deep_dump("PATTERN B: BEFORE inspect", app_b)

    app_b.control.inspect().active_queues()
    deep_dump("PATTERN B: AFTER inspect", app_b)

    # ── Compare key differences ──
    log("\n\n>>> KEY DIFFERENCES")
    log("(Recreating apps fresh for clean comparison)")

    kombu.pools.reset()
    a = Celery("cmp_a", broker="redis://localhost:6379/0", backend="redis://localhost:6379/1")
    a.conf.update(broker_transport_options={"visibility_timeout": 86400})

    b = Celery("cmp_b", config_source={
        "broker_url": "redis://localhost:6379/0",
        "result_backend": "redis://localhost:6379/1",
        "broker_transport_options": {"visibility_timeout": 86400},
    })

    log("A.connection_for_read().as_uri() = %s", a.connection_for_read().as_uri())
    log("B.connection_for_read().as_uri() = %s", b.connection_for_read().as_uri())
    log("A.connection_for_write().as_uri() = %s", a.connection_for_write().as_uri())
    log("B.connection_for_write().as_uri() = %s", b.connection_for_write().as_uri())

    log("A.connection_for_read().transport_options = %s", a.connection_for_read().transport_options)
    log("B.connection_for_read().transport_options = %s", b.connection_for_read().transport_options)

    log("A.pool.connection.as_uri() = %s", a.pool.connection.as_uri())
    log("B.pool.connection.as_uri() = %s", b.pool.connection.as_uri())

    log("A.pool.connection.transport_options = %s", a.pool.connection.transport_options)
    log("B.pool.connection.transport_options = %s", b.pool.connection.transport_options)

    # The pool connection identity — this determines the kombu.pools key
    log("")
    log("A.pool.connection is A.connection_for_write(): %s",
        a.pool.connection is a.connection_for_write())
    log("B.pool.connection is B.connection_for_write(): %s",
        b.pool.connection is b.connection_for_write())

    # Check kombu.pools _data directly
    log("")
    log("kombu.pools.connections._data keys:")
    for key in kombu.pools.connections._data:
        log("  %s", list(key))
    log("kombu.pools.producers._data keys:")
    for key in kombu.pools.producers._data:
        log("  %s", list(key))

    log("len(kombu.pools.connections._data) = %s", len(kombu.pools.connections._data))
    for key, pool in kombu.pools.connections._data.items():
        log("  key=%s pool_id=%s", list(key), id(pool))
        log("    is A.pool: %s", pool is a.pool)
        log("    is B.pool: %s", pool is b.pool)

    # Now the critical test: do both apps share the SAME kombu pool?
    log("")
    log("=== CRITICAL: Do A and B share the same kombu.pools entry? ===")
    log("A.pool id=%s", id(a.pool))
    log("B.pool id=%s", id(b.pool))
    log("A.pool is B.pool: %s", a.pool is b.pool)

    # Access producer pools to force creation
    ap = a.amqp.producer_pool
    bp = b.amqp.producer_pool
    log("A.amqp.producer_pool id=%s", id(ap))
    log("B.amqp.producer_pool id=%s", id(bp))
    log("A.amqp.producer_pool is B.amqp.producer_pool: %s", ap is bp)

    log("")
    log("After accessing both producer_pools:")
    log("kombu.pools.connections._data has %s entries", len(kombu.pools.connections._data))
    log("kombu.pools.producers._data has %s entries", len(kombu.pools.producers._data))
    for key, pool in kombu.pools.connections._data.items():
        log("  conn key=%s pool_id=%s", list(key), id(pool))
        log("    is A.pool: %s, is B.pool: %s", pool is a.pool, pool is b.pool)

    # The REAL test: does inspect() on A pollute B's state?
    log("")
    log("=== POLLUTION TEST: inspect() on A, does it affect B? ===")
    log("B.amqp._producer_pool BEFORE A.inspect = %s (id=%s)", bp, id(bp))

    a.control.inspect().active_queues()

    bp_after = b.amqp._producer_pool
    log("B.amqp._producer_pool AFTER A.inspect = %s (id=%s)", bp_after, id(bp_after) if bp_after else None)
    log("B._producer_pool changed: %s", bp is not bp_after)
    log("B._producer_pool is A._producer_pool: %s", bp_after is a.amqp._producer_pool)

    log("\nDONE")


if __name__ == "__main__":
    compare()
