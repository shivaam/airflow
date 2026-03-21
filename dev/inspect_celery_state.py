"""
Dump full Celery app internal state before and after inspect() to trace
exactly what gets lazily initialized and what state would be inherited
by forked prefork children.

Run on EC2: source /opt/airflow-scripts/env.sh && python3 ~/airflow/dev/inspect_celery_state.py
"""
import os
import socket

from airflow.providers.celery.executors.celery_executor import app as celery_app


def get_open_fds():
    """Get all open file descriptors for this process."""
    fds = {}
    pid = os.getpid()
    fd_dir = f"/proc/{pid}/fd"
    if os.path.exists(fd_dir):
        for fd_name in os.listdir(fd_dir):
            try:
                fd_num = int(fd_name)
                target = os.readlink(f"{fd_dir}/{fd_name}")
                fds[fd_num] = target
            except (ValueError, OSError):
                pass
    return fds


def get_socket_fds():
    """Get only socket file descriptors."""
    return {fd: target for fd, target in get_open_fds().items() if "socket" in target}


def dump_resource_pool(name, pool_obj):
    """Dump internals of a kombu ResourcePool (ConnectionPool or ProducerPool)."""
    if pool_obj is None:
        print(f"  {name}: None")
        return

    print(f"  {name}: {pool_obj}")
    print(f"    type: {type(pool_obj).__name__}")
    print(f"    limit: {getattr(pool_obj, 'limit', 'N/A')}")
    print(f"    _closed: {getattr(pool_obj, '_closed', 'N/A')}")

    # _dirty = set of currently checked-out resources
    dirty = getattr(pool_obj, "_dirty", None)
    print(f"    _dirty: {dirty} (len={len(dirty) if dirty else 0})")

    # _resource = the underlying Queue of available resources
    resource = getattr(pool_obj, "_resource", None)
    if resource is not None:
        try:
            qsize = resource.qsize()
            print(f"    _resource.qsize: {qsize}")
            # Peek at items in the queue without removing them
            items = list(resource.queue) if hasattr(resource, "queue") else []
            for i, item in enumerate(items):
                print(f"    _resource[{i}]: {type(item).__name__} id={id(item)}")
                # If it's a Connection, show its transport details
                if hasattr(item, "transport"):
                    t = item.transport
                    print(f"      transport: {t}")
                    print(f"      connected: {getattr(item, 'connected', 'N/A')}")
                    client = getattr(t, "client", None) or getattr(t, "_connection", None)
                    if client:
                        print(f"      client: {client}")
                        conn = getattr(client, "connection", None) or getattr(client, "_sock", None)
                        if conn:
                            print(f"      socket: {conn}")
                            if hasattr(conn, "fileno"):
                                try:
                                    print(f"      fileno: {conn.fileno()}")
                                except Exception as e:
                                    print(f"      fileno: error ({e})")
                # If it's a Producer, show its connection
                if hasattr(item, "connection"):
                    print(f"      connection: {item.connection}")
                if hasattr(item, "channel"):
                    ch = item.channel
                    print(f"      channel: {ch}")
                    if hasattr(ch, "connection"):
                        print(f"      channel.connection: {ch.connection}")
        except Exception as e:
            print(f"    _resource inspection error: {e}")
    else:
        print(f"    _resource: None")


def dump_connection(name, conn_obj):
    """Dump a kombu Connection object."""
    if conn_obj is None:
        print(f"  {name}: None")
        return
    print(f"  {name}: {conn_obj}")
    print(f"    connected: {getattr(conn_obj, 'connected', 'N/A')}")
    print(f"    transport: {getattr(conn_obj, 'transport', 'N/A')}")
    t = getattr(conn_obj, "transport", None)
    if t:
        client = getattr(t, "client", None) or getattr(t, "_connection", None)
        if client:
            print(f"    transport.client: {client}")
            sock = getattr(client, "connection", None) or getattr(client, "_sock", None)
            if sock:
                print(f"    socket: {sock}")
                if hasattr(sock, "fileno"):
                    try:
                        print(f"    fileno: {sock.fileno()}")
                    except Exception as e:
                        print(f"    fileno: error ({e})")


def dump_full_state(label):
    """Dump everything we can find about the Celery app's connection state."""
    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")

    # 1. Connection pool (app.pool)
    dump_resource_pool("app.pool (ConnectionPool)", celery_app.pool)

    # 2. Producer pool (app.amqp._producer_pool)
    dump_resource_pool("app.amqp._producer_pool (ProducerPool)", celery_app.amqp._producer_pool)

    # 3. Control channel
    ctrl = celery_app.control
    print(f"\n  app.control: {ctrl}")
    mb = getattr(ctrl, "mailbox", None)
    if mb:
        print(f"  app.control.mailbox: {mb}")
        print(f"    namespace: {getattr(mb, 'namespace', 'N/A')}")
        mb_pp = getattr(mb, "producer_pool", None)
        dump_resource_pool("  mailbox.producer_pool", mb_pp)
        dump_connection("  mailbox.connection", getattr(mb, "connection", None))

    # 4. Backend connection
    backend = celery_app.backend
    print(f"\n  app.backend: {type(backend).__name__}")
    if hasattr(backend, "url"):
        # Mask password
        url = str(backend.url)
        print(f"    url: {url[:50]}...")

    # 5. AMQP default connection
    amqp = celery_app.amqp
    print(f"\n  app.amqp: {amqp}")
    default_conn = getattr(amqp, "_default_connection", None)
    dump_connection("  amqp._default_connection", default_conn)

    # 6. Socket FDs
    sock_fds = get_socket_fds()
    print(f"\n  Open socket FDs ({len(sock_fds)}):")
    for fd, target in sorted(sock_fds.items()):
        print(f"    fd {fd}: {target}")

    print()


# ============================================================
# Main
# ============================================================
print(f"PID: {os.getpid()}")

dump_full_state("STEP 1: BEFORE inspect()")

print("\n--- Calling celery_app.control.inspect().active_queues() ---")
insp = celery_app.control.inspect()
result = insp.active_queues()
print(f"inspect result: {result}")

dump_full_state("STEP 2: AFTER inspect()")

# Now do what our fix does
celery_app.amqp._producer_pool = None
dump_full_state("STEP 3: AFTER reset _producer_pool = None")

print("\n\nDONE")
