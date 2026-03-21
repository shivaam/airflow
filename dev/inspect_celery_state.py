"""Dump Celery app internal state before and after inspect() to find all tainted pools."""
from airflow.providers.celery.executors.celery_executor import app as celery_app


def dump(label):
    print(f"\n=== {label} ===")
    print(f"  amqp._producer_pool: {celery_app.amqp._producer_pool}")
    pool = celery_app.pool
    print(f"  pool: {pool}")
    print(f"  pool._dirty: {getattr(pool, '_dirty', 'N/A')}")
    ctrl = celery_app.control
    mb = ctrl.mailbox
    print(f"  control.mailbox.producer_pool: {getattr(mb, 'producer_pool', 'N/A')}")
    print(f"  control.mailbox.connection: {getattr(mb, 'connection', 'N/A')}")


dump("BEFORE inspect")

insp = celery_app.control.inspect()
result = insp.active_queues()
print(f"\ninspect result: {result}")

dump("AFTER inspect")

# Reset producer pool (our fix)
celery_app.amqp._producer_pool = None
dump("AFTER reset _producer_pool only")

# Also reset the connection pool
try:
    celery_app.pool.force_close_all()
    print("\nforce_close_all succeeded")
except Exception as e:
    print(f"\nforce_close_all error: {e}")

dump("AFTER force_close_all")
