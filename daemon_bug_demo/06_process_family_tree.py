"""
PROCESS FAMILY TREE - See parent/child relationships.

This creates a small family of processes so you can see
how PIDs and PPIDs (Parent PIDs) connect them.

Run this: python 06_process_family_tree.py
"""

import os
import time


def show_identity(name):
    print(f"  [{name}] PID={os.getpid()}  Parent={os.getppid()}")


print("=== Process Family Tree ===\n")
show_identity("ORIGINAL")

pid1 = os.fork()

if pid1 == 0:
    # First child
    show_identity("CHILD-A ")

    pid2 = os.fork()
    if pid2 == 0:
        # Grandchild
        show_identity("GRAND-A1")
        time.sleep(0.1)
        os._exit(0)
    else:
        os.waitpid(pid2, 0)
        os._exit(0)
else:
    pid3 = os.fork()
    if pid3 == 0:
        # Second child
        show_identity("CHILD-B ")
        time.sleep(0.1)
        os._exit(0)
    else:
        os.waitpid(pid1, 0)
        os.waitpid(pid3, 0)

print(f"""
Family tree:

  ORIGINAL (PID {os.getpid()})
  ├── CHILD-A
  │   └── GRAND-A1
  └── CHILD-B

Every process knows its own PID and its parent's PID (PPID).
That's how the OS tracks the family tree.
""")

print("You can also see this live with: ps -o pid,ppid,comm")
