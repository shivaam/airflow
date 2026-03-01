"""
WHAT IS FORK? - See process creation in action.

fork() clones the current process. After the call, there are TWO
identical processes running the same code. The only difference is
what fork() returns:
  - Parent gets the child's PID (positive number)
  - Child gets 0

Run this: python 04_what_is_fork.py
"""

import os
import time

print(f"=== Before fork ===")
print(f"I am process {os.getpid()}, my parent is {os.getppid()}")
print(f"There is ONE process running this code right now.\n")

print("Calling fork()...\n")

pid = os.fork()

# --- From this line onward, TWO processes are running ---

if pid > 0:
    # fork() returned a positive number → I am the PARENT
    print(f"[PARENT] I am PID {os.getpid()}")
    print(f"[PARENT] fork() returned {pid} — that's my child's PID")
    print(f"[PARENT] I'll wait for my child to finish...")
    os.waitpid(pid, 0)  # wait for child to exit
    print(f"[PARENT] Child is done. I'm exiting too.")

elif pid == 0:
    # fork() returned 0 → I am the CHILD
    print(f"  [CHILD] I am PID {os.getpid()}")
    print(f"  [CHILD] My parent is PID {os.getppid()}")
    print(f"  [CHILD] fork() returned 0 — that's how I know I'm the child")
    time.sleep(1)
    print(f"  [CHILD] I'm done. Exiting.")
    os._exit(0)
