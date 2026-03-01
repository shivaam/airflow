"""
FORK IN SLOW MOTION - Watch two processes diverge.

This prints timestamps so you can see both processes
running at the same time, independently.

Run this: python 07_fork_slow_motion.py
"""

import os
import time

x = 42
print(f"BEFORE fork: one process, x = {x}, PID = {os.getpid()}\n")

pid = os.fork()

# ---- RIGHT HERE, there are now TWO processes ----
# Both have x = 42
# Both are about to execute the if statement
# But 'pid' is different in each one

if pid > 0:
    # PARENT only
    x = 100
    print(f"  PARENT: pid={pid} (that's my child), my PID={os.getpid()}, x={x}")
    time.sleep(0.5)
    print(f"  PARENT: x is still {x} — child can't change my copy")
    os.waitpid(pid, 0)

elif pid == 0:
    # CHILD only
    x = 999
    print(f"  CHILD:  pid={pid} (zero means I'm the child), my PID={os.getpid()}, x={x}")
    time.sleep(0.3)
    print(f"  CHILD:  x is still {x} — parent can't change my copy")
    os._exit(0)

print(f"\nAFTER: only the parent gets here. x = {x}")
