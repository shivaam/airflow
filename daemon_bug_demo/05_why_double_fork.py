"""
WHY DOUBLE FORK? - Understanding each step of daemonization.

The goal: create a process that:
  1. Survives after your terminal closes
  2. Has no controlling terminal (can't be killed by Ctrl+C)
  3. Doesn't become a zombie

Each fork solves a specific problem. Let's see them one at a time.

Run this: python 05_why_double_fork.py
Then run: cat /tmp/double_fork_demo.log
"""

import os
import time


def log(msg):
    """Write to a file since the daemon has no terminal."""
    with open("/tmp/double_fork_demo.log", "a") as f:
        f.write(msg + "\n")


# Clear the log file
open("/tmp/double_fork_demo.log", "w").close()

original_pid = os.getpid()
print(f"=== Double Fork Demo ===\n")
print(f"Original process: PID {original_pid}, Parent: {os.getppid()}")
print(f"My parent is your shell (the terminal you typed the command in).\n")

# =====================================================================
# FORK #1 — Detach from the shell
# =====================================================================
# WHY: When you run a command, your shell WAITS for it to finish.
#       If we fork, the parent can exit immediately, and the shell
#       thinks the command is done. The child keeps running.
#
# Without this: your terminal would hang until the daemon exits.
# =====================================================================

print("--- FORK #1: Detach from the shell ---")
pid = os.fork()

if pid > 0:
    # Parent: exit so the shell gets its prompt back
    print(f"Parent (PID {os.getpid()}) exiting. Shell gets its prompt back.")
    print(f"Child (PID {pid}) continues in the background.\n")
    print(f"Check the log: cat /tmp/double_fork_demo.log")
    os._exit(0)

# We are now the child. Shell thinks the command is done.
log(f"[After Fork #1] I am PID {os.getpid()}, parent was {original_pid}")

# =====================================================================
# setsid() — Become a session leader
# =====================================================================
# WHY: Even though the shell isn't waiting for us, we're still part
#       of the shell's "session." If the terminal window closes, the
#       OS sends SIGHUP to everyone in that session — including us.
#
# setsid() creates a brand new session with us as the leader.
# Now closing the terminal won't kill us.
#
# But there's a catch: as a session leader, if we ever open a
# terminal device, it becomes our "controlling terminal" again.
# That's what fork #2 prevents.
# =====================================================================

os.setsid()
log(f"[After setsid()] New session created. I am session leader.")
log(f"  Session ID: {os.getsid(0)}")

# =====================================================================
# FORK #2 — Stop being the session leader
# =====================================================================
# WHY: A session leader CAN acquire a controlling terminal just by
#       opening /dev/tty or a terminal device. If that happens, we're
#       back to square one — terminal signals can reach us.
#
# By forking again, the grandchild is NOT the session leader,
# so it can NEVER accidentally acquire a controlling terminal.
#
# This is the "belt AND suspenders" of daemonization.
# =====================================================================

pid = os.fork()

if pid > 0:
    # First child (session leader) exits
    log(f"[Fork #2] Session leader (PID {os.getpid()}) exiting.")
    log(f"  Grandchild (PID {pid}) is the actual daemon.")
    os._exit(0)

# We are now the grandchild — the actual daemon
log(f"\n[THE DAEMON] I am PID {os.getpid()}")
log(f"  Parent PID: {os.getppid()}")
log(f"  Session ID: {os.getsid(0)}")
log(f"  I am NOT the session leader (my PID != Session ID)")
log(f"  I cannot acquire a controlling terminal")
log(f"  I will survive even if the original terminal is closed")

# =====================================================================
# Close inherited FDs and redirect stdio
# =====================================================================
# WHY: We inherited file descriptors from the original process.
#       stdin/stdout/stderr still point to the (now gone) terminal.
#       Other FDs might hold locks or resources we don't need.
#       Clean slate = no surprises.
#
# THIS is the step that kills the selector's FD in the Airflow bug.
# =====================================================================

log(f"\n[CLEANUP] Closing inherited file descriptors...")
for fd in range(3, 256):
    try:
        os.close(fd)
        log(f"  Closed FD {fd}")
    except OSError:
        pass  # wasn't open

devnull = os.open(os.devnull, os.O_RDWR)
os.dup2(devnull, 0)
os.dup2(devnull, 1)
os.dup2(devnull, 2)

log(f"\n[CLEANUP] Redirected stdin/stdout/stderr to /dev/null")
log(f"  stdin  (FD 0) → /dev/null")
log(f"  stdout (FD 1) → /dev/null")
log(f"  stderr (FD 2) → /dev/null")

# Simulate doing some work
log(f"\n[DAEMON] Now doing work in the background...")
for i in range(3):
    time.sleep(1)
    log(f"[DAEMON] Tick {i + 1}...")

log(f"[DAEMON] Done. Exiting.")
