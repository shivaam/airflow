# Airflow Issue #50038 — DAG Processor Cannot Run in Daemon Mode

## Summary

When running `airflow dag-processor --daemon`, the process crashes with:
- Linux: `OSError: [Errno 22] Invalid argument`
- macOS: `OSError: [Errno 9] Bad file descriptor`

Running without `--daemon` works fine.

## Key Concepts

### File Descriptors (FDs)

A file descriptor is a small integer (0, 1, 2, 3...) that the kernel uses as an
index into a per-process table of open resources. FDs can point to:

| FD | Default mapping |
|----|-----------------|
| 0  | stdin           |
| 1  | stdout          |
| 2  | stderr          |
| 3+ | anything you open: files, sockets, pipes, **epoll/kqueue instances** |

When you call `open()`, `socket()`, `epoll_create()`, etc., the kernel picks the
lowest available integer and maps it to an internal kernel object.

### Selectors (epoll / kqueue)

Python's `selectors.DefaultSelector()` wraps the OS's I/O multiplexing mechanism:
- **Linux** → `epoll` (via `epoll_create1()` syscall)
- **macOS** → `kqueue` (via `kqueue()` syscall)

A selector is itself a file descriptor. When you create one, the kernel allocates
an FD (say, FD 5) that represents the epoll/kqueue instance. You then "register"
other FDs with it — telling the OS "watch these sockets for me."

```
selector = selectors.DefaultSelector()   # kernel: epoll_create1() → FD 5
selector.register(socket_fd, EVENT_READ) # kernel: epoll_ctl(5, ADD, socket_fd)
events = selector.select(timeout=1.0)    # kernel: epoll_wait(5, ...)
```

### Unix Daemonization (Double Fork)

A daemon is a background process with no controlling terminal. The classic recipe:

1. **Fork #1** — Parent exits → shell gets its prompt back
2. **`setsid()`** — Create new session → detach from terminal's session
3. **Fork #2** — Session leader exits → grandchild can never reacquire a terminal
4. **Close all inherited FDs** — Clean slate, no leaked resources from parent
5. **Redirect stdin/stdout/stderr** — Point to /dev/null or log files

Step 4 is the critical one for this bug. The `python-daemon` library iterates
through every possible FD number and calls `os.close(fd)` on each, except those
explicitly listed in `files_preserve`.

### Why FDs Are Closed During Daemonization

A daemon inherits all open FDs from its parent process. These might include:
- Terminal handles (stdin/stdout/stderr pointing to the TTY)
- Pipes to the shell
- Sockets from a parent server
- Lock files, log files, etc.

A well-behaved daemon should not hold onto any of these. Closing them prevents:
- Keeping the terminal alive after the user logs out
- Holding locks that should be released
- Leaking resources the daemon doesn't need

The `files_preserve` parameter is the escape hatch — you list FDs that the daemon
actually needs to keep (like log file handles).

## The Bug: Timeline

```
┌─────────────────────────────────────────────────────────────────────┐
│ TIME 1: CLI creates DagFileProcessorManager                        │
│                                                                     │
│   dag_processor_command.py:                                         │
│     manager = DagFileProcessorManager(...)                          │
│       └── attrs field: selector = selectors.DefaultSelector()       │
│           └── kernel: epoll_create1() → FD 5                        │
│                                                                     │
│   FD table: [0:stdin, 1:stdout, 2:stderr, 5:epoll_instance]        │
├─────────────────────────────────────────────────────────────────────┤
│ TIME 2: DaemonContext activates (--daemon flag)                     │
│                                                                     │
│   daemon_utils.py → daemon.DaemonContext.__enter__():               │
│     1. fork() → parent exits                                        │
│     2. setsid()                                                     │
│     3. fork() → session leader exits                                │
│     4. for fd in range(0, SC_OPEN_MAX):                             │
│            if fd not in files_preserve:                              │
│                os.close(fd)          ← CLOSES FD 5 (the epoll!)     │
│     5. redirect 0,1,2 to log files                                  │
│                                                                     │
│   FD table: [0:/dev/null, 1:logfile, 2:logfile]                     │
│   FD 5 is GONE. Kernel freed the epoll instance.                    │
│   But the Python selector object still thinks it owns FD 5.         │
├─────────────────────────────────────────────────────────────────────┤
│ TIME 3: callback() → manager.run() → _start_new_processes()        │
│                                                                     │
│   _create_process() spawns a child, creates socket pair (FD 6, 7)   │
│   _register_pipe_readers() calls:                                   │
│     self.selector.register(FD 7, EVENT_READ)                        │
│       └── Python: epoll_ctl(5, EPOLL_CTL_ADD, 7, ...)              │
│       └── Kernel: "FD 5 is not a valid epoll instance"              │
│       └── Returns EINVAL (Linux) or EBADF (macOS)                   │
│                                                                     │
│   💥 OSError: [Errno 22] Invalid argument                           │
└─────────────────────────────────────────────────────────────────────┘
```

### Why the Python Object Doesn't Know the FD Is Dead

The `DaemonContext` closes FDs using raw `os.close(fd)`, which operates at the
OS level. Python's `selectors.EpollSelector` object still exists in memory with
`self._epfd = 5` stored internally. Python has no way to know that FD 5 was
closed behind its back — there's no callback or notification mechanism.

It's like having a hotel key card after checkout. The card still exists, but the
lock won't accept it.

### Why Non-Daemon Mode Works

Without `--daemon`, `daemon_utils.py` skips `DaemonContext` entirely:

```python
else:
    signal.signal(signal.SIGINT, sigint_handler)
    callback()  # runs directly, no fork, no FD closing
```

No fork, no FD closing → the selector's FD stays valid.

## Code Locations

| File | Role |
|------|------|
| `airflow/cli/commands/dag_processor_command.py` | CLI entry point. Creates `DagFileProcessorManager`, then calls `run_command_with_daemon_option()` |
| `airflow/cli/commands/daemon_utils.py` | Handles `--daemon` flag. Uses `daemon.DaemonContext` which does the double fork + FD closing |
| `airflow/dag_processing/manager.py` line 177 | `selector: selectors.BaseSelector = attrs.field(factory=selectors.DefaultSelector)` — created at `__init__` time, BEFORE the daemon fork |
| `airflow/dag_processing/manager.py` → `run()` | Called AFTER the daemon fork. This is where the selector should be recreated |
| `airflow/dag_processing/manager.py` → `_start_new_processes()` → `_create_process()` | Where `selector.register()` is called and the crash happens |

## The Fix

One line at the top of `DagFileProcessorManager.run()`:

```python
def run(self):
    # Recreate the selector to get a fresh FD after the daemon fork
    self.selector = selectors.DefaultSelector()
    ...
```

This creates a new selector with a new, valid FD after the daemon fork has
already closed the old one. Safe in non-daemon mode too — `run()` is called
once and the selector hasn't been used yet.

## Is It Worth Fixing?

**Yes**, but it's low priority. Here's the tradeoff:

**Arguments for fixing:**
- It's a one-line fix with zero risk of regression
- The `--daemon` flag exists and is documented — users expect it to work
- Multiple users hit this across Linux and macOS
- The bug has been open since April 2025 with no fix as of late 2025

**Arguments for low priority:**
- Modern deployments use containers (Docker/K8s) or systemd, not daemon mode
- Easy workaround: `nohup airflow dag-processor > nohup.out &`
- The `--daemon` flag is considered "old school" by maintainers

**Verdict:** Worth a PR. It's a trivial fix, the root cause is well understood,
and it removes a sharp edge for users who do use daemon mode.

## Discussion Timeline (Issue #50038)

| Date | Who | What |
|------|-----|------|
| Apr 30 | yzhsieh | Filed the bug. `--daemon` crashes, without it works fine. |
| May 1 | yaobaishijie | Confirmed same issue on Python 3.9 |
| May 1 | vatsrahul1001 | Reproduced on macOS (EBADF instead of EINVAL) |
| May 6 | joyceguouk | Another confirmation |
| May 12 | ravi-simtel | Confirmed on EC2 Linux. Suggested `nohup` workaround |
| May 14 | kaxil | Added to Airflow 3.0.2 milestone |
| May 28 | Lee-W | Reproduced, suspected `os.fork()` in supervisor.py, got stuck |
| Jun 1 | phanikumv | Moved milestone to 3.0.3 |
| Jun 17 | ashb | Confirmed PR #51699 didn't fix it. Guessed "daemon closing FDs" |
| Jun 19 | uranusjr | Admitted "haven't found the direct cause yet" |
| Jul 1 | vatsrahul1001 | Moved milestone to 3.1.0 |
| Aug 17 | phanikumv | Moved milestone to 3.1+ |
| Sep 1 | vatsrahul1001 | Added priority:low label |
| Oct 23 | darrenbarnes-crx | Questioned why it's low priority |
| Nov 13 | lp-jump | Found root cause: selector created before fork, epoll FD is non-inheritable across daemon fork |
| Nov 13 | ashb | Confirmed the diagnosis. Offered to help review a PR |
| Nov 13 | potiuk | Agreed daemon mode is "old-school", good candidate for community contribution |
| Nov 14 | lp-jump | Proposed the fix: recreate selector at start of `run()` |
| Nov 14 | ashb | Confirmed: "Sounds like it, yeah" |
