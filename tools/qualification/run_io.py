"""Run qualification commands and persist their evidence."""

import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
STOP_GRACE = 5


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def logged(command, directory, label, env, timeout=1800, cancelled=None):
    print('+ ' + ' '.join(map(str, command)), flush=True)
    with (directory / f'{label}.log').open('w') as log:
        process = subprocess.Popen(list(map(str, command)), cwd=ROOT, env=env,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, errors='replace', start_new_session=True)
        expired, finished = threading.Event(), threading.Event()
        def signal_group(signum):
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                pass
        def terminate():
            # An exited parent can leave a descendant holding the output pipe.
            expired.set()
            signal_group(signal.SIGTERM)
            if not finished.wait(STOP_GRACE):
                signal_group(signal.SIGKILL)
        def watch():
            deadline = time.monotonic() + timeout
            while not finished.wait(0.1):
                if time.monotonic() >= deadline or cancelled is not None and cancelled.is_set():
                    terminate()
                    return
        watchdog = threading.Thread(target=watch, daemon=True)
        watchdog.start()
        try:
            for line in process.stdout:
                print(line, end='', flush=True)
                log.write(line)
                log.flush()
            code = process.wait()
        finally:
            finished.set()
            signal_group(signal.SIGTERM)
            try:
                process.wait(timeout=STOP_GRACE)
            except subprocess.TimeoutExpired:
                signal_group(signal.SIGKILL)
                process.wait()
            signal_group(signal.SIGKILL)
            process.stdout.close()
            watchdog.join(timeout=STOP_GRACE+1)
    if cancelled is not None and cancelled.is_set():
        raise InterruptedError(f'{label} cancelled')
    if expired.is_set():
        raise RuntimeError(f'{label} exceeded {timeout}s; inspect {directory / (label + ".log")}')
    if code:
        raise RuntimeError(f'{label} exited {code}; inspect {directory / (label + ".log")}')
