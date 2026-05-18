import os
import sys
import fcntl
import atexit
from pathlib import Path

def enforce_singleton(name):
    lock_dir = Path("/tmp/nautilus_locks")
    lock_dir.mkdir(exist_ok=True, parents=True)
    lock_file = lock_dir / f"{name}.lock"
    fh = open(lock_file, "w")
    try:
        fcntl.lockf(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.write(str(os.getpid()))
        fh.flush()
    except (IOError, OSError):
        print(f"[GUARD] Another instance of '{name}' is already running. Exiting.")
        sys.exit(0)
    def release_lock():
        try:
            fh.close()
            if lock_file.exists(): lock_file.unlink()
        except: pass
    atexit.register(release_lock)
    return fh
