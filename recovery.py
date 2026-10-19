import time
import db
from config import r, QUEUE, STALE_SECONDS, MAX_RECOVERIES


def run_once():
    """Find jobs stuck in 'running' with no heartbeat (worker crashed) and re-queue them."""
    recovered = []
    for job in db.stale_running(time.time() - STALE_SECONDS):
        if job["recovery_count"] < MAX_RECOVERIES:
            db.mark_recovered(job["id"])
            r.lpush(QUEUE, job["id"])
            db.log(job["id"], "worker interruption detected -> job re-queued by recovery")
            recovered.append(job["id"])
        else:
            db.mark_failed(job["id"], "worker crashed repeatedly; recovery limit reached")
            db.log(job["id"], "recovery limit reached -> marked failed")
    return recovered


def loop():
    while True:
        try:
            run_once()
        except Exception as e:
            print("recovery error:", e)
        time.sleep(5)
