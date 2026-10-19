import json, threading, time
import db
from config import r, QUEUE, DELAYED, RETRY_DELAYS, HEARTBEAT_SECONDS
from tasks import TASKS

current = {"id": None}


def heartbeat_loop():
    """While a job runs, keep updated_at fresh. If the worker dies, this stops and
    the recovery module sees the job as stale."""
    while True:
        time.sleep(HEARTBEAT_SECONDS)
        if current["id"]:
            db.touch(current["id"])


def promote_due():
    """Move jobs whose retry delay has passed from the delayed set to the queue."""
    for job_id in r.zrangebyscore(DELAYED, 0, time.time()):
        if r.zrem(DELAYED, job_id):
            r.lpush(QUEUE, job_id)


def process(job_id):
    job = db.claim(job_id)
    if job is None:
        print(f"[skip] {job_id} already taken/finished")
        return
    current["id"] = job_id
    attempt = job["attempts"]
    db.log(job_id, f"running (attempt {attempt})")
    print(f"[run ] {job_id} {job['task_type']} attempt {attempt}")
    try:
        result = TASKS[job["task_type"]](json.loads(job["payload"]), attempt)
        db.mark_completed(job_id, str(result))
        db.log(job_id, f"completed: {result}")
        print(f"[done] {job_id}")
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        if job["retries"] < job["max_retries"]:
            delay = RETRY_DELAYS[min(job["retries"], len(RETRY_DELAYS) - 1)]
            db.mark_retry(job_id, err)
            r.zadd(DELAYED, {job_id: time.time() + delay})
            db.log(job_id, f"failed ({err}) -> retry {job['retries'] + 1}/{job['max_retries']} in {delay}s")
            print(f"[retry] {job_id} in {delay}s")
        else:
            db.mark_failed(job_id, err)
            db.log(job_id, f"FAILED after {job['retries']} retries: {err}")
            print(f"[fail] {job_id}")
    finally:
        current["id"] = None


def main():
    db.init_db()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    print("worker started, waiting for jobs...")
    while True:
        promote_due()
        item = r.brpop(QUEUE, timeout=1)
        if item:
            process(item[1])


if __name__ == "__main__":
    main()
