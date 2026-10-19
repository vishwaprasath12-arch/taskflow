import json, os, sqlite3, time, uuid
from contextlib import contextmanager

DB_PATH = os.environ.get("TASKFLOW_DB", "taskflow.db")


@contextmanager
def db():
    c = sqlite3.connect(DB_PATH, timeout=10)
    c.row_factory = sqlite3.Row
    try:
        yield c
        c.commit()
    finally:
        c.close()


def init_db():
    with db() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("""CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            request_key TEXT UNIQUE,
            task_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL,          -- pending | running | retrying | completed | failed
            attempts INTEGER DEFAULT 0,    -- times execution started
            retries INTEGER DEFAULT 0,     -- retries scheduled after failures
            max_retries INTEGER DEFAULT 3,
            recovered INTEGER DEFAULT 0,   -- 1 if re-queued after worker crash
            recovery_count INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            error TEXT,
            result TEXT,
            created_at REAL, started_at REAL, updated_at REAL, finished_at REAL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT, ts REAL, message TEXT)""")


def log(job_id, message):
    with db() as c:
        c.execute("INSERT INTO events (job_id, ts, message) VALUES (?,?,?)",
                  (job_id, time.time(), message))


def create_job(task_type, payload, request_key, max_retries):
    job_id = uuid.uuid4().hex[:8]
    now = time.time()
    with db() as c:  # UNIQUE(request_key) raises IntegrityError on duplicates
        c.execute("""INSERT INTO jobs (id, request_key, task_type, payload, status,
                     max_retries, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)""",
                  (job_id, request_key, task_type, json.dumps(payload), "pending",
                   max_retries, now, now))
    log(job_id, f"submitted ({task_type})")
    return job_id


def get_job(job_id):
    with db() as c:
        row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return dict(row) if row else None


def get_by_key(request_key):
    with db() as c:
        row = c.execute("SELECT * FROM jobs WHERE request_key=?", (request_key,)).fetchone()
        return dict(row) if row else None


def list_jobs(limit=100):
    with db() as c:
        rows = c.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(x) for x in rows]


def list_events(limit=40):
    with db() as c:
        rows = c.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(x) for x in rows]


def claim(job_id):
    """Atomically move pending/retrying -> running. Returns the job row, or None if
    someone else already took it / it is already finished (prevents duplicate execution)."""
    now = time.time()
    with db() as c:
        cur = c.execute("""UPDATE jobs SET status='running', attempts=attempts+1,
                           started_at=COALESCE(started_at,?), updated_at=?
                           WHERE id=? AND status IN ('pending','retrying')""",
                        (now, now, job_id))
        if cur.rowcount != 1:
            return None
        return dict(c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())


def touch(job_id):
    with db() as c:
        c.execute("UPDATE jobs SET updated_at=? WHERE id=? AND status='running'",
                  (time.time(), job_id))


def mark_completed(job_id, result):
    now = time.time()
    with db() as c:
        c.execute("""UPDATE jobs SET status='completed', result=?, error=NULL,
                     success_count=success_count+1, finished_at=?, updated_at=? WHERE id=?""",
                  (result, now, now, job_id))


def mark_retry(job_id, error):
    with db() as c:
        c.execute("""UPDATE jobs SET status='retrying', retries=retries+1, error=?, updated_at=?
                     WHERE id=?""", (error, time.time(), job_id))


def mark_failed(job_id, error):
    now = time.time()
    with db() as c:
        c.execute("UPDATE jobs SET status='failed', error=?, finished_at=?, updated_at=? WHERE id=?",
                  (error, now, now, job_id))


def stale_running(cutoff):
    with db() as c:
        rows = c.execute("SELECT * FROM jobs WHERE status='running' AND updated_at < ?",
                         (cutoff,)).fetchall()
        return [dict(x) for x in rows]


def mark_recovered(job_id):
    with db() as c:
        c.execute("""UPDATE jobs SET status='pending', recovered=1, recovery_count=recovery_count+1,
                     error='worker interrupted - re-queued', updated_at=? WHERE id=?""",
                  (time.time(), job_id))


def reset_all():
    with db() as c:
        c.execute("DELETE FROM jobs")
        c.execute("DELETE FROM events")


def metrics(dup_blocked=0):
    with db() as c:
        x = c.execute("""SELECT COUNT(*) total,
            SUM(status='completed') completed, SUM(status='failed') failed,
            SUM(status IN ('pending','retrying')) queued, SUM(status='running') running,
            SUM(recovered=1) recovered, SUM(recovered=1 AND status='completed') recovered_ok,
            SUM(success_count>1) dup_exec, SUM(retries) retries,
            AVG(CASE WHEN status='completed' THEN finished_at-created_at END) avg_delay
            FROM jobs""").fetchone()
    g = lambda k: x[k] or 0
    finished = g("completed") + g("failed")
    return {
        "total": g("total"), "completed": g("completed"), "failed": g("failed"),
        "queued": g("queued"), "running": g("running"), "total_retries": g("retries"),
        "completion_rate": round(100 * g("completed") / finished, 1) if finished else 0,
        "recovered_jobs": g("recovered"),
        "recovery_rate": round(100 * g("recovered_ok") / g("recovered"), 1) if g("recovered") else 0,
        "avg_processing_delay_s": round(x["avg_delay"] or 0, 2),
        "duplicate_submissions_blocked": int(dup_blocked or 0),
        "duplicate_executions": g("dup_exec"),
    }
