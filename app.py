import os, sqlite3, threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

import db, recovery
from config import r, QUEUE, DELAYED, DUP_COUNTER, MAX_RETRIES
from tasks import TASKS


@asynccontextmanager
async def lifespan(app):
    db.init_db()
    threading.Thread(target=recovery.loop, daemon=True).start()
    yield


app = FastAPI(title="TaskFlow", lifespan=lifespan)


class JobIn(BaseModel):
    task_type: str
    payload: dict = {}
    request_key: Optional[str] = None
    max_retries: int = MAX_RETRIES


@app.get("/")
def dashboard():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


@app.post("/jobs")
def submit(body: JobIn):
    if body.task_type not in TASKS:
        raise HTTPException(400, f"unknown task_type. choose from {list(TASKS)}")
    try:
        job_id = db.create_job(body.task_type, body.payload, body.request_key, body.max_retries)
    except sqlite3.IntegrityError:  # same request_key submitted again
        r.incr(DUP_COUNTER)
        existing = db.get_by_key(body.request_key)
        db.log(existing["id"], f"duplicate submission blocked (key={body.request_key})")
        return {"job_id": existing["id"], "status": existing["status"], "duplicate": True}
    r.lpush(QUEUE, job_id)
    return {"job_id": job_id, "status": "pending", "duplicate": False}


@app.get("/jobs")
def jobs():
    return db.list_jobs()


@app.get("/jobs/{job_id}")
def job(job_id: str):
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "job not found")
    return j


@app.get("/metrics")
def metrics():
    return db.metrics(r.get(DUP_COUNTER))


@app.get("/events")
def events():
    return db.list_events()


@app.post("/recover")
def recover_now():
    return {"recovered": recovery.run_once()}


@app.post("/reset")
def reset():
    db.reset_all()
    r.delete(QUEUE, DELAYED, DUP_COUNTER)
    return {"ok": True}
