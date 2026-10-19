# TaskFlow - failure-aware async job runner

FastAPI + Redis queue + Python worker + SQLite + HTML dashboard.

## Run (3 terminals, inside this folder)
```
pip install -r requirements.txt
docker run -d -p 6379:6379 redis          # once
uvicorn app:app --port 8000               # terminal 1 (API + dashboard + recovery loop)
python worker.py                          # terminal 2
python test_scenarios.py                  # terminal 3 (normal / retry / failure / duplicates)
python test_scenarios.py --crash          # crash test (stop terminal 2 worker first)
```
Dashboard: http://localhost:8000

## How it works
- POST /jobs -> row in SQLite (pending) -> job id pushed to Redis list
- Worker claims job atomically (pending/retrying -> running), runs it, heartbeat every 3s
- Failure -> retry with delays 2s/5s/10s (max 3) via Redis sorted set; then FAILED + error saved
- Worker crash -> heartbeat stops -> recovery loop (API process, every 5s) re-queues "running" jobs stale >10s
- Duplicates: UNIQUE request_key in SQLite (blocked) + atomic claim (no double execution)
- Metrics: /metrics (completion rate, recovery rate, avg delay, duplicates)

## Limitations (say this honestly in the PPT)
At-least-once delivery: a job killed mid-run is re-run from the start, so tasks should be idempotent.
Single Redis / SQLite instance (prototype).
