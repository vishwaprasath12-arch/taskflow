"""Run:  python test_scenarios.py          -> normal / temp failure / repeated failure / duplicates
        python test_scenarios.py --crash  -> kills workers mid-job and checks recovery
API (uvicorn) must be running on :8000. Do NOT start a separate worker for --crash."""
import subprocess, sys, time, json
import requests

BASE = "http://localhost:8000"


def submit(task_type, payload=None, key=None):
    return requests.post(f"{BASE}/jobs", json={"task_type": task_type, "payload": payload or {},
                                               "request_key": key}).json()


def metrics():
    return requests.get(f"{BASE}/metrics").json()


def wait_all_done(timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        m = metrics()
        if m["total"] and m["queued"] == 0 and m["running"] == 0:
            return m
        time.sleep(1)
    print("TIMEOUT waiting for jobs"); return metrics()


def report(title, m):
    print(f"\n=== {title} ===")
    print(json.dumps(m, indent=2))


def normal_scenarios():
    requests.post(f"{BASE}/reset")
    for i in range(10): submit("send_email", {"to": f"user{i}@example.com"})
    for _ in range(3):  submit("flaky", {"fail_times": 2})        # temporary failures
    for _ in range(2):  submit("always_fail")                     # repeated failures
    for _ in range(3):  submit("generate_report", {"seconds": 2}, key="report-42")  # duplicates
    print("submitted 10 normal + 3 flaky + 2 always-fail + 3 duplicate requests (1 unique)")
    report("RESULTS (needs a worker running: python worker.py)", wait_all_done())


def crash_scenario():
    requests.post(f"{BASE}/reset")
    workers = [subprocess.Popen([sys.executable, "worker.py"]) for _ in range(2)]
    for _ in range(4): submit("long_task", {"seconds": 6})
    print("waiting until 2 jobs are running...")
    t = time.time()
    while metrics()["running"] < 2 and time.time() - t < 30: time.sleep(0.5)
    print(">>> KILLING WORKERS NOW")
    for w in workers: w.kill()
    print("workers dead. recovery should detect stale jobs in ~10-15s...")
    time.sleep(1)
    new_worker = subprocess.Popen([sys.executable, "worker.py"])
    m = wait_all_done()
    new_worker.kill()
    report("CRASH RECOVERY RESULTS", m)


if __name__ == "__main__":
    crash_scenario() if "--crash" in sys.argv else normal_scenarios()
