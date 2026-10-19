import time


def send_email(p, attempt):
    time.sleep(0.5)
    return f"email sent to {p.get('to', 'user')}"


def generate_report(p, attempt):
    time.sleep(p.get("seconds", 3))
    return "report generated"


def process_file(p, attempt):
    time.sleep(p.get("seconds", 2))
    return f"processed {p.get('name', 'file.csv')}"


def flaky(p, attempt):
    """Temporary failure: fails the first `fail_times` attempts, then succeeds."""
    if attempt <= p.get("fail_times", 2):
        raise ConnectionError(f"temporary upstream error (attempt {attempt})")
    time.sleep(1)
    return f"succeeded on attempt {attempt}"


def always_fail(p, attempt):
    """Repeated failure: used to show the retry limit stops endless loops."""
    raise RuntimeError("permanent failure (simulated)")


def long_task(p, attempt):
    """Long job used for the worker-crash demo."""
    time.sleep(p.get("seconds", 6))
    return "long job done"


TASKS = {
    "send_email": send_email,
    "generate_report": generate_report,
    "process_file": process_file,
    "flaky": flaky,
    "always_fail": always_fail,
    "long_task": long_task,
}
