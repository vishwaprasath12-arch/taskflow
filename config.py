import os
import redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

QUEUE = "taskflow:queue"          # ready-to-run jobs
DELAYED = "taskflow:delayed"      # jobs waiting for a retry delay (sorted set)
DUP_COUNTER = "taskflow:dup_blocked"

MAX_RETRIES = 3
RETRY_DELAYS = [2, 5, 10]         # seconds before retry 1, 2, 3
HEARTBEAT_SECONDS = 3             # worker pings DB while running a job
STALE_SECONDS = 10                # no heartbeat for this long => worker presumed dead
MAX_RECOVERIES = 2                # re-queue an interrupted job at most this many times
