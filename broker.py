from taskiq_redis import RedisStreamBroker
import os
from dotenv import load_dotenv

load_dotenv(override=True)

REDIS_URL      = os.getenv("CACHE_REDIS_URL", "redis://localhost:6379/0")
REDIS_PASSWORD = os.getenv("CACHE_REDIS_PASSWORD")

broker = RedisStreamBroker(
    url=REDIS_URL,
    password=REDIS_PASSWORD,      # Taskiq passes this straight to redis-py
)
