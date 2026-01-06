"""
Multi‑agent ADK client (Cloud‑Run + httpx)

Call pattern from worker
------------------------
    agent = AGENT_ROUTING[dest_id]          # pick the config object
    sid   = await get_or_create_session(redis, phone, agent, init_state=state)
    txt,_ = await query(agent, phone, sid, user_prompt)
"""

from __future__ import annotations
from collections import namedtuple
import os, uuid, json, datetime as dt, logging, httpx
from dotenv import load_dotenv
from redis.asyncio import Redis

load_dotenv(override=True)          # leave .env convenience for local dev

# -----------------------------------------------------------
# Configuration helpers
# -----------------------------------------------------------
AgentConfig = namedtuple(
    "AgentConfig", ["service_url", "app_name", "id_token", "session_ttl"]
)

def _log_env(label: str, value: str | None):
    logging.info("   → %-20s %s", label, value or "<missing>")

LOG_LEVEL = os.getenv("TASKS_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL,
                    format="[%(asctime)s][%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# A *default* config picked up from env‑vars (handy for quick tests)
DEFAULT_AGENT = AgentConfig(
    service_url = os.getenv("ADK_SERVICE_URL", ""),
    app_name    = os.getenv("ADK_APP_NAME", ""),
    id_token    = os.getenv("ADK_ID_TOKEN", "sample_token"),
    session_ttl = int(os.getenv("SESSION_TTL_SECONDS", 30 * 60)),
)

# ------------------------------------------------------------------
# 🔑  YOU choose how to build the routing table – here’s a sample:
# ------------------------------------------------------------------
# AGENT_ROUTING lives in tasks_adk.py; we only need the type here.
# ------------------------------------------------------------------

# -----------------------------------------------------------------------------
# HTTP plumbing
# -----------------------------------------------------------------------------
_async_client = httpx.AsyncClient(timeout=300)

# -----------------------------------------------------------------------------
# Session helper
# -----------------------------------------------------------------------------





import asyncio, random, httpx, datetime as dt
from typing import Optional

MAX_RETRIES          = 10        # 1 original try + up to 3 retries
BACKOFF_BASE_SECONDS = 1.0      # initial wait before 1st retry
BACKOFF_JITTER       = 0.4      # ±40 % random jitter


print("Running latest version of adk_client")

async def get_or_create_session(
    redis: Redis,
    phone: str,
    agent: AgentConfig,
    *,
    init_state: Optional[dict] = None,
) -> str:
    """
    Ensures a live session on the correct Cloud‑Run agent.

    Retries automatically on transient 5xx responses or network/connection
    errors, using exponential back‑off with jitter.
    """
    cache_key = f"adk_session:user:{phone}:dest:{agent.app_name}"
    if (sid := await redis.get(cache_key)):
        await redis.expire(cache_key, agent.session_ttl)
        return sid.decode()

    new_sid   = f"sess-{uuid.uuid4()}"
    sess_url  = (
        f"{agent.service_url}/apps/{agent.app_name}/users/{phone}/sessions/{new_sid}"
    )
    payload   = (init_state or {}).copy()
    payload["created_at"] = dt.datetime.utcnow().isoformat()

    # Build headers – only include Authorization if we actually have a token
    headers = {"Content-Type": "application/json"}
    if agent.id_token:
        headers["Authorization"] = f"Bearer {agent.id_token}"

    # ------------------------------------------------------------------ #
    # Retry loop                                                          #
    # ------------------------------------------------------------------ #
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await _async_client.post(
                sess_url, headers=headers, json=payload, timeout=10.0
            )

            if resp.status_code in (200, 201, 409):
                logger.info(
                    "[SESSION] %s created/validated on %s (attempt %d)",
                    new_sid, agent.app_name, attempt
                )
                await redis.set(cache_key, new_sid, ex=agent.session_ttl)
                return new_sid

            # Treat non‑5xx errors (4xx) as permanent and raise immediately
            if resp.status_code < 500 or resp.status_code >= 600:
                resp.raise_for_status()

            # Fall through → transient 5xx considered retry‑able
            raise httpx.HTTPStatusError(
                "Transient server error", request=resp.request, response=resp
            )

        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            if attempt == MAX_RETRIES:
                logger.critical(
                    "[SESSION] FAILED after %d attempts – giving up. Last error: %s",
                    attempt, exc,
                )
                raise

            # calculate back‑off with jitter
            wait = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            wait *= 1 + random.uniform(-BACKOFF_JITTER, BACKOFF_JITTER)

            logger.warning(
                "[SESSION] Transient error on attempt %d (%s). "
                "Retrying in %.2fs…",
                attempt, exc, wait,
            )
            await asyncio.sleep(wait)

    # Should never reach here
    raise RuntimeError("Unexpected fall‑through in get_or_create_session()")


# # Before adding retry mechanism
# async def get_or_create_session(
#     redis: Redis,
#     phone: str,
#     agent: AgentConfig,
#     *,
#     init_state: dict | None = None,
# ) -> str:
#     """Ensures a live session on the correct Cloud‑Run service."""
#     cache_key = f"adk_session:user:{phone}:dest:{agent.app_name}"
#     if (sid := await redis.get(cache_key)):
#         await redis.expire(cache_key, agent.session_ttl)
#         return sid.decode()

#     new_sid   = f"sess-{uuid.uuid4()}"
#     sess_url  = f"{agent.service_url}/apps/{agent.app_name}/users/{phone}/sessions/{new_sid}"
#     payload   = (init_state or {}) | {"created_at": dt.datetime.utcnow().isoformat()}

#     headers   = {
#         "Authorization": f"Bearer {agent.id_token}",
#         "Content-Type":  "application/json",
#     }

#     logger.info("[SESSION] Creating session %s on %s", new_sid, agent.app_name)
#     resp = await _async_client.post(sess_url, headers=headers, json=payload)
#     if resp.status_code not in (200, 201, 409):
#         resp.raise_for_status()

#     await redis.set(cache_key, new_sid, ex=agent.session_ttl)
#     return new_sid


async def query(
    agent: AgentConfig,
    user_id: str,
    session_id: str,
    message: str,
    *,
    max_attempts: int = 10,
    base_delay: float = 0.8,          # first sleep; doubles each retry
) -> tuple[str | None, bool]:
    """
    Calls /run_sse (streaming=False) and returns (reply_text, got_response).

    Retries on network errors or HTTP 5xx. 4xx errors fail fast.
    """

    run_url = f"{agent.service_url}/run_sse"
    headers = {
        "Authorization": f"Bearer {agent.id_token}",
        "Content-Type":  "application/json",
        "Accept":        "text/event-stream",
    }
    body = {
        "app_name":    agent.app_name,
        "user_id":     user_id,
        "session_id":  session_id,
        "new_message": {"role": "user", "parts": [{"text": message}]},
        "streaming":   False,
    }

    attempt = 1
    delay   = base_delay
    while True:
        logger.info(
            "[RUN_SSE] attempt %d/%d – session %s",
            attempt, max_attempts, session_id,
        )
        try:
            reply, got_response, chunks = None, False, []

            async with _async_client.stream(
                "POST", run_url, headers=headers, json=body
            ) as r:

                # Retry on upstream 5xx
                if r.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"ADK 5xx ({r.status_code})",
                        request=r.request,
                        response=r,
                    )

                r.raise_for_status()                    # 4xx ⇒ fail fast

                async for line in r.aiter_lines():
                    got_response = True
                    if line.startswith("data:"):
                        data = line[5:].strip()
                        if data == '{"done":true}':
                            break
                        try:
                            chunks.append(json.loads(data))
                        except json.JSONDecodeError:
                            logger.debug("Bad JSON chunk: %s", data)

            # --------- extract final assistant text --------------------
            for ev in chunks:
                parts = ev.get("content", {}).get("parts", [])
                for part in parts:
                    if isinstance(part, dict) and part.get("text"):
                        reply = part["text"]

            return reply, got_response                         # ✅ success

        # ---------------- handle retryable failures -------------------
        except (
            httpx.ConnectError,
            httpx.ReadError,
            httpx.RemoteProtocolError,
            httpx.PoolTimeout,
            httpx.HTTPStatusError,
        ) as e:
            status = getattr(e, "response", None)
            status = status.status_code if status is not None else None

            # Stop retrying on 4xx or after max_attempts
            if attempt >= max_attempts or (status and status < 500):
                logger.error("[RUN_SSE] giving up after %d attempts – %s", attempt, e)
                raise

            logger.warning(
                "[RUN_SSE] transient failure (%s). retrying in %.1fs…",
                status or e.__class__.__name__,
                delay,
            )
            await asyncio.sleep(delay)
            attempt += 1
            delay   *= 2

# -----------------------------------------------------------------------------
# Query helper before adding retry
# -----------------------------------------------------------------------------
# async def query(
#     agent: AgentConfig,
#     user_id: str,
#     session_id: str,
#     message: str,
# ) -> tuple[str | None, bool]:
#     """POSTs to /run_sse (streaming False) and returns (text, got_answer?)."""
#     run_url = f"{agent.service_url}/run_sse"
#     headers = {
#         "Authorization": f"Bearer {agent.id_token}",
#         "Content-Type":  "application/json",
#         "Accept":        "text/event-stream",
#     }
#     body = {
#         "app_name":    agent.app_name,
#         "user_id":     user_id,
#         "session_id":  session_id,
#         "new_message": {"role": "user", "parts": [{"text": message}]},
#         "streaming":   False,
#     }

#     reply, got_response, chunks = None, False, []
#     async with _async_client.stream("POST", run_url, headers=headers, json=body) as r:
#         if r.status_code != 200:
#             err = await r.aread()
#             logger.error("ADK error %s – %s", r.status_code, err.decode())
#             r.raise_for_status()

#         async for line in r.aiter_lines():
#             got_response = True
#             if line.startswith("data:"):
#                 data = line[5:].strip()
#                 if data == '{"done":true}':
#                     break
#                 try:
#                     chunks.append(json.loads(data))
#                 except json.JSONDecodeError:
#                     logger.warning("Bad JSON chunk: %s", data)

#     for ev in chunks:
#         parts = ev.get("content", {}).get("parts", [])
#         for part in parts:
#             if isinstance(part, dict) and part.get("text"):
#                 reply = part["text"]

#     return reply, got_response

__all__ = ["AgentConfig", "get_or_create_session", "query"]
