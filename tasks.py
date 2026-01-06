# tasks_adk.py  –  multi‑destination / multi‑agent version
# ---------------------------------------------------------------------------
import os, sys, uuid, logging, asyncio, json, time
from typing import List, Optional, Dict

sys.path.append(os.path.abspath("src"))

from redis.asyncio import Redis
from redis_conn import get_redis
from broker import broker
from datetime import datetime
from zoneinfo import ZoneInfo
from hubspot_helpers import _send_reply_to_thread

from adk_client import (                       # ← new style client
    AgentConfig,
    get_or_create_session,
    query,
)

# ────────────────────────────────────────────────────────────────
# 1.  AGENT ROUTING TABLE  (edit to fit your Cloud‑Run services)
# ----------------------------------------------------------------
AGENT_ROUTING: dict[str, AgentConfig] = {
    # dest_id       service‑url                         app‑name            id‑token                         ttl
    "1665275371": AgentConfig("https://tipti-client-agent-dev-862265114281.us-east1.run.app",
                              "tipti_client_agent_dev",
                              os.getenv("ID_TOKEN_AGENT1", "default_token"), 1800),
    "1930123456": AgentConfig("https://agent‑two‑uc.a.run.app",
                              "support_agent",
                              os.getenv("ID_TOKEN_AGENT2", "default_token"), 1800),
}
DEFAULT_AGENT = next(iter(AGENT_ROUTING.values()))           # fall‑back

# ----------------------------------------------------------------
# 2.  Logging & Lua helpers
# ----------------------------------------------------------------
LOG_LEVEL = os.getenv("TASKS_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL,
                    format="[%(asctime)s][%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

LOCK_TTL = 300

LUA_FETCH_AND_CLEAR = """
local v = redis.call('LRANGE', KEYS[1], 0, -1)
if #v > 0 then redis.call('DEL', KEYS[1]) end
return v
"""
TAKEOVER_LUA = """
if redis.call('GET', KEYS[1]) == 'starter'
then  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2]);  return 1
elseif redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
then  return 1
else  return 0
end
"""

# ----------------------------------------------------------------
# 3.  Taskiq worker
# ----------------------------------------------------------------
@broker.task
async def process_user_batch_task(
    *,
    user_phone: str,
    dest_id: str,                       # ← new → WhatsApp target number
    initial_load: bool,
    thread_id: int,
    recipient_actor_id: str,
    channel_id: str,
    channel_account_id: str,
    app_id: int,
    contact_name: Optional[str] = None,
    current_batch: Optional[List[str]] = None,
    is_testing: bool = False,
    enqueue_time: Optional[float] = None,
    crm_profile: Optional[Dict[str, object]] = None,
    **_ignore,
) -> str:
    """
    • Batches buffered messages
    • Routes to the appropriate Cloud‑Run agent
    • Sends reply back to HubSpot
    """
    logger.info("Corriendo worker para user_phone %s, channel_id %s channel_account_id %s", user_phone, channel_id, channel_account_id)

    redis: Redis = await get_redis()

    # --- destination‑scoped keys ------------------------------------------
    lock_key = f"lock:user:{user_phone}:dest:{dest_id}"
    buf_key  = f"buffer:user:{user_phone}:dest:{dest_id}"
    wid      = f"worker:{uuid.uuid4().hex}"

    if not await redis.eval(TAKEOVER_LUA, 1, lock_key, wid, LOCK_TTL):
        return "no_lock"

    try:
        # --------------------------------------------------------------- ①
        # Drain the buffer the first time; otherwise use passed batch
        if initial_load:
            batch_strings = [
                b.decode() for b in await redis.eval(LUA_FETCH_AND_CLEAR, 1, buf_key)
            ]
        else:
            batch_strings = current_batch or []
        if not batch_strings:
            return "empty"

        # --------------------------------------------------------------- ②
        # Pick the agent for this dest (fallback to default)
        agent = AGENT_ROUTING.get(dest_id, DEFAULT_AGENT)
        logger.info("Using agent %s for dest %s", agent.app_name, dest_id)

        # --------------------------------------------------------------- ③
        # Build / refresh ADK session
        nombre_para_chatbot = contact_name or f"User {user_phone}"
        gy_tz = ZoneInfo("America/Guayaquil")
        init_state = {
            "nombre_contacto_ws": nombre_para_chatbot,
            "numero_de_telefono": user_phone,
            "fecha_y_hora": datetime.now(gy_tz).strftime("%A, %Y-%m-%d %H:%M:%S"),
            "dest_id": dest_id,
        }
        if dest_id == "1930123456":          # sample per‑dest tweak
            init_state["soporte_flag"] = True

        adk_session_id = await get_or_create_session(
            redis, user_phone, agent, init_state=init_state
        )

        # --------------------------------------------------------------- ④
        user_prompt = " | ".join(batch_strings)
        logger.info("El user prompt es %s", user_prompt)
        reply_text = "Lo siento — ocurrió un problema procesando tu mensaje."

        for attempt in range(2):
            logger.info("[WORKER] ADK call attempt %s, session %s", attempt + 1, adk_session_id)

            reply_text, got_response = await query(
                agent, user_phone, adk_session_id, user_prompt
            )

            if got_response:
                break                       # success (even if reply_text None)
            if attempt == 0:
                # Reset session and retry once
                await redis.delete(f"adk_session:user:{user_phone}:dest:{agent.app_name}")
                adk_session_id = await get_or_create_session(
                    redis, user_phone, agent, init_state=init_state
                )
        # --------------------------------------------------------------- ⑤
        # If new messages arrived meanwhile, re‑enqueue
        new_bytes = await redis.eval(LUA_FETCH_AND_CLEAR, 1, buf_key)
        if new_bytes:
            combined = batch_strings + [b.decode() for b in new_bytes]
            instruct = "IMPORTANTE: Ignora tu última respuesta anterior..."
            if not combined[0].startswith("IMPORTANTE: Ignora"):
                combined.insert(0, instruct)
            await redis.delete(lock_key)
            await process_user_batch_task.kiq(
                user_phone=user_phone,
                dest_id=dest_id,
                initial_load=False,
                current_batch=combined,
                thread_id=thread_id,
                recipient_actor_id=recipient_actor_id,
                channel_id=channel_id,
                channel_account_id=channel_account_id,
                app_id=app_id,
                contact_name=contact_name,
                is_testing=is_testing,
                enqueue_time=enqueue_time,
            )
            return "requeued"

        # --------------------------------------------------------------- ⑥
        if not is_testing:
            await _send_reply_to_thread(
                thread_id=thread_id,
                message_text=reply_text,
                recipient_actor_id=recipient_actor_id,
                channel_id=channel_id,
                channel_account_id=channel_account_id,
            )
        else:
            await asyncio.sleep(0.2)   # tiny delay to mimic real send
            duration = (time.time() - enqueue_time) if enqueue_time else -1
            logger.info("TEST_MODE reply=%s dur=%.3fs", reply_text, duration)

        # reset spam counters for user (optional)
        await redis.hset(f"spam_state:user:{user_phone}", mapping={
            "l1_msg": 0, "l1_last": 0, "l1_until": 0
        })
        return "done"

    finally:
        if (owner := await redis.get(lock_key)) and owner.decode() == wid:
            await redis.delete(lock_key)
