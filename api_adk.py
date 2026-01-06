# stdlib
import asyncio, base64, hashlib, hmac, json, re, time, logging
from pathlib import Path
from typing import Tuple, Optional
from datetime import datetime
from zoneinfo import ZoneInfo
# 3rd-party
import httpx
# --- api.py ------------------------------------------------
import time
from pathlib import Path
from typing import Optional, List

from fastapi import FastAPI, Depends, HTTPException, Request, Response
from pydantic import BaseModel
#from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
#import sqlalchemy

#from database import get_db
#from models import Client
from redis_conn import get_redis
from tasks import process_user_batch_task
import httpx, os, time, json, pathlib
from zoneinfo import ZoneInfo   # Python 3.9+
from datetime import datetime, timezone
import re
from pathlib import Path
import json, time, httpx
from fastapi import HTTPException
from ficha_helper import fetch_ficha_by_phone, HS_FICHA_PROPS, build_contact_payload_from_ficha, _get_contact_properties, _update_contact_properties
from typing import Optional, Tuple    # ← Tuple added

import logging

import asyncio, ipaddress, urllib.parse
from redis.exceptions import ResponseError

from tasks_bq import log_rows_to_bq   # import the TaskIQ task
from bq_settings import DEST_TABLE
from datetime import datetime, timezone


import base64, hashlib, hmac, os, time

from hubspot_helpers_v2 import( 
  _is_valid_signature,
  _is_admin_responding,
  get_or_create_contact,
  _get_conversation_details,
  _associate_contact_to_ticket,
  _send_reply_to_thread,
  ensure_ticket_pipeline_for_thread,
)

APP_SECRET = os.getenv("HS_APP_SECRET")   # set this in your shell

SECRET = os.getenv("HS_CLIENT_SECRET")        # 36-char client secret

FICHA_SYNC_INTERVAL   =  5 #24 * 60 * 60          # 1 day  (= 86 400 s)
FICHA_SYNC_KEY_PREFIX = "ficha_sync_ts:"


#SIGNATURE_DEBUG = os.getenv("SIGNATURE_DEBUG") == "1"
#LOG_LEVEL = os.getenv("TASKS_LOG_LEVEL", "INFO").upper()
# logging.basicConfig(level=LOG_LEVEL, format="[%(asctime)s][%(levelname)s] %(message)s")
# logger = logging.getLogger(__name__)
# #─── Constants that must match the ones hard-coded in spam.lua ─────────────
L1_MESSAGE_THRESHOLD         = 10
MESSAGE_COUNT_WINDOW         = 60
L1_BLOCK_DURATION            = 300

L1_ACTIVATIONS_FOR_L2        = 3
L1_ACTIVATION_WINDOW_FOR_L2  = 600
L2_BLOCK_DURATION            = 900

L2_ACTIVATIONS_FOR_L3        = 2
L2_ACTIVATION_WINDOW_FOR_L3  = 1800
L3_BLOCK_DURATION            = 3600


#ADMIN_SESSION_TTL = 15 * 60  

DEV_CHANNEL_ID = 1007
TARGET_CHANNEL_ACCOUNT_ID   = "1665275371"

REDIRECT_URI  = "http://localhost:8000/redirect/"



from config import (CLIENT_ID,      
CLIENT_SECRET,
ENV_REFRESH_TOKEN,   # used only for first-run bootstrap
BASE_API_URL,    
TOKEN_FILE,     
SIGNATURE_DEBUG,
ADMIN_SESSION_TTL,
GUAYAQUIL,
ISO_Z_RE,
LOG_LEVEL,
logger,
client,
SENDER_ACTOR_ID_CACHE)




from adk_client import (                       # ← new style client
    AgentConfig,
    get_or_create_session,
    query,
)


CLIENT_CHANNEL = os.getenv("CLIENT_CHANNEL")
SHOPPER_CHANNEL = os.getenv("SHOPPER_CHANNEL")

CLIENT_URL= os.getenv("CLIENT_URL")
SHOPPER_URL= os.getenv("SHOPPER_URL")

AGENT_ROUTING: dict[str, AgentConfig] = {
    # dest_id       service‑url                         app‑name            id‑token                         ttl
    CLIENT_CHANNEL: AgentConfig(CLIENT_URL,
                              "tipti_client_agent",
                              os.getenv("ID_TOKEN_AGENT1", "default_token"), 1800),
    SHOPPER_CHANNEL: AgentConfig(SHOPPER_URL,
                              "tipti_shopper_agent",
                              os.getenv("ID_TOKEN_AGENT2", "default_token"), 1800),
}

DEFAULT_AGENT = next(iter(AGENT_ROUTING.values()))  

# --- api.py (ADD near env reads) ---
PIPELINE_BY_DEST = {
    # dest_id (channelAccountId) → {"pipeline": "<pipelineId>", "stage": "<stageId>"}
    CLIENT_CHANNEL: {
        "pipeline": os.getenv("PIPELINE_ID_CLIENT", ""),
        "stage":    os.getenv("PIPELINE_STAGE_CLIENT_NEW", ""),
    },
    SHOPPER_CHANNEL: {
        "pipeline": os.getenv("PIPELINE_ID_SHOPPER", ""),
        "stage":    os.getenv("PIPELINE_STAGE_SHOPPER_NEW", ""),
    },
}


# PIPELINE 'P. Careteam' id=0
#    stage 'Sin asignar' id=1
#    stage 'En atención del agente' id=2
#    stage 'Escalado Interno' id=4
#    stage 'Seguimiento de Cierre' id=1096626083
#    stage 'Solución Completa' id=1096626086
# PIPELINE 'P. Pruebas' id=757122634
#    stage 'Nuevo' id=1102351377
#    stage 'Esperando al contacto' id=1102351378
#    stage 'Esperando acción por nuestra parte' id=1102351379
#    stage 'Cerrados' id=1102351380
#    stage 'Cerrado con encuesta enviada' id=1142587911
# PIPELINE 'P.Shopper Dev' id=769658036
#    stage 'New' id=1123630697
#    stage 'Waiting on contact' id=1123630698
#    stage 'Waiting on us' id=1123630699
#    stage 'Closed' id=1123630700
# PIPELINE 'P. Masivos' id=769911468



app = FastAPI(title="Chat-Queue demo")
spam_sha: Optional[str] = None           # filled the first time we call Redis
import os, asyncio, ipaddress, urllib.parse
from redis.asyncio import Redis
from redis.exceptions import ResponseError

def _mask(secret: str | None) -> str:
    if not secret:
        return "None"
    return f"{secret[:3]}…{secret[-2:]} (len={len(secret)})" if len(secret) > 6 else f"{secret[0]}…{secret[-1]}"

@app.on_event("startup")
async def redis_startup_diagnostics():
    # 1️⃣  Pull env-vars *right here* so we’re sure we see what the container sees  
    redis_url      = os.getenv("CACHE_REDIS_URL", "redis://localhost:6379/0")
    redis_password = os.getenv("CACHE_REDIS_PASSWORD")  # may be None
    if redis_password:
        redis_password = redis_password.strip()         # strip stray \n or spaces

    # 2️⃣  Show raw inputs (masked where needed)
    logger.info("🔍 CACHE_REDIS_URL         = %s", redis_url)
    logger.info("🔍 CACHE_REDIS_PASSWORD    = %s", _mask(redis_password))
   

    # 3️⃣  Decode URL for sanity checks
    p      = urllib.parse.urlparse(redis_url)
    scheme = p.scheme or "redis"
    host   = p.hostname or "<missing>"
    port   = p.port or (6380 if scheme == "rediss" else 6379)
    db     = p.path.lstrip("/") or "0"
    url_pw = p.password

    logger.info("🔍 URL scheme             = %s", scheme)
    logger.info("🔍 Host / port            = %s : %s", host, port)
    logger.info("🔍 DB-index               = %s", db)
    logger.info("🔍 Password embedded?     = %s", bool(url_pw))
    if url_pw:
        logger.debug("🔍 URL embedded password = %s", _mask(url_pw))

    # 4️⃣  Which password will actually be used?
    final_pw = redis_password if redis_password is not None else url_pw
    logger.info("🔍 Final password used    = %s", _mask(final_pw))

    # 5️⃣  Try to connect & ping
    try:
        test_conn: Redis = Redis.from_url(redis_url, password=redis_password)
        await test_conn.ping()
        logger.info("✅ Redis ping OK – authentication succeeded.")
    except ResponseError as e:
        if "NOAUTH" in str(e):
            logger.error("❌ Redis replied NOAUTH – wrong / missing password.")
            logger.error("   • Check CACHE_REDIS_PASSWORD or embed in URL.")
            logger.error("   • Watch for stray newlines / spaces.")
        else:
            logger.exception("❌ Redis returned an error:")
    except (OSError, asyncio.TimeoutError) as e:
        logger.error("❌ Network error contacting Redis: %s", e)
        logger.error("   • Is Cloud Run attached to the correct VPC connector?")
        logger.error("   • Is port %s open in firewall rules?", port)
        try:
            ipaddress.ip_address(host)
        except ValueError:
            logger.warning("   • Hostname may not resolve inside the VPC.")
    except Exception:
        logger.exception("❌ Unexpected failure connecting to Redis")


# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],  # Or specify your frontend URL
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
# ---------- request schema ---------------------------------
class MessageIn(BaseModel):
    phone: str
    text:  str


async def _ensure_lua_loaded(redis: Redis) -> str:
    """
    Loads spam.lua into Redis the first time we need it and returns the SHA.
    Stores the SHA in the module-level variable spam_sha.
    """
    global spam_sha
    if spam_sha:
        return spam_sha

    lua_source = Path("spam.lua").read_text()
    spam_sha = await redis.script_load(lua_source)
    return spam_sha


async def check_spam_and_enqueue(
    redis: Redis,
    phone: str,
    dest_id: str,
    text: str
) -> None:
    """
    Same logic as before but keys are now scoped by destination number.
    """
    sha = await _ensure_lua_loaded(redis)

    state_key = f"spam_state:user:{phone}:dest:{dest_id}"
    buf_key   = f"buffer:user:{phone}:dest:{dest_id}"
    now       = time.time()

    args = [
        now, text,
        L1_MESSAGE_THRESHOLD, MESSAGE_COUNT_WINDOW, L1_BLOCK_DURATION,
        L1_ACTIVATIONS_FOR_L2, L1_ACTIVATION_WINDOW_FOR_L2, L2_BLOCK_DURATION,
        L2_ACTIVATIONS_FOR_L3, L2_ACTIVATION_WINDOW_FOR_L3, L3_BLOCK_DURATION,
    ]

    reply = await redis.evalsha(sha, 2, state_key, buf_key, *args)

    if reply[0] == 0:
        level, retry_in = reply[1], int(reply[2])
        raise HTTPException(
            429, f"User rate-limited ({level}). Try again in {retry_in}s.",
        )


def _parse_allowed_dests(env_value: str | None) -> dict[str, dict]:
    """
    Returns  {channelAccountId: {"channel_id": int, "label": str}}
    """
    if not env_value:
        return {}
    table: dict[str, dict] = {}
    for item in env_value.split(","):
        try:
            account_id, ch_id, *label = item.split(":")
            table[account_id] = {
                "channel_id": int(ch_id),
                "label":      ":".join(label) or account_id,
            }
        except ValueError:
            raise RuntimeError(f"Bad ALLOWED_HS_DESTS entry: {item}")
    return table

ALLOWED_DESTS: dict[str, dict] = _parse_allowed_dests(
    os.getenv("ALLOWED_HS_DESTS")
)

if not ALLOWED_DESTS:
    raise RuntimeError(
        "🚫  ALLOWED_HS_DESTS env-var empty – webhook would reject all messages."
)



@app.get("/hubspot/webhook")
async def verify(req: Request):
    if req.query_params.get("hub.mode") == "subscribe":
        return Response(
            content=req.query_params["hub.challenge"], status_code=200, media_type="text/plain"
        )

    print("received_webhook get request")
    return Response(status_code=400)


# ───────────────────────────────────────────────────────────────


@app.post("/hubspot/webhook")
async def hubspot_webhook(
    req: Request,
    redis: Redis = Depends(get_redis),
):
    logger.info("▶ hubspot_webhook called")

    if not await _is_valid_signature(req):
        raise HTTPException(401, "Invalid signature")

    try:
        events = await req.json()

        for event in events:
            if event.get("subscriptionType") != "conversation.newMessage":
                continue

            message_id = event.get("messageId")
            thread_id  = event.get("objectId")
            app_id     = event.get("appId")
            if not all([message_id, thread_id, app_id]):
                logger.debug("Skipping event with missing ids: %s", event)
                continue

            # ① Fetch message details
           
            (
                text, phone_number, sender_actor_id, channel_id, channel_account_id,
                client_type, sender_name, sender_label, bg_phone_number
            ) = await _get_conversation_details(thread_id, message_id)


            

            dest_id = str(channel_account_id)
            logger.info("HubSpot msg → thread=%s phone=%s dest=%s",
                        thread_id, phone_number, dest_id)
            
            expected = PIPELINE_BY_DEST.get(dest_id)


            # ② Allow‑list check
            dest_meta = ALLOWED_DESTS.get(dest_id)
            if not dest_meta or int(channel_id) != dest_meta["channel_id"]:
                continue

            if not phone_number:
                continue


            agent       = AGENT_ROUTING.get(dest_id, DEFAULT_AGENT)
            sid_key     = f"adk_session:user:{bg_phone_number}:dest:{agent.app_name}"
            sid_bytes   = await redis.get(sid_key)
            session_id  = sid_bytes.decode() if sid_bytes else None
            logger.info("[SESSION INFO] phone_number %s  bg_phone_number %s session_id %s agent.app_name %s", phone_number, bg_phone_number, session_id, agent.app_name)
            

            # 🔸 NEW: proactive / agent-first conversation ---------------------
            if session_id is None and client_type != "SYSTEM":
                # build a very small init_state – anything you already have is fine
                init_state = {
                    "dest_id":            dest_id,
                    "nombre_contacto_ws": sender_name or f"Prospect {bg_phone_number}",
                    "numero_de_telefono": bg_phone_number,
                    "fecha_y_hora":       datetime.now(GUAYAQUIL).strftime("%A, %Y-%m-%d %H:%M:%S"),
                    "proactive_seed":     True,                 # handy flag for debugging
                }
                # create & cache the session **synchronously**
                # log para saber que se hizo una session por que un admin escribio primero
                logger.info("[ADMIN INITIATED SESSION] for client %s ", bg_phone_number )
                session_id = await get_or_create_session(
                    redis, bg_phone_number, agent, init_state=init_state
                )

           

            row = {
                "numero_celular":  bg_phone_number,
                "sender":          sender_label,
                "mensaje":         text,
                "timestamp":       datetime.now(timezone.utc).isoformat(),

                # — NEW ————————————————————————————————
                "hubspot_thread":  str(thread_id),      # required BQ column
                # ————————————————————————————————

                "insertId":        message_id,          # dedup key for BQ
                "dest_table":      DEST_TABLE.get(dest_id),
                "id_conversacion": None,                # filled later (session_id)
            }

            if session_id:
                row["id_conversacion"] = session_id
                await log_rows_to_bq.kiq([row])              # normal path
            else:
                # park it; key chosen so worker can find it
                pend_key = f"pending_bq:{thread_id}"
                await redis.rpush(pend_key, json.dumps(row))
                await redis.expire(pend_key, 3600)   

                       
            if client_type != "SYSTEM":
                continue 
            else:
                if expected and expected.get("pipeline") and expected.get("stage"):
                    try:
                        await ensure_ticket_pipeline_for_thread(
                            thread_id=str(thread_id),
                            want_pipeline=expected["pipeline"],
                            want_stage=expected["stage"],
                        )
                    except Exception:
                        logger.exception("[PIPELINE] Failed to normalize ticket for thread %s", thread_id)

            # ③ Contact sync + ficha upsert
            clean_sender_name = sender_name.replace("Maybe: ", "") if sender_name else None

            # ── inside hubspot_webhook loop ───────────────────────────────────────
            contact_id, crm_name, was_created = await get_or_create_contact(
                phone_number, app_id, whatsapp_name=clean_sender_name
            )

            if was_created and contact_id:                           # ← run only for new contacts
                await _associate_contact_to_ticket(thread_id, contact_id)

            logger.info("HubSpot msg → thread=%s phone=%s dest=%s contact_id=%s",
                        thread_id, phone_number, dest_id, contact_id)
           
            if not contact_id:
                continue
            
            # ③  Ficha → HubSpot refresh logic  (24 h cache in Redis)
            SYNC_KEY      = f"{FICHA_SYNC_KEY_PREFIX}{phone_number}"
            last_sync_raw = await redis.get(SYNC_KEY)
            needs_refresh = True

            if last_sync_raw is not None:
                try:
                    last_sync_ts = float(last_sync_raw.decode())
                    needs_refresh = (time.time() - last_sync_ts) > FICHA_SYNC_INTERVAL
                    if not needs_refresh:
                        logger.info("no se va ha hacer refresh de ficha para numero %s por que el ultimo update fue hace menos del tiempo de refresh", phone_number)
                except Exception:
                    needs_refresh = True        # corrupt value → force refresh

            ficha_payload = {}
            if needs_refresh:
                ficha = await fetch_ficha_by_phone(phone_number)
                if ficha:
                    logger.info("👤 Tipti ficha for %s:\n%s",
                                phone_number, json.dumps(ficha, indent=2, ensure_ascii=False))
                    ficha_payload = build_contact_payload_from_ficha(ficha)
                    if ficha_payload:
                        await _update_contact_properties(contact_id, ficha_payload)
                        # stamp “last sync” (keep a 1-week TTL to avoid unbounded growth)
                        await redis.set(SYNC_KEY, str(time.time()), ex=7 * 24 * 60 * 60)
                else:
                    logger.info("Tipti ficha not fetched (dev env or no match). for %s", phone_number)

            # always pull latest props (after a possible update)
            hs_ficha_props   = await _get_contact_properties(contact_id, HS_FICHA_PROPS)
            user_profile_dict = ficha_payload or hs_ficha_props    # prefer fresh data

           
            # ④ Admin override — only enforced on the CLIENT channel
            if dest_id == CLIENT_CHANNEL:
                try:
                    if await _is_admin_responding(contact_id, app_id):
                        logger.info("[ADMIN_OVERRIDE] Skipping bot reply: admin responding on CLIENT channel.")
                        continue
                except Exception:
                    # Fail open so the bot keeps working even if HS read fails
                    logger.exception("[ADMIN_OVERRIDE] Admin check failed; proceeding with bot reply.")
            else:
                # Optional: visibility for debugging
                logger.debug("[ADMIN_OVERRIDE] Ignored for dest_id=%s (not CLIENT channel).", dest_id)

            # ⑤ Spam guard
            await check_spam_and_enqueue(redis, phone_number, dest_id, text)

            # ⑥ Prepare worker payload (no init_state here)
            final_name = crm_name or clean_sender_name or f"User {phone_number}"
            lock_key   = f"lock:user:{phone_number}:dest:{dest_id}"

            if await redis.set(lock_key, b"starter", nx=True, ex=300):
                await process_user_batch_task.kiq(
                    user_phone         = phone_number,
                    thread_id          = thread_id,
                    recipient_actor_id = sender_actor_id,
                    channel_id         = channel_id,
                    channel_account_id = channel_account_id,
                    app_id             = app_id,
                    initial_load       = True,
                    contact_name       = final_name,
                    dest_id            = dest_id,        # ← NEW, for worker
                    crm_profile        = user_profile_dict,
                )

    except json.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON body")
    except Exception as e:
        logger.exception("Webhook error: %s", e)

    return Response(status_code=200)

@app.post("/locust/webhook")
async def locust_webhook(
    req: Request,
    redis: Redis = Depends(get_redis),
):
    """
    A “twin” of the production webhook for Locust load-tests.

    • Bypasses HubSpot signature verification.
    • Lets the payload override `channel_account_id`, `phone_number`, etc.
    • Runs the **real** spam-guard and worker enqueue, but flags jobs as testing.
    """
    logger.info("Locust test webhook received a request.")
    tz_gye = ZoneInfo("America/Guayaquil")

    try:
        events = await req.json()

        for event in events:
            if event.get("subscriptionType") != "conversation.newMessage":
                continue

            # ── 1) Destination routing ──────────────────────────────────
            first_dest, first_meta = next(iter(ALLOWED_DESTS.items()))
            dest_id    = str(event.get("channel_account_id", first_dest))
            dest_meta  = ALLOWED_DESTS.get(dest_id, first_meta)
            channel_id = dest_meta["channel_id"]

            # ── 2) Dummy message fields (Locust can override) ───────────
            phone_number    = event.get("phone_number", "15551234567")
            thread_id       = event.get("objectId", 98765)
            sender_actor_id = "V-12345"
            text            = "Hola, ¿cuál es mi nombre y mi número de teléfono?"
            app_id          = event.get("appId", 54321)
            final_name      = "Locust User"

            # ── 3) Spam / rate-limit check (real Lua) ────────────────────
            try:
                await check_spam_and_enqueue(redis, phone_number, dest_id, text)
            except HTTPException:
                logger.info("Locust msg %s blocked by spam-guard", phone_number)
                continue

            # ── 4) Build test init_state (passes dest_id) ───────────────
            init_state = build_init_state(
                dest_id,
                nombre_contacto_ws = final_name,
                numero_de_telefono = phone_number,
                fecha_y_hora       = datetime.now(tz_gye).strftime("%A, %Y-%m-%d %H:%M:%S"),
                test_mode          = True,
            )

            # ── 5) Enqueue worker (keyed by user+dest) ──────────────────
            lock_key = f"lock:user:{phone_number}:dest:{dest_id}"
            if await redis.set(lock_key, b"starter", nx=True, ex=300):
                await process_user_batch_task.kiq(
                    user_phone         = phone_number,
                    thread_id          = thread_id,
                    recipient_actor_id = sender_actor_id,
                    channel_id         = str(channel_id),
                    channel_account_id = dest_id,
                    app_id             = app_id,
                    initial_load       = True,
                    contact_name       = final_name,
                    dest_id            = dest_id,       # ← **added**
                    init_state         = init_state,
                    is_testing         = True,
                    enqueue_time       = time.time(),
                )

    except json.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON body")
    except Exception as e:
        logger.exception("Locust webhook error: %s", e)
        raise HTTPException(500, str(e))

    return Response(status_code=200, content="Test webhook processed.")


def build_init_state(dest_id: str, **kwargs) -> dict:
    """
    Returns the dictionary that will be sent as ADK `state`.

    • `dest_id`  – HubSpot channelAccountId the user wrote to  
    • `**kwargs` – any extra name=value pairs the caller wants included
    """
    tz_gye = ZoneInfo("America/Guayaquil")
    state  = {
        "dest_id":        dest_id,
        "fecha_y_hora":   datetime.now(tz_gye).strftime("%A, %Y-%m-%d %H:%M:%S"),
    }
    # Optional, destination-specific tweaks
    if dest_id == "1930123456":
        state["soporte_flag"] = True

    state.update(kwargs)          # user-supplied extras win
    return state






@app.get("/redirect/")
async def hubspot_oauth_callback(request: Request):
    code  = request.query_params.get("code")
    state = request.query_params.get("state")

    if not code:
        raise HTTPException(status_code=400, detail="Missing code")

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.hubapi.com/oauth/v1/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type":    "authorization_code",
                "client_id":     CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "redirect_uri":  REDIRECT_URI,
                "code":          code,
            },
            timeout=10,
        )
    resp.raise_for_status()
    tokens = resp.json()
    # Persist to disk for dev; in prod use a DB or secrets-manager
    Path(TOKEN_FILE).write_text(json.dumps(tokens, indent=2))

    expires = time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.localtime(time.time() + tokens["expires_in"])
    )
    return Response(
        f"✅ HubSpot connected! Access-token valid until {expires}. "
        "You can close this tab.",
        media_type="text/plain",
    )
# --- Add this class and endpoint for easy testing ---
class ReplyPayload(BaseModel):
    thread_id: int
    message_text: str
    recipient_actor_id: str
    channel_id: str
    channel_account_id: str

@app.post("/test-reply")
async def test_send_reply(payload: ReplyPayload):
    """
    A test endpoint to manually send a reply to a HubSpot conversation thread.
    This corrected version no longer passes the unnecessary app_id.
    """
    response = await _send_reply_to_thread(
        thread_id=payload.thread_id,
        message_text=payload.message_text,
        recipient_actor_id=payload.recipient_actor_id,
        channel_id=payload.channel_id,
        channel_account_id=payload.channel_account_id
    )

    if response:
        return {"status": "success", "hubspot_response": response}
    else:
        raise HTTPException(status_code=500, detail="Failed to send reply. Check server logs for details.")



@app.get("/force-refresh-token")
async def force_refresh():
    """
    A manual endpoint to force a refresh of the HubSpot token.
    Useful for debugging or if the token becomes invalid for other reasons.
    """
    try:
        with open(TOKEN_FILE, "r") as f:
            token_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        raise HTTPException(status_code=400, detail=f"Could not read token file: {TOKEN_FILE}")

    refresh_token = token_data.get("refresh_token")
    if not refresh_token:
        raise HTTPException(status_code=400, detail="No refresh_token found in token file.")

    print("Forcing token refresh...")
    refresh_payload = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
    }
    
    async with httpx.AsyncClient() as refresh_client:
        response = await refresh_client.post(f"{BASE_API_URL}/oauth/v1/token", data=refresh_payload)
    
    if response.status_code != 200:
        print(f"Error refreshing token: {response.status_code} {response.text}")
        raise HTTPException(status_code=500, detail=f"Could not refresh HubSpot token: {response.text}")
    
    new_token_data = response.json()
    # Add the 'expires_at' field for our proactive checker
    new_token_data["expires_at"] = int(time.time()) + new_token_data["expires_in"]
    
    # Save the new token data to the file
    with open(TOKEN_FILE, "w") as f:
        json.dump(new_token_data, f, indent=2)
    
    print("Token refreshed and saved successfully.")
    return {"status": "success", "new_token_data": new_token_data}


@app.on_event("shutdown")
async def close_httpx():
    await client.aclose()

   