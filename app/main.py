import logging
import os
from typing import Any, Dict

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import socketio

from .redis_manager import ProjectRelayManager

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fastapi-socketio")


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/2")
# CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]
CORS_ORIGINS = "*"
# Log CORS_ORIGINS value at startup
logger.info(f"CORS_ORIGINS={CORS_ORIGINS}")
SIO_PATH = os.getenv("SIO_PATH", "/socket.io")

# Socket.IO server (ASGI)
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins=CORS_ORIGINS or "*")
_fastapi = FastAPI(title="Project Planning Progress Relay")

_fastapi.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

relay = ProjectRelayManager(sio=sio, redis_url=REDIS_URL)

# Mount Socket.IO lên cùng app
app = socketio.ASGIApp(sio, other_asgi_app=_fastapi, socketio_path=SIO_PATH)


@sio.event
async def connect(sid, environ, auth):
    logger.info("Client connected sid=%s", sid)


@sio.event
async def disconnect(sid):
    logger.info("Client disconnected sid=%s", sid)


@sio.event
async def join(sid, data: Dict[str, Any]):
    project_id = (data or {}).get("projectId")
    if not project_id:
        await sio.emit("error", {"message": "projectId is required"}, to=sid)
        return
    room = f"project:{project_id}"
    await sio.save_session(sid, {"project_id": project_id, "room": room})
    await sio.enter_room(sid, room)
    await relay.join(project_id, room)
    await sio.emit("joined", {"room": room}, to=sid)


@sio.event
async def leave(sid, data: Dict[str, Any] = None):
    sess = await sio.get_session(sid)
    room = (data or {}).get("room") or (sess.get("room") if sess else None)
    project_id = (data or {}).get("projectId") or (
        sess.get("project_id") if sess else None
    )
    if not room or not project_id:
        return
    await sio.leave_room(sid, room)
    await relay.leave(project_id, room)
