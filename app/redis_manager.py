import asyncio
import json
import logging
from typing import Dict, Set
from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class ProjectRelayManager:
    """
    Quản lý subscribe Redis cho từng project_id và chuyển tiếp đến Socket.IO rooms.
    - 1 task subscribe/ project_id (dù có nhiều client)
    - Tự hủy subscribe khi không còn client nghe
    """

    def __init__(self, sio, redis_url: str):
        self.sio = sio
        self.redis_url = redis_url
        self._locks: Dict[str, asyncio.Lock] = {}
        self._rooms: Dict[str, Set[str]] = {}  # project_id -> {room, ...}
        self._tasks: Dict[str, asyncio.Task] = {}  # project_id -> task

    def _channel(self, project_id: str) -> str:
        return f"project:{project_id}"

    def rooms_for(self, project_id: str) -> Set[str]:
        return self._rooms.setdefault(project_id, set())

    def lock_for(self, project_id: str) -> asyncio.Lock:
        if project_id not in self._locks:
            self._locks[project_id] = asyncio.Lock()
        return self._locks[project_id]

    async def join(self, project_id: str, room: str):
        async with self.lock_for(project_id):
            rooms = self.rooms_for(project_id)
            rooms.add(room)
            if project_id not in self._tasks:
                self._tasks[project_id] = asyncio.create_task(
                    self._subscribe_and_forward(project_id)
                )
                logger.info("Started relay task for project_id=%s", project_id)

    async def leave(self, project_id: str, room: str):
        async with self.lock_for(project_id):
            rooms = self.rooms_for(project_id)
            rooms.discard(room)
            if not rooms:
                task = self._tasks.pop(project_id, None)
                if task:
                    task.cancel()
                    logger.info("Cancelled relay task for project_id=%s", project_id)

    async def _subscribe_and_forward(self, project_id: str):
        channel = self._channel(project_id)
        redis = Redis.from_url(self.redis_url)
        pubsub = redis.pubsub()
        try:
            await pubsub.subscribe(channel)
            logger.info("Subscribed Redis channel=%s", channel)
            async for message in pubsub.listen():
                if not message or message.get("type") != "message":
                    continue
                raw = message.get("data")
                try:
                    payload = json.loads(raw)
                except Exception:
                    continue
                # forward tới tất cả rooms của project_id
                for room in list(self.rooms_for(project_id)):
                    await self.sio.emit("progress", payload, room=room)
        except asyncio.CancelledError:
            logger.info("Relay task cancelled for project_id=%s", project_id)
        except Exception:
            logger.exception("Relay loop crashed for project_id=%s", project_id)
        finally:
            try:
                await pubsub.unsubscribe(channel)
            except Exception:
                pass
            await pubsub.close()
            await redis.close()
            logger.info("Closed Redis for project_id=%s", project_id)
