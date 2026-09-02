import logging
import redis
from app.core.config import settings

logger = logging.getLogger("DedupService")


class RedisDedupService:
    """Atomic transaction deduplication using Redis SETNX."""

    def __init__(self):
        self._pool = redis.ConnectionPool(
            host=settings.redis_host,
            port=settings.redis_port,
            db=0,
            decode_responses=True,
            socket_timeout=3.0,
            socket_connect_timeout=3.0,
        )
        self._redis = redis.Redis(connection_pool=self._pool)

    def is_duplicate(self, txid: str, ttl_seconds: int = 86400) -> bool:
        if not txid:
            return False
        try:
            is_new = self._redis.set(f"tx:{txid.strip()}", "PROCESSED", nx=True, ex=ttl_seconds)
            return not bool(is_new)
        except redis.RedisError as e:
            logger.error(f"Redis error checking txid '{txid}': {e}")
            return False

    def release(self, txid: str) -> None:
        try:
            self._redis.delete(f"tx:{txid.strip()}")
        except redis.RedisError:
            pass