import logging
from typing import Optional
import redis.asyncio as aioredis

logger = logging.getLogger("DedupService")


class RedisDedupService:
    """Prevents duplicate bank webhook notifications using Redis Atomic SETNX."""

    def __init__(self, redis_client: aioredis.Redis, ttl_seconds: int = 86400):
        self._redis = redis_client
        self._ttl = ttl_seconds

    async def is_duplicate(self, txid: Optional[str]) -> bool:
        if not txid or not str(txid).strip():
            logger.warning("Empty TxID provided to dedup check; bypassing duplicate filter.")
            return False

        clean_txid = str(txid).strip()
        key = f"dedup:tx:{clean_txid}"
        try:
            # SETNX ធានាថា key មិនស្ទួន និងកំណត់ TTL ក្នុង atomic operation តែមួយ
            is_new = await self._redis.set(key, "1", ex=self._ttl, nx=True)
            return not bool(is_new)
        except Exception as e:
            logger.error("Redis dedup check error for TxID %s: %s", clean_txid, e)
            return False

    async def release(self, txid: Optional[str]) -> None:
        """លុប key ចេញពី Redis វិញ ក្នុងករណី broadcast បរាជ័យ ដើម្បីអនុញ្ញាតឱ្យ retry។"""
        if not txid or not str(txid).strip():
            return

        clean_txid = str(txid).strip()
        key = f"dedup:tx:{clean_txid}"
        try:
            await self._redis.delete(key)
            logger.info("Released dedup lock for TxID: %s", clean_txid)
        except Exception as e:
            logger.error("Failed to release Redis key for TxID %s: %s", clean_txid, e)


# Backward-compatible alias
DedupService = RedisDedupService