import redis
import os

# ភ្ជាប់ទៅកាន់ Redis Container
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def is_duplicate_transaction(txid: str, expire_seconds: int = 86400) -> bool:
    """
    ពិនិត្យមើលថាតើ txid នេះធ្លាប់បានដំណើរការរួចរាល់ហើយឬនៅ។
    ប្រសិនបើជាលើកដំបូង វានឹង save ចូល Redis ហើយ return False (មិនស្ទួន)។
    ប្រសិនបើធ្លាប់មានហើយ វានឹង return True (ស្ទួន)។
    """
    # setnx: កំណត់តម្លៃបានតែពេលដែល key មិនទាន់មានក្នុង Redis ប៉ុណ្ណោះ
    is_new = r.set(f"tx:{txid}", "PROCESSED", nx=True, ex=expire_seconds)
    return not is_new