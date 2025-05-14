import threading
import time
from dataclasses import dataclass, field

@dataclass
class TokenBucketRateLimiter:
    """
    Simple Token-Bucket rate limiter
    'tokens_per_interval' tokens are replenished every 'interval' seconds,
      up to 'max_tokens'.
    Each .acquire_token() call consumes 1 token.
    """
    tokens_per_interval: int = 1
    interval: float = 7.0
    max_tokens: int = 1
    available_tokens: int = field(init=False)
    last_refill_time: float = field(init=False)
    lock: threading.Lock = field(init=False)

    def __post_init__(self) -> None:
        self.available_tokens = self.max_tokens
        self.last_refill_time = time.monotonic()
        self.lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill_time
        intervals_passed = int(elapsed // self.interval)
        if intervals_passed > 0:
            refill_amount = intervals_passed * self.tokens_per_interval
            self.available_tokens = min(
                self.available_tokens + refill_amount,
                self.max_tokens
            )
            self.last_refill_time += intervals_passed * self.interval

    def acquire_token(self) -> None:
        while True:
            with self.lock:
                self._refill()
                if self.available_tokens > 0:
                    self.available_tokens -= 1
                    return
            time.sleep(0.05)
