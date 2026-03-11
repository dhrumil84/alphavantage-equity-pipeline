import time
from collections import deque
import threading
import logging

logger = logging.getLogger(__name__)

class RateLimiter:
    """
    A token bucket rate limiter to enforce a maximum number of calls
    within a rolling time window.

    This implementation uses a deque to track the timestamps of recent calls
    and blocks (sleeps) when the rate limit would be exceeded. It is
    thread-safe using a threading RLock.

    Example Usage:
        limiter = RateLimiter(calls_per_minute=75)
        for symbol in symbols:
            with limiter:
                # API call here
            
            # OR as a simple callable:
            limiter.wait()
            # API call here
    """
    def __init__(self, calls_per_minute: int = 75, window_seconds: int = 60):
        self.capacity = calls_per_minute
        self.window_seconds = window_seconds
        # Store timestamps of calls
        self.timestamps = deque()
        # Ensure thread safety
        self.lock = threading.RLock()
        
    def wait(self) -> None:
        """
        Blocks the current thread if the rate limit has been reached,
        until enough time has passed to allow another call.
        """
        with self.lock:
            now = time.time()
            
            # Remove timestamps older than our window
            while self.timestamps and (now - self.timestamps[0] >= self.window_seconds):
                self.timestamps.popleft()
                
            # If we are at capacity, calculate how long to wait
            if len(self.timestamps) >= self.capacity:
                # The oldest call in the window dictates when the next slot opens
                oldest_call_time = self.timestamps[0]
                sleep_time = (oldest_call_time + self.window_seconds) - now
                
                if sleep_time > 0:
                    logger.debug(f"Rate limit reached ({self.capacity} calls in {self.window_seconds}s). Sleeping for {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                
                # After sleeping, time has advanced, so we pop the oldest
                # (which must now be outside the window)
                self.timestamps.popleft()
                # Recalculate 'now' after sleeping
                now = time.time()
            
            # Record the new call
            self.timestamps.append(now)

    def __enter__(self):
        """Allows context manager usage."""
        self.wait()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Allows context manager usage."""
        pass
