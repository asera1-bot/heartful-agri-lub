import time

def with_retry(func, retries=2, wait=1):
    for attempt in range(retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt == retries:
                raise
            time.sleep(wait)
