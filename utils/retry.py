import time
import requests


def fetch_with_retry(url, *, retries=3, backoff=2, **kwargs):
    last_exc = None
    last_response = None
    for attempt in range(retries):
        try:
            response = requests.get(url, **kwargs)
            last_response = response
            if response.status_code == 429 or response.status_code >= 500:
                if attempt < retries - 1:
                    time.sleep(backoff * (2 ** attempt))
                    continue
            return response
        except requests.exceptions.RequestException as e:
            last_exc = e
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
    if last_exc:
        raise last_exc
    return last_response
