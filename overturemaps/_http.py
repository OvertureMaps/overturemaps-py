"""HTTP requests that identify the Overture Maps client."""

import platform
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from importlib.metadata import version
from typing import Optional
from urllib.error import HTTPError
from urllib.request import Request, urlopen

DEFAULT_TIMEOUT = 30
# Retries for callers that opt in: HTTP 429 (rate limited) and 5xx responses are
# retried this many times, with exponential backoff.
MAX_RETRIES = 3
RETRY_BACKOFF = 1.0
# Upper bound for a single wait, including one requested by a Retry-After header.
MAX_RETRY_DELAY = 30.0


def _user_agent() -> str:
    python_version = platform.python_version()
    system = platform.system()
    return f"overturemaps/{version('overturemaps')} Python/{python_version} ({system})"


def _is_retryable(error: HTTPError) -> bool:
    return error.code == 429 or 500 <= error.code < 600


def _retry_after(error: HTTPError) -> Optional[float]:
    """Return the wait in seconds requested by a Retry-After header, if any.

    The header holds either a number of seconds or an HTTP date.
    """
    value = error.headers.get("Retry-After") if error.headers is not None else None
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())


def _retry_delay(error: HTTPError, attempt: int) -> float:
    delay = _retry_after(error)
    if delay is None:
        delay = RETRY_BACKOFF * 2**attempt + random.uniform(0, RETRY_BACKOFF)
    return min(delay, MAX_RETRY_DELAY)


def open_url(
    url: str,
    timeout: Optional[int] = None,
    accept: Optional[str] = None,
    retries: int = 0,
):
    """Open a URL with an application User-Agent instead of urllib's default.

    HTTP 429 and 5xx responses are retried up to ``retries`` times (none by
    default) with exponential backoff, honouring the server's Retry-After header.
    """
    headers = {"User-Agent": _user_agent()}
    if accept:
        headers["Accept"] = accept
    request = Request(url, headers=headers)
    attempt = 0
    while True:
        try:
            return urlopen(
                request, timeout=timeout if timeout is not None else DEFAULT_TIMEOUT
            )
        except HTTPError as error:
            if attempt >= retries or not _is_retryable(error):
                raise
            delay = _retry_delay(error, attempt)
            error.close()
        time.sleep(delay)
        attempt += 1
