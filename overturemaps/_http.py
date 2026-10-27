"""HTTP requests that identify the Overture Maps client."""

from importlib.metadata import version
from typing import Optional
from urllib.request import Request, urlopen

DEFAULT_TIMEOUT = 30


def open_url(url: str, timeout: Optional[int] = None):
    """Open a URL with an application User-Agent instead of urllib's default."""
    request = Request(url, headers={"User-Agent": f"overturemaps/{version('overturemaps')}"})
    return urlopen(request, timeout=timeout if timeout is not None else DEFAULT_TIMEOUT)
