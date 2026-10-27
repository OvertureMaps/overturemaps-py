"""HTTP requests that identify the Overture Maps client."""

import platform
from importlib.metadata import version
from typing import Optional
from urllib.request import Request, urlopen

DEFAULT_TIMEOUT = 30


def _user_agent() -> str:
    python_version = platform.python_version()
    system = platform.system()
    return f"overturemaps/{version('overturemaps')} Python/{python_version} ({system})"


def open_url(url: str, timeout: Optional[int] = None, accept: Optional[str] = None):
    """Open a URL with an application User-Agent instead of urllib's default."""
    headers = {"User-Agent": _user_agent()}
    if accept:
        headers["Accept"] = accept
    request = Request(url, headers=headers)
    return urlopen(request, timeout=timeout if timeout is not None else DEFAULT_TIMEOUT)
