"""HTTP requests that identify the Overture Maps client."""

from importlib.metadata import version
from urllib.request import Request, urlopen


def open_url(url: str):
    """Open a URL with an application User-Agent instead of urllib's default."""
    request = Request(url, headers={"User-Agent": f"overturemaps/{version('overturemaps')}"})
    return urlopen(request)
