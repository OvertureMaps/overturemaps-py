"""Tests for application identification in HTTP requests."""

from urllib.request import Request

from overturemaps import _http


def test_open_url_sends_application_user_agent(mocker):
    url = "https://stac.overturemaps.org/catalog.json"
    version = mocker.patch("overturemaps._http.version", return_value="1.2.3")
    urlopen = mocker.patch("overturemaps._http.urlopen")

    response = _http.open_url(url)

    version.assert_called_once_with("overturemaps")
    urlopen.assert_called_once()
    request = urlopen.call_args.args[0]
    assert isinstance(request, Request)
    assert request.full_url == url
    assert request.get_header("User-agent") == "overturemaps/1.2.3"
    assert response is urlopen.return_value


def test_open_url_uses_default_timeout(mocker):
    urlopen = mocker.patch("overturemaps._http.urlopen")

    _http.open_url("https://stac.overturemaps.org/catalog.json")

    assert urlopen.call_args.kwargs["timeout"] == _http.DEFAULT_TIMEOUT


def test_open_url_accepts_timeout_override(mocker):
    urlopen = mocker.patch("overturemaps._http.urlopen")

    _http.open_url("https://stac.overturemaps.org/catalog.json", timeout=5)

    assert urlopen.call_args.kwargs["timeout"] == 5
