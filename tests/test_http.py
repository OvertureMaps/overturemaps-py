"""Tests for application identification in HTTP requests."""

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from urllib.error import HTTPError
from urllib.request import Request

import pytest

from overturemaps import _http

URL = "https://stac.overturemaps.org/2026-10-21.0/collections.parquet"


def _http_error(code, headers=None):
    return HTTPError(URL, code, "error", headers or {}, None)


@pytest.fixture
def sleep(mocker):
    mocker.patch("overturemaps._http.random.uniform", return_value=0.0)
    return mocker.patch("overturemaps._http.time.sleep")


def test_open_url_sends_application_user_agent(mocker):
    url = "https://stac.overturemaps.org/catalog.json"
    mocker.patch("overturemaps._http.version", return_value="1.2.3")
    mocker.patch("overturemaps._http.platform.python_version", return_value="3.12.0")
    mocker.patch("overturemaps._http.platform.system", return_value="Linux")
    urlopen = mocker.patch("overturemaps._http.urlopen")

    response = _http.open_url(url)

    urlopen.assert_called_once()
    request = urlopen.call_args.args[0]
    assert isinstance(request, Request)
    assert request.full_url == url
    assert request.get_header("User-agent") == "overturemaps/1.2.3 Python/3.12.0 (Linux)"
    assert response is urlopen.return_value


def test_open_url_uses_default_timeout(mocker):
    urlopen = mocker.patch("overturemaps._http.urlopen")

    _http.open_url("https://stac.overturemaps.org/catalog.json")

    assert urlopen.call_args.kwargs["timeout"] == _http.DEFAULT_TIMEOUT


def test_open_url_accepts_timeout_override(mocker):
    urlopen = mocker.patch("overturemaps._http.urlopen")

    _http.open_url("https://stac.overturemaps.org/catalog.json", timeout=5)

    assert urlopen.call_args.kwargs["timeout"] == 5


def test_open_url_sends_accept_header(mocker):
    urlopen = mocker.patch("overturemaps._http.urlopen")

    _http.open_url("https://stac.overturemaps.org/catalog.json", accept="application/json")

    request = urlopen.call_args.args[0]
    assert request.get_header("Accept") == "application/json"


def test_open_url_omits_accept_header_by_default(mocker):
    urlopen = mocker.patch("overturemaps._http.urlopen")

    _http.open_url("https://stac.overturemaps.org/catalog.json")

    request = urlopen.call_args.args[0]
    assert request.get_header("Accept") is None


def test_open_url_does_not_retry_by_default(mocker, sleep):
    urlopen = mocker.patch("overturemaps._http.urlopen", side_effect=_http_error(429))

    with pytest.raises(HTTPError):
        _http.open_url(URL)

    assert urlopen.call_count == 1
    sleep.assert_not_called()


@pytest.mark.parametrize("code", [429, 500, 502, 503, 504])
def test_open_url_retries_rate_limits_and_server_errors(mocker, sleep, code):
    response = object()
    urlopen = mocker.patch(
        "overturemaps._http.urlopen",
        side_effect=[_http_error(code), _http_error(code), response],
    )

    assert _http.open_url(URL, retries=_http.MAX_RETRIES) is response
    assert urlopen.call_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [1.0, 2.0]


@pytest.mark.parametrize("code", [400, 403, 404])
def test_open_url_does_not_retry_client_errors(mocker, sleep, code):
    urlopen = mocker.patch("overturemaps._http.urlopen", side_effect=_http_error(code))

    with pytest.raises(HTTPError) as excinfo:
        _http.open_url(URL, retries=_http.MAX_RETRIES)

    assert excinfo.value.code == code
    assert urlopen.call_count == 1
    sleep.assert_not_called()


def test_open_url_gives_up_after_max_retries(mocker, sleep):
    urlopen = mocker.patch("overturemaps._http.urlopen", side_effect=_http_error(429))

    with pytest.raises(HTTPError) as excinfo:
        _http.open_url(URL, retries=_http.MAX_RETRIES)

    assert excinfo.value.code == 429
    assert urlopen.call_count == _http.MAX_RETRIES + 1
    assert [call.args[0] for call in sleep.call_args_list] == [1.0, 2.0, 4.0]


def test_open_url_honours_retry_after_seconds(mocker, sleep):
    mocker.patch(
        "overturemaps._http.urlopen",
        side_effect=[_http_error(429, {"Retry-After": "7"}), object()],
    )

    _http.open_url(URL, retries=_http.MAX_RETRIES)

    sleep.assert_called_once_with(7.0)


def test_open_url_honours_retry_after_http_date(mocker, sleep):
    retry_at = datetime.now(timezone.utc) + timedelta(seconds=10)
    mocker.patch(
        "overturemaps._http.urlopen",
        side_effect=[
            _http_error(503, {"Retry-After": format_datetime(retry_at, usegmt=True)}),
            object(),
        ],
    )

    _http.open_url(URL, retries=_http.MAX_RETRIES)

    # HTTP dates have whole-second precision.
    assert 8.0 < sleep.call_args.args[0] <= 10.0


def test_open_url_caps_long_retry_after(mocker, sleep):
    mocker.patch(
        "overturemaps._http.urlopen",
        side_effect=[_http_error(429, {"Retry-After": "3600"}), object()],
    )

    _http.open_url(URL, retries=_http.MAX_RETRIES)

    sleep.assert_called_once_with(_http.MAX_RETRY_DELAY)


def test_open_url_ignores_unparseable_retry_after(mocker, sleep):
    mocker.patch(
        "overturemaps._http.urlopen",
        side_effect=[_http_error(429, {"Retry-After": "soon"}), object()],
    )

    _http.open_url(URL, retries=_http.MAX_RETRIES)

    sleep.assert_called_once_with(1.0)
