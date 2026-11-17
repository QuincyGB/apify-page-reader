"""Page -> Markdown/text extraction core.

Ported from StallRow's /v1/page-to-markdown implementation
(~/workspace/stall-row/app/services/fetch.py, the page->markdown
section). The pipeline is unchanged in behaviour:

- SSRF guard: every URL (and every redirect hop) is resolved and
  rejected if it points at a private, loopback, link-local, reserved,
  or multicast address.
- Fetch hardening: retries with backoff on transient statuses, a
  descriptive User-Agent after a bot-filter 403, manual redirect
  following (each hop SSRF-checked before it is fetched), a 5 MiB
  body cap, explicit charset decoding, and plain-text / Markdown
  bodies served as text instead of failing extraction.
- Extraction: trafilatura main-content extraction (markdown or txt),
  with a visible-text fallback when trafilatura's heuristics reject a
  real page.
- Last resort: if the direct pipeline fails end-to-end (JS-challenge /
  consent walls), one attempt through a public reader proxy; the
  result is marked in-band (fallback="reader-proxy") so it is never
  mistaken for a direct extraction.

Honesty rule (inherited from the source): a page that cannot be
fetched or extracted raises — the caller never delivers (or charges
for) fabricated content.
"""

from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
from html import unescape as html_unescape
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import httpx


class UpstreamError(Exception):
    """The upstream page failed or returned unusable data."""


_UA = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )
}

# A descriptive UA, used after a 403: Wikimedia's robot policy (and many
# others) refuses impersonating browser UAs from datacenter egress but
# accepts an identified bot.
_DESCRIPTIVE_UA = {
    "User-Agent": (
        "PageReader/1.0 (Apify actor; page content extractor)"
    )
}
_PAGE_ACCEPT = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "text/plain;q=0.8,text/markdown;q=0.8,*/*;q=0.5"
)
_PAGE_HEADERS_BASE = {"Accept": _PAGE_ACCEPT, "Accept-Language": "en-US,en;q=0.9"}

_MAX_PAGE_BYTES = 5 * 1024 * 1024  # fail cleanly past 5 MiB
_MAX_REDIRECTS = 5
_FETCH_ATTEMPTS = 3
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_BACKOFF_S = (0.5, 1.5)

_HTML_TYPES = frozenset({"text/html", "application/xhtml+xml"})
_TEXT_TYPES = frozenset({"text/plain", "text/markdown", "text/x-markdown"})
_MIN_FALLBACK_WORDS = 15


# --- SSRF guard ---------------------------------------------------------------


def ssrf_check(url: str) -> None:
    """Reject non-http(s) URLs and any host resolving to a private,
    loopback, link-local, reserved, or multicast address."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"URL scheme must be http or https, got {parsed.scheme!r}")
    host = parsed.hostname
    if not host:
        raise ValueError("URL has no host")
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror as e:
        raise ValueError(f"host does not resolve: {host}") from e
    for info in infos:
        ip_str = info[4][0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            raise ValueError(f"URL host resolves to a non-public address ({ip})")


# --- Fetch pipeline -----------------------------------------------------------


async def _read_capped(resp: httpx.Response) -> bytes:
    chunks: list[bytes] = []
    total = 0
    async for chunk in resp.aiter_bytes():
        total += len(chunk)
        if total > _MAX_PAGE_BYTES:
            raise UpstreamError(
                f"page exceeds the {_MAX_PAGE_BYTES // (1024 * 1024)} MiB size limit"
            )
        chunks.append(chunk)
    return b"".join(chunks)


async def _get_following_redirects(
    client: httpx.AsyncClient, url: str, headers: dict
) -> tuple[str, int, httpx.Headers, bytes | None]:
    """One GET with manual redirect following: every redirect target is
    SSRF-checked BEFORE it is fetched (client-level following would only
    let us check the final URL, after the target was already contacted).
    The body is read (size-capped) only on a 200."""
    current = url
    for hop in range(_MAX_REDIRECTS + 1):
        async with client.stream(
            "GET", current, headers=headers, follow_redirects=False
        ) as resp:
            if resp.status_code in (301, 302, 303, 307, 308):
                location = resp.headers.get("location")
                if not location:
                    raise UpstreamError(
                        f"page returned HTTP {resp.status_code} without a Location header"
                    )
                if hop == _MAX_REDIRECTS:
                    raise UpstreamError("page redirected too many times")
                target = urljoin(current, location)
                # getaddrinfo is blocking DNS — keep it off the event loop.
                await asyncio.to_thread(ssrf_check, target)
                current = target
                continue
            body = await _read_capped(resp) if resp.status_code == 200 else None
            return current, resp.status_code, resp.headers, body
    raise UpstreamError("page redirected too many times")  # pragma: no cover


async def _fetch_page(
    client: httpx.AsyncClient, url: str
) -> tuple[str, httpx.Headers, bytes]:
    """Fetch one page with retries. Transient statuses (429/5xx) and
    transport errors retry with backoff under the browser UA; a 403
    (bot filter) switches the next attempt to the descriptive UA.
    Deterministic failures raise UpstreamError."""
    use_descriptive = False
    last_error = "page fetch failed"
    for attempt in range(_FETCH_ATTEMPTS):
        if attempt:
            await asyncio.sleep(_BACKOFF_S[attempt - 1])
        headers = {
            **(_DESCRIPTIVE_UA if use_descriptive else _UA),
            **_PAGE_HEADERS_BASE,
        }
        try:
            final_url, status, resp_headers, body = await _get_following_redirects(
                client, url, headers
            )
        except httpx.HTTPError as e:
            last_error = f"page fetch failed: {e}"
            continue
        if status == 200:
            assert body is not None  # read exactly on 200 above
            return final_url, resp_headers, body
        last_error = f"page returned HTTP {status}"
        if status == 403:
            use_descriptive = True
            continue
        if status in _RETRY_STATUSES:
            continue
        raise UpstreamError(last_error)
    raise UpstreamError(last_error)


# --- Decoding + text helpers ----------------------------------------------------


def _decode_body(body: bytes, headers: httpx.Headers) -> str:
    """Decode with the declared charset, then a <meta charset>, then
    UTF-8; never raise on a bad label or a stray byte."""
    charset = None
    m = re.search(
        r"charset=([\w.:-]+)", headers.get("content-type", ""), re.IGNORECASE
    )
    if m:
        charset = m.group(1)
    if not charset:
        head = body[:4096].decode("ascii", errors="ignore")
        m = re.search(r"<meta[^>]+charset=[\"']?([\w.:-]+)", head, re.IGNORECASE)
        if m:
            charset = m.group(1)
    for enc in (charset, "utf-8"):
        if not enc:
            continue
        try:
            return body.decode(enc)
        except (LookupError, UnicodeDecodeError):
            continue
    return body.decode("utf-8", errors="replace")


def _looks_like_html(text: str) -> bool:
    head = text.lstrip()[:512].lower()
    return head.startswith(("<!doctype html", "<html")) or "<html" in head[:256]


def _normalize_text(text: str) -> str:
    """Light normalization for plain-text deliveries: unify newlines,
    trim trailing spaces, collapse 2+ blank lines to one."""
    lines = [
        ln.rstrip()
        for ln in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    ]
    out: list[str] = []
    blanks = 0
    for ln in lines:
        if ln.strip():
            out.append(ln)
            blanks = 0
        else:
            blanks += 1
            if blanks == 1:
                out.append("")
    return "\n".join(out).strip("\n")


def _text_title(normalized: str) -> str | None:
    """A Markdown document's leading '# ' heading is its title."""
    for ln in normalized.split("\n"):
        s = ln.strip()
        if not s:
            continue
        if s.startswith("# "):
            return s[2:].strip() or None
        return None
    return None


class _VisibleText(HTMLParser):
    """Collect visible text with block boundaries as newlines; script,
    style, head and template content is not visible text."""

    _SKIP = frozenset({"script", "style", "head", "template", "svg"})
    _BLOCK = frozenset(
        {
            "address", "article", "aside", "blockquote", "br", "dd", "div",
            "dl", "dt", "fieldset", "figcaption", "figure", "footer", "form",
            "h1", "h2", "h3", "h4", "h5", "h6", "header", "hr", "li",
            "main", "nav", "ol", "p", "pre", "section", "table", "tr",
            "ul",
        }
    )

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:  # noqa: ANN001
        if tag in self._SKIP:
            self._skip_depth += 1
        elif tag in self._BLOCK:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP:
            if self._skip_depth:
                self._skip_depth -= 1
        elif tag in self._BLOCK:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip_depth:
            self.parts.append(data)


def _visible_text(html_text: str) -> str:
    parser = _VisibleText()
    try:
        parser.feed(html_text)
    except Exception:  # noqa: BLE001 — malformed HTML: keep what parsed
        pass
    raw = "".join(parser.parts)
    squashed = "\n".join(
        re.sub(r"[ \t]+", " ", ln) for ln in raw.split("\n")
    )
    return _normalize_text(squashed)


# --- Extraction -----------------------------------------------------------------


def _extract_content(text: str, output_format: str) -> str | None:
    """Main-content extraction. CPU-bound (trafilatura heuristics, with
    the visible-text fallback), so extract_page runs it via
    asyncio.to_thread."""
    import trafilatura

    content = trafilatura.extract(
        text,
        output_format="markdown" if output_format == "markdown" else "txt",
        include_links=(output_format == "markdown"),
        include_images=False,
    )
    if not content:
        # Trafilatura's heuristics reject some real pages (odd structure,
        # link-dense indexes). Fall back to the page's visible text —
        # but only when there is enough of it to be a delivery, not
        # boilerplate dressed up as content.
        fallback = _visible_text(text)
        if len(fallback.split()) >= _MIN_FALLBACK_WORDS:
            content = fallback
    return content


async def _extract_direct(
    client: httpx.AsyncClient, url: str, output_format: str
) -> dict:
    await asyncio.to_thread(ssrf_check, url)  # blocking DNS — off the loop
    final_url, headers, body = await _fetch_page(client, url)
    text = _decode_body(body, headers)
    ctype = headers.get("content-type", "").split(";")[0].strip().lower()

    if ctype in _TEXT_TYPES and not _looks_like_html(text):
        content = _normalize_text(text)
        if not content:
            raise UpstreamError("page returned no readable text content")
        return {
            "url": url,
            "final_url": final_url,
            "title": _text_title(content),
            "content": content,
            "word_count": len(content.split()),
        }

    if (
        ctype
        and ctype not in _HTML_TYPES
        and ctype not in _TEXT_TYPES
        and not _looks_like_html(text)
    ):
        raise UpstreamError(f"unsupported content type: {ctype}")

    content = await asyncio.to_thread(_extract_content, text, output_format)
    if not content:
        raise UpstreamError("no main content could be extracted from the page")
    title = None
    m = re.search(r"<title[^>]*>(.*?)</title>", text, re.IGNORECASE | re.DOTALL)
    if m:
        cleaned = html_unescape(re.sub(r"<[^>]+>", "", m.group(1)))
        title = re.sub(r"\s+", " ", cleaned).strip() or None
    return {
        "url": url,
        "final_url": final_url,
        "title": title,
        "content": content,
        "word_count": len(content.split()),
    }


# Reader-proxy fallback (inherited from the source service): the one
# delivery failure class the fetch hardening cannot close is the
# JS-challenge / consent wall. As a LAST resort (direct pipeline failed
# end-to-end), one attempt through a public reader proxy; the delivery
# is marked in-band (fallback="reader-proxy") so it is never mistaken
# for a direct extraction. Best-effort by design: any failure there
# simply re-raises the direct error. The target URL was SSRF-checked
# by the direct path before this ever runs, and a ValueError (SSRF /
# bad URL) never reaches the fallback at all.
_READER_PROXY_PREFIX = "https://r.jina.ai/"


async def _reader_fallback(client: httpx.AsyncClient, url: str) -> dict | None:
    try:
        resp = await client.get(
            _READER_PROXY_PREFIX + url,
            headers={**_UA, "Accept": "text/plain"},
            follow_redirects=True,
        )
    except httpx.HTTPError:
        return None
    if resp.status_code != 200:
        return None
    content = _normalize_text(resp.text)
    if len(content.split()) < _MIN_FALLBACK_WORDS:
        return None
    return {
        "url": url,
        "final_url": url,
        "title": _text_title(content),
        "content": content,
        "word_count": len(content.split()),
        "fallback": "reader-proxy",
    }


async def extract_page(
    client: httpx.AsyncClient, url: str, output_format: str = "markdown"
) -> dict:
    """Extract one page. Returns {url, final_url, title, content,
    word_count} (+ fallback marker when the reader proxy delivered).
    Raises ValueError for a refused URL (SSRF / bad scheme) and
    UpstreamError for a fetch/extraction failure."""
    try:
        return await _extract_direct(client, url, output_format)
    except UpstreamError:
        fallback = await _reader_fallback(client, url)
        if fallback is not None:
            return fallback
        raise
