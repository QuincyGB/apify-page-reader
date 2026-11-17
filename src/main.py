"""Page Reader — an Apify actor that turns web pages into clean
Markdown (or plain text).

Input: a list of start URLs, a max-pages cap, and an output format.
Each successfully extracted page becomes one dataset item and one
pay-per-event charge (event: "page-extracted"). Failed or refused
URLs are logged and skipped — never charged, never faked.

Extraction core: src/extractor.py, ported from StallRow's
/v1/page-to-markdown (trafilatura + hardened, SSRF-checked fetching).
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import httpx
from apify import Actor

from .extractor import UpstreamError, extract_page

CHARGE_EVENT = "page-extracted"


def _scrub_no_proxy() -> None:
    """Drop bracketed IPv6 entries ([::1]) from no_proxy/NO_PROXY:
    httpx's proxy parser raises InvalidURL on them when a client is
    constructed. (Same fix the fleet services shipped after hitting
    this in the wild; harmless where the entries are absent.)"""
    for key in ("no_proxy", "NO_PROXY"):
        value = os.environ.get(key)
        if value:
            os.environ[key] = ",".join(
                part for part in value.split(",") if "[" not in part
            )


def _urls_from_input(actor_input: dict) -> list[str]:
    """Accept startUrls in the standard Apify shapes: a list of
    {"url": ...} objects (requestListSources editor) or plain
    strings; plus an optional plain `urls` string list."""
    urls: list[str] = []
    for entry in actor_input.get("startUrls") or []:
        if isinstance(entry, dict) and entry.get("url"):
            urls.append(str(entry["url"]))
        elif isinstance(entry, str) and entry.strip():
            urls.append(entry.strip())
    for entry in actor_input.get("urls") or []:
        if isinstance(entry, str) and entry.strip():
            urls.append(entry.strip())
    return urls


async def main() -> None:
    _scrub_no_proxy()
    async with Actor:
        actor_input = await Actor.get_input() or {}

        urls = _urls_from_input(actor_input)
        max_pages = int(actor_input.get("maxPages") or 10)
        output_format = str(actor_input.get("outputFormat") or "markdown").lower()
        if output_format not in ("markdown", "text"):
            await Actor.fail(
                status_message=f"outputFormat must be 'markdown' or 'text', got {output_format!r}"
            )
            return
        if not urls:
            await Actor.fail(
                status_message="No URLs given: fill in startUrls (or urls) in the input."
            )
            return

        todo = urls[:max_pages]
        if len(urls) > len(todo):
            Actor.log.info(
                f"Input had {len(urls)} URLs; processing the first {len(todo)} "
                f"(maxPages={max_pages})."
            )

        extracted = 0
        failed = 0
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0)
        ) as client:
            for url in todo:
                try:
                    result = await extract_page(client, url, output_format)
                except ValueError as e:
                    # SSRF refusal / bad URL — a caller fault, logged
                    # plainly; never charged.
                    failed += 1
                    Actor.log.warning(f"Refused {url}: {e}")
                    continue
                except UpstreamError as e:
                    failed += 1
                    Actor.log.warning(f"Failed {url}: {e}")
                    continue

                item = {
                    "url": result["url"],
                    "finalUrl": result["final_url"],
                    "title": result["title"],
                    "format": output_format,
                    "wordCount": result["word_count"],
                    "fetchedAt": datetime.now(timezone.utc).isoformat(),
                }
                if output_format == "markdown":
                    item["markdown"] = result["content"]
                else:
                    item["text"] = result["content"]
                if result.get("fallback"):
                    item["fallback"] = result["fallback"]

                await Actor.push_data(item)
                extracted += 1

                # Pay-per-event: one charge per extracted page, only
                # on success. Stop early if the user's spend limit is
                # reached (push_data would stop delivering anyway).
                charge = await Actor.charge(event_name=CHARGE_EVENT, count=1)
                if charge.event_charge_limit_reached:
                    Actor.log.warning(
                        "Charge limit reached; stopping. "
                        f"Extracted {extracted} page(s), {failed} failed/refused."
                    )
                    break

        Actor.log.info(
            f"Done. Extracted {extracted} page(s); {failed} failed or refused "
            "(failures are never charged)."
        )


if __name__ == "__main__":
    asyncio.run(main())
