# Page Reader — Web Page to Markdown

Give it a list of public web pages. Get back each page's **title and main
content as clean Markdown** (or plain text) — navigation, ads, cookie
banners, and other boilerplate stripped out. One dataset item per page.

Built for feeding articles, docs, and blog posts into LLM pipelines, RAG
indexes, research workflows, and content monitoring — anywhere you need
the words on the page, not the page's chrome.

## What it does

- Extracts the main content of articles, documentation pages, and blog
  posts using trafilatura, the widely used open-source extraction engine.
- Follows redirects safely (every hop is checked), retries transient
  failures, and handles pages served as plain text or Markdown directly.
- Returns the page's real title, the final URL after redirects, and a
  word count for every page.

## Pricing

Pay per event: you are charged **once per page that extracts
successfully**. Pages that fail, are blocked, or are refused (see
Limits) are **never charged**.

## Input

| Field | Type | Description |
|---|---|---|
| `startUrls` | list of URLs | The pages to extract. |
| `urls` | list of strings (optional) | A plain URL list; combined with `startUrls`. |
| `maxPages` | integer (default 10) | Process at most this many URLs. |
| `outputFormat` | `markdown` \| `text` (default `markdown`) | Markdown keeps headings, lists, and links; text is the same content unformatted. |

### Example input

```json
{
  "startUrls": [
    { "url": "https://en.wikipedia.org/wiki/Web_scraping" },
    { "url": "https://docs.python.org/3/library/asyncio.html" }
  ],
  "maxPages": 10,
  "outputFormat": "markdown"
}
```

## Output

One dataset item per successfully extracted page:

```json
{
  "url": "https://en.wikipedia.org/wiki/Web_scraping",
  "finalUrl": "https://en.wikipedia.org/wiki/Web_scraping",
  "title": "Web scraping - Wikipedia",
  "format": "markdown",
  "markdown": "# Web scraping\n\nWeb scraping, web harvesting, or web data extraction is ...",
  "wordCount": 4213,
  "fetchedAt": "2026-11-17T12:00:00+00:00"
}
```

With `"outputFormat": "text"`, the content is in a `text` field instead
of `markdown`.

## Limits — the honest part

- **Public pages only.** The actor refuses URLs that point at private,
  loopback, or otherwise non-public addresses, and it cannot log in —
  paywalled or login-gated pages will fail (and are not charged).
- **No JavaScript rendering.** Content that only exists after a browser
  runs the page's scripts may extract partially or fail. Most articles,
  docs, and blogs are server-rendered and work fine.
- **Some sites block all automated fetching.** The actor retries with
  an identified bot user-agent, and as a last resort may deliver the
  page through a public reader proxy — such items are marked with
  `"fallback": "reader-proxy"` so you always know how your content
  was obtained.
- **Pages over 5 MB are refused.**
- Extraction quality depends on the page: clean article markup extracts
  beautifully; heavily script-driven or link-list pages extract as
  their visible text.
