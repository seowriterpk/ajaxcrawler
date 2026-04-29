from __future__ import annotations

import asyncio
import html
import inspect
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Iterable
from urllib.parse import unquote, urljoin, urlparse, urlunparse

import httpx
from bs4 import BeautifulSoup


WHATSAPP_RE = re.compile(
    r"""(?ix)
    https?://(?:
        chat\.whatsapp\.com/(?:invite/)?[A-Za-z0-9_-]{8,} |
        whatsapp\.com/channel/[A-Za-z0-9_-]{8,} |
        wa\.me/[0-9]{7,15}
    )
    """
)


# Public directory pages often hide the final WhatsApp URL behind internal
# /group/invite/<code>, /group/join/<code>, or /group/rules/<code> paths.
# On Groupsor-like sites, that code is the public invite code used on the final
# WhatsApp URL. This gives the crawler a deterministic public-page extraction
# path even when the site loads cards through delayed AJAX/lazy scroll.
DIRECTORY_INTERNAL_GROUP_RE = re.compile(
    r"""(?ix)
    https?://(?P<host>[^\s"'<>]+?)/group/(?P<kind>invite|join|rules)/(?P<code>[A-Za-z0-9_-]{8,})
    |
    (?P<rel>/group/(?P<kind2>invite|join|rules)/(?P<code2>[A-Za-z0-9_-]{8,}))
    """
)

GROUPSOR_HOST_HINTS = {
    "groupsor.link",
    "www.groupsor.link",
}


GROUPSOR_INTERNAL_URL_RE = re.compile(
    r"""(?ix)
    (?:
        https?://(?:www\.)?groupsor\.link
    )?
    /group/(?P<kind>join|invite|rules)/(?P<code>[A-Za-z0-9_-]{8,})
    """
)

GOOD_CLICK_WORDS = {
    "join", "join group", "join now", "join whatsapp", "join group now",
    "i agree", "agree", "continue", "proceed", "rules", "invite",
    "open group", "visit group", "whatsapp", "group",

    # Dynamic / AJAX / popup style buttons
    "load more", "show more", "see more", "more groups", "next", "next page",
    "view", "view group", "show group", "show whatsapp", "show link",
    "get link", "get invite", "open link", "click here", "click to join",
    "continue reading", "read more", "see whatsapp group", "join popup",
    "find group", "search", "submit"
}

BAD_CLICK_WORDS = {
    "report", "add group", "submit group", "privacy", "terms",
    "contact", "login", "register", "advertise", "facebook",
    "instagram", "telegram", "youtube", "policy", "dmca",
    "share", "share on", "tweet", "pinterest", "linkedin"
}

BAD_HREF_PARTS = {
    "pagead2.googlesyndication.com",
    "doubleclick.net",
    "googleads",
    "/report",
    "/addgroup",
    "mailto:",
    "tel:",
    "javascript:void",
    "#",
}


@dataclass
class FoundLink:
    invite_url: str
    normalized_url: str
    source_page: str
    source_domain: str
    source_query: str
    discovered_at: str
    extraction_method: str
    click_text: str = ""
    raw_url: str = ""


@dataclass
class Candidate:
    url: str
    text: str
    score: int


@dataclass
class PageTask:
    url: str
    depth: int = 0
    source_query: str = ""
    parent_url: str = ""
    method_hint: str = "seed"


@dataclass
class CrawlStats:
    queued: int = 0
    running: int = 0
    visited: int = 0
    failed: int = 0

    # raw_found = every discovered WhatsApp hit event.
    # unique_found = unique normalized invite URLs inside the current crawl.
    raw_found: int = 0
    unique_found: int = 0
    found: int = 0  # kept for old UI compatibility; mirrors unique_found.

    duplicates: int = 0
    browser_rendered: int = 0
    candidates_added: int = 0
    current_url: str = ""
    status: str = "idle"
    elapsed: float = 0.0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", html.unescape(value)).strip().lower()


def normalize_page_url(raw: str | None, base: str | None = None) -> str | None:
    if not raw:
        return None

    raw = html.unescape(unquote(str(raw).strip().strip('"').strip("'")))

    if base:
        raw = urljoin(base, raw)

    parsed = urlparse(raw)

    if parsed.scheme not in {"http", "https"}:
        return None

    netloc = parsed.netloc.lower()
    path = parsed.path or "/"

    return urlunparse((parsed.scheme, netloc, path, "", parsed.query, ""))


def normalize_whatsapp_url(raw: str) -> str | None:
    raw = html.unescape(unquote(str(raw).strip().strip('"').strip("'")))
    parsed = urlparse(raw)
    host = parsed.netloc.lower()
    path = parsed.path.strip("/")

    if host == "chat.whatsapp.com":
        parts = path.split("/")
        code = parts[-1] if parts else ""
        if len(code) >= 8:
            return f"https://chat.whatsapp.com/{code}"

    if host == "whatsapp.com" and path.startswith("channel/"):
        return f"https://whatsapp.com/{path}"

    if host == "wa.me":
        phone = path.split("/")[0]
        if phone.isdigit():
            return f"https://wa.me/{phone}"

    return None


def source_domain(url: str) -> str:
    return urlparse(url).netloc.lower()


def same_domain(a: str, b: str) -> bool:
    return source_domain(a) == source_domain(b)


def extract_whatsapp_links(text: str) -> list[str]:
    found: list[str] = []

    for match in WHATSAPP_RE.findall(html.unescape(text or "")):
        normalized = normalize_whatsapp_url(match)
        if normalized:
            found.append(normalized)

    return list(dict.fromkeys(found))




def extract_directory_internal_group_links(text: str, page_url: str) -> list[dict[str, str]]:
    """
    JS-console equivalent for Groupsor-style pages.

    It searches ANY visible/rendered HTML or AJAX body for:
      /group/join/CODE
      /group/invite/CODE
      /group/rules/CODE

    On groupsor.link, the public CODE maps to:
      https://chat.whatsapp.com/invite/CODE

    This is exactly what the browser-console test confirmed by downloading 1200 rows.
    """
    found: list[dict[str, str]] = []
    base_host = source_domain(page_url)

    if base_host not in GROUPSOR_HOST_HINTS and "groupsor.link" not in (text or ""):
        return []

    for match in GROUPSOR_INTERNAL_URL_RE.finditer(html.unescape(text or "")):
        kind = match.group("kind") or "join"
        code = match.group("code") or ""

        if not code:
            continue

        internal_url = f"https://groupsor.link/group/{kind}/{code}"
        whatsapp_url = normalize_whatsapp_url(f"https://chat.whatsapp.com/invite/{code}")

        if not whatsapp_url:
            continue

        found.append({
            "whatsapp_url": whatsapp_url,
            "internal_url": internal_url,
            "code": code,
            "kind": kind,
        })

    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in found:
        key = row["whatsapp_url"]
        if key not in seen:
            out.append(row)
            seen.add(key)

    return out


def is_whatsapp_url(url: str) -> bool:
    return normalize_whatsapp_url(url) is not None


def is_bad_href(href: str) -> bool:
    low = (href or "").lower()
    return any(part in low for part in BAD_HREF_PARTS)


def click_score(text: str, href: str) -> int:
    t = clean_text(text)
    h = clean_text(href)
    combined = f"{t} {h}"

    if any(bad in combined for bad in BAD_CLICK_WORDS):
        return -10

    score = 0

    for good in GOOD_CLICK_WORDS:
        if good in combined:
            score += 3

    if "/group/rules/" in h:
        score += 10
    if "/group/invite/" in h:
        score += 10
    if "/group/join/" in h:
        score += 10
    if "chat.whatsapp.com" in h:
        score += 20
    if "button" in combined:
        score += 1

    return score


def is_allowed_flow_url(candidate_url: str, start_url: str, same_domain_only: bool = True) -> bool:
    c = normalize_page_url(candidate_url)
    if not c:
        return False

    if is_bad_href(c):
        return False

    if is_whatsapp_url(c):
        return True

    if same_domain_only and source_domain(c) != source_domain(start_url):
        return False

    return True


def extract_candidates(html_text: str, page_url: str, same_domain_only: bool = True, max_candidates: int = 30) -> list[Candidate]:
    soup = BeautifulSoup(html_text or "", "lxml")
    candidates: list[Candidate] = []

    for a in soup.select("a[href]"):
        href = normalize_page_url(a.get("href"), page_url)
        if not href:
            continue

        text = a.get_text(" ", strip=True) or a.get("title") or ""
        score = click_score(text, href)

        if score > 0 and is_allowed_flow_url(href, page_url, same_domain_only):
            candidates.append(Candidate(url=href, text=text, score=score))

    # Simple onclick redirects.
    for tag in soup.select("[onclick]"):
        onclick = tag.get("onclick", "") or ""
        text = tag.get_text(" ", strip=True) or tag.get("value") or "onclick"
        raw_urls = re.findall(
            r"""['"]([^'"]*(?:/group/rules/|/group/invite/|chat\.whatsapp\.com|wa\.me/)[^'"]*)['"]""",
            onclick,
            flags=re.I,
        )

        for raw in raw_urls:
            href = normalize_page_url(raw, page_url)
            if href and is_allowed_flow_url(href, page_url, same_domain_only):
                candidates.append(Candidate(url=href, text=text, score=click_score(text, href) + 3))

    best: dict[str, Candidate] = {}
    for c in candidates:
        if c.url not in best or c.score > best[c.url].score:
            best[c.url] = c

    return sorted(best.values(), key=lambda x: x.score, reverse=True)[:max_candidates]


def make_hit(
    invite_url: str,
    source_page: str,
    source_query: str,
    method: str,
    click_text: str = "",
    raw_url: str = "",
) -> FoundLink:
    normalized = normalize_whatsapp_url(invite_url) or invite_url

    return FoundLink(
        invite_url=normalized,
        normalized_url=normalized,
        source_page=source_page,
        source_domain=source_domain(source_page),
        source_query=source_query,
        discovered_at=utc_now(),
        extraction_method=method,
        click_text=click_text,
        raw_url=raw_url or invite_url,
    )


def parse_input_lines(text: str) -> list[str]:
    rows: list[str] = []
    for line in (text or "").replace(",", "\n").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(line)
    return rows


def normalize_seed(value: str) -> str | None:
    value = value.strip()
    if not value:
        return None
    if not value.startswith(("http://", "https://")):
        value = "https://" + value
    return normalize_page_url(value)


async def maybe_call(callback: Callable[..., Any] | None, event: dict[str, Any]) -> None:
    if not callback:
        return
    result = callback(event)
    if inspect.isawaitable(result):
        await result


async def http_fetch(client: httpx.AsyncClient, url: str, timeout: float) -> tuple[str, str]:
    resp = await client.get(
        url,
        timeout=timeout,
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 educational-link-checker/1.0",
            "Accept": "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.8",
        },
    )
    content_type = resp.headers.get("content-type", "")
    if "text" not in content_type and "html" not in content_type and "json" not in content_type:
        return str(resp.url), ""
    return str(resp.url), resp.text or ""


class BrowserPiercer:
    def __init__(self, settings: dict[str, Any], on_event: Callable[..., Any] | None = None):
        self.settings = settings
        self.on_event = on_event
        self.hits: dict[str, FoundLink] = {}

    def capture_url(self, url: str, source_page: str, source_query: str, method: str, click_text: str = "") -> None:
        normalized = normalize_whatsapp_url(url)
        if normalized and normalized not in self.hits:
            self.hits[normalized] = make_hit(
                invite_url=url,
                source_page=source_page,
                source_query=source_query,
                method=method,
                click_text=click_text,
                raw_url=url,
            )

    async def pierce(self, start_url: str, source_query: str = "") -> list[FoundLink]:
        try:
            from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
        except Exception as exc:
            await maybe_call(self.on_event, {"type": "log", "level": "ERROR", "message": f"Playwright import failed: {exc}"})
            return []

        browser = None
        context = None

        try:
            async with async_playwright() as pw:
                launch_args = {
                    "headless": True,
                    "args": [
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--disable-extensions",
                    ],
                }

                # Streamlit Cloud works better with apt chromium from packages.txt.
                system_paths = ["/usr/bin/chromium", "/usr/bin/chromium-browser", "/usr/bin/google-chrome"]
                for p in system_paths:
                    try:
                        import os
                        if os.path.exists(p):
                            launch_args["executable_path"] = p
                            break
                    except Exception:
                        pass

                browser = await pw.chromium.launch(**launch_args)
                context = await browser.new_context(
                    java_script_enabled=True,
                    ignore_https_errors=True,
                    viewport={"width": 1365, "height": 768},
                )

                async def route_handler(route):
                    req_url = route.request.url
                    if is_whatsapp_url(req_url):
                        self.capture_url(req_url, start_url, source_query, "browser_network_intercept")
                        await route.abort()
                        return
                    await route.continue_()

                await context.route("**/*", route_handler)

                async def inspect_response_body(response):
                    """
                    AJAX-heavy sites often return the group cards or final link in XHR/fetch JSON/HTML.
                    This reads small text-like responses and extracts WhatsApp links without needing
                    the link to appear in the original page HTML.
                    """
                    try:
                        resp_url = response.url
                        self.capture_url(resp_url, start_url, source_query, "browser_ajax_url")

                        headers = response.headers or {}
                        ctype = (headers.get("content-type") or "").lower()
                        if not any(x in ctype for x in ["text", "html", "json", "javascript", "xml"]):
                            return

                        body = await response.text()
                        if not body:
                            return

                        for link in extract_whatsapp_links(body + " " + resp_url):
                            self.capture_url(link, resp_url, source_query, "browser_ajax_body")

                        for row in extract_directory_internal_group_links(body + " " + resp_url, resp_url):
                            self.capture_url(
                                row["whatsapp_url"],
                                row["internal_url"],
                                source_query,
                                f"groupsor_browser_ajax_{row['kind']}",
                            )
                    except Exception:
                        return

                def attach_watchers(page):
                    page.on("request", lambda request: self.capture_url(request.url, page.url or start_url, source_query, "browser_request"))
                    page.on("response", lambda response: asyncio.create_task(inspect_response_body(response)))
                    page.on("framenavigated", lambda frame: self.capture_url(frame.url, page.url or start_url, source_query, "browser_frame_navigation"))

                context.on("page", attach_watchers)
                page = await context.new_page()
                attach_watchers(page)

                visited: set[str] = set()
                queue: list[Candidate] = [Candidate(start_url, "start", 100)]
                steps = int(self.settings.get("browser_steps", 8))
                timeout_ms = int(self.settings.get("browser_timeout_ms", 15000))
                same_domain_only = bool(self.settings.get("same_domain_only", True))

                for _ in range(steps):
                    if not queue:
                        break

                    candidate = queue.pop(0)
                    current = normalize_page_url(candidate.url)
                    if not current or current in visited:
                        continue

                    visited.add(current)

                    await maybe_call(self.on_event, {
                        "type": "status",
                        "status": "rendering",
                        "current_url": current,
                    })

                    try:
                        await page.goto(current, wait_until="domcontentloaded", timeout=timeout_ms)
                    except PlaywrightTimeoutError:
                        pass
                    except Exception as exc:
                        await maybe_call(self.on_event, {"type": "log", "level": "WARNING", "message": f"Browser goto failed: {current} | {exc}"})
                        continue

                    ajax_wait_ms = int(float(self.settings.get("ajax_wait_seconds", 8.0)) * 1000)
                    await page.wait_for_timeout(max(700, ajax_wait_ms))

                    # Trigger lazy-loaded content before and after clicks.
                    await self.auto_scroll_page(page)
                    await page.wait_for_timeout(1200)
                    await self.scan_pages(context, source_query, candidate.text)

                    html_text = await page.content()
                    for c in extract_candidates(html_text, page.url, same_domain_only=same_domain_only):
                        if c.url not in visited and is_allowed_flow_url(c.url, start_url, same_domain_only):
                            queue.append(c)

                    await self.click_relevant_controls(context, start_url, source_query)
                    await self.auto_scroll_page(page)
                    await self.scan_pages(context, source_query, "post_interaction")

                return list(self.hits.values())

        except Exception as exc:
            await maybe_call(self.on_event, {"type": "log", "level": "ERROR", "message": f"Browser fallback failed: {exc}"})
            return []

        finally:
            try:
                if context:
                    await context.close()
                if browser:
                    await browser.close()
            except Exception:
                pass

    async def auto_scroll_page(self, page) -> None:
        """
        Helps with delayed AJAX / infinite-scroll / lazy-loaded group lists.
        Some Groupsor-like pages do not inject cards until after a few seconds
        and a small scroll. This scrolls in controlled rounds and waits for XHR.
        """
        try:
            rounds = int(self.settings.get("scroll_rounds", 10) or 10)
            rounds = max(1, min(rounds, 60))

            for _ in range(rounds):
                await page.evaluate("window.scrollBy(0, Math.max(650, window.innerHeight || 850));")
                await page.wait_for_timeout(650)

            # Return near top and do one final scan-friendly wait.
            await page.evaluate("window.scrollTo(0, 0);")
            await page.wait_for_timeout(500)
        except Exception:
            return

    async def scan_pages(self, context, source_query: str, click_text: str) -> None:
        for page in list(context.pages):
            try:
                self.capture_url(page.url, page.url, source_query, "browser_current_url", click_text)
                content = await page.content()
                scan_blob = content + " " + page.url

                for link in extract_whatsapp_links(scan_blob):
                    self.capture_url(link, page.url, source_query, "browser_dom", click_text)

                for row in extract_directory_internal_group_links(scan_blob, page.url):
                    self.capture_url(
                        row["whatsapp_url"],
                        row["internal_url"],
                        source_query,
                        f"groupsor_browser_dom_{row['kind']}",
                        click_text,
                    )
            except Exception:
                continue

    async def click_relevant_controls(self, context, start_url: str, source_query: str) -> None:
        selector = "a, button, [role='button'], input[type='button'], input[type='submit']"
        same_domain_only = bool(self.settings.get("same_domain_only", True))

        for page in list(context.pages):
            try:
                loc = page.locator(selector)
                count = min(await loc.count(), 40)
                scored = []

                for i in range(count):
                    el = loc.nth(i)

                    try:
                        if not await el.is_visible(timeout=400):
                            continue

                        try:
                            text = await el.inner_text(timeout=400)
                        except Exception:
                            text = await el.get_attribute("value") or ""

                        href = await el.get_attribute("href") or ""
                        href_abs = normalize_page_url(href, page.url) if href else ""
                        score = click_score(text, href_abs or text)

                        if score <= 0:
                            continue

                        if href_abs and not is_allowed_flow_url(href_abs, start_url, same_domain_only):
                            continue

                        scored.append((score, text, el))
                    except Exception:
                        continue

                scored.sort(key=lambda x: x[0], reverse=True)

                for _score, text, el in scored[:12]:
                    try:
                        await el.scroll_into_view_if_needed(timeout=1000)
                        await el.click(timeout=2500)
                        await page.wait_for_timeout(1400)

                        # Some sites open a modal/popup or trigger AJAX after the first click.
                        # Scroll, wait, and rescan every page/modal state.
                        await self.auto_scroll_page(page)
                        await page.wait_for_timeout(900)
                        await self.scan_pages(context, source_query, text)

                        try:
                            await page.keyboard.press("Escape")
                            await page.wait_for_timeout(150)
                        except Exception:
                            pass
                    except Exception:
                        continue

            except Exception:
                continue


class CrawlEngine:
    def __init__(self, settings: dict[str, Any], on_event: Callable[..., Any] | None = None):
        self.settings = settings
        self.on_event = on_event
        self.queue: asyncio.Queue[PageTask] = asyncio.Queue()
        self.visited: set[str] = set()
        self.found: set[str] = set()
        self.domain_counts: dict[str, int] = {}
        self.stats = CrawlStats()
        self.start_time = time.time()
        self.stop_requested = False
        self.browser_sem = asyncio.Semaphore(int(settings.get("browser_concurrency", 1)))

    async def emit(self, event: dict[str, Any]) -> None:
        self.stats.elapsed = round(time.time() - self.start_time, 1)
        event.setdefault("stats", asdict(self.stats))
        await maybe_call(self.on_event, event)

    def can_visit(self, url: str) -> bool:
        if not url or url in self.visited:
            return False

        # 0 means unlimited at the app level.
        max_pages_total = int(self.settings.get("max_pages_total", 0) or 0)
        if max_pages_total > 0 and len(self.visited) >= max_pages_total:
            return False

        domain = source_domain(url)
        max_pages_per_domain = int(self.settings.get("max_pages_per_domain", 0) or 0)
        if max_pages_per_domain > 0 and self.domain_counts.get(domain, 0) >= max_pages_per_domain:
            return False

        return True

    async def add_task(self, task: PageTask) -> bool:
        normalized = normalize_page_url(task.url)
        if not normalized:
            return False

        task.url = normalized
        if not self.can_visit(task.url):
            return False

        await self.queue.put(task)
        self.stats.queued = self.queue.qsize()
        self.stats.candidates_added += 1
        return True

    async def run(self, seeds: Iterable[str]) -> dict[str, Any]:
        self.start_time = time.time()
        self.stats.status = "starting"

        unique_seeds = []
        for seed in seeds:
            normalized = normalize_seed(seed)
            if normalized and normalized not in unique_seeds:
                unique_seeds.append(normalized)

        for seed in unique_seeds:
            await self.add_task(PageTask(url=seed, depth=0, source_query=seed, method_hint="seed"))

        if not unique_seeds:
            await self.emit({"type": "done", "message": "No valid seed URLs provided."})
            return {"stats": asdict(self.stats), "results": []}

        await self.emit({"type": "log", "level": "INFO", "message": f"Started crawl with {len(unique_seeds)} seed URLs"})

        limits = httpx.Limits(max_connections=int(self.settings.get("http_concurrency", 12)) + 5)
        async with httpx.AsyncClient(limits=limits) as client:
            workers = [
                asyncio.create_task(self.worker(client, i))
                for i in range(int(self.settings.get("http_concurrency", 12)))
            ]

            await self.queue.join()

            for worker in workers:
                worker.cancel()

            await asyncio.gather(*workers, return_exceptions=True)

        self.stats.status = "completed"
        self.stats.queued = self.queue.qsize()
        await self.emit({"type": "done", "message": "Crawl completed.", "stats": asdict(self.stats)})
        return {"stats": asdict(self.stats)}

    async def worker(self, client: httpx.AsyncClient, worker_id: int) -> None:
        while True:
            task = await self.queue.get()
            try:
                await self.process_task(client, task, worker_id)
            finally:
                self.queue.task_done()
                self.stats.queued = self.queue.qsize()

    async def process_task(self, client: httpx.AsyncClient, task: PageTask, worker_id: int) -> None:
        if not self.can_visit(task.url):
            return

        self.visited.add(task.url)
        domain = source_domain(task.url)
        self.domain_counts[domain] = self.domain_counts.get(domain, 0) + 1

        self.stats.running += 1
        self.stats.visited = len(self.visited)
        self.stats.current_url = task.url
        self.stats.status = "fetching"
        await self.emit({"type": "status", "status": "fetching", "current_url": task.url})

        await asyncio.sleep(float(self.settings.get("request_delay", 0.15)))

        try:
            final_url, text = await http_fetch(client, task.url, float(self.settings.get("http_timeout", 12.0)))

            self.stats.current_url = final_url
            self.stats.status = "extracting"

            hits = [
                make_hit(link, final_url, task.source_query, "http_html")
                for link in extract_whatsapp_links(text + " " + final_url)
            ]

            # Groupsor-like fix: listing pages may expose only delayed/internal
            # /group/join|invite|rules/<code> links. For Groupsor, that public code
            # maps to the final WhatsApp invite code shown in the page flow.
            for row in extract_directory_internal_group_links(text + " " + final_url, final_url):
                hits.append(
                    make_hit(
                        row["whatsapp_url"],
                        final_url,
                        task.source_query,
                        f"groupsor_internal_{row['kind']}",
                        click_text=row["internal_url"],
                        raw_url=row["internal_url"],
                    )
                )

            self.stats.raw_found += len(hits)

            new_hits = []
            duplicate_hits = []
            for hit in hits:
                if hit.normalized_url in self.found:
                    self.stats.duplicates += 1
                    duplicate_hits.append(asdict(hit))
                    continue
                self.found.add(hit.normalized_url)
                self.stats.unique_found += 1
                self.stats.found = self.stats.unique_found
                new_hits.append(asdict(hit))

            # Emit both unique and duplicate raw hits so exports cannot silently shrink.
            emit_rows = new_hits + duplicate_hits
            if emit_rows:
                await self.emit({
                    "type": "results",
                    "results": emit_rows,
                    "unique_results": new_hits,
                    "raw_results": emit_rows,
                    "message": f"Discovered {len(emit_rows)} raw hit(s), {len(new_hits)} new unique.",
                })

            same_domain_only = bool(self.settings.get("same_domain_only", True))
            max_depth = int(self.settings.get("max_depth", 0) or 0)
            depth_allows_more = (max_depth == 0) or (task.depth < max_depth)

            if depth_allows_more:
                candidates = extract_candidates(text, final_url, same_domain_only=same_domain_only)

                for c in candidates:
                    if is_whatsapp_url(c.url):
                        hit = make_hit(c.url, final_url, task.source_query, "http_anchor_candidate", click_text=c.text)
                        self.stats.raw_found += 1
                        if hit.normalized_url not in self.found:
                            self.found.add(hit.normalized_url)
                            self.stats.unique_found += 1
                            self.stats.found = self.stats.unique_found
                            await self.emit({
                                "type": "results",
                                "results": [asdict(hit)],
                                "unique_results": [asdict(hit)],
                                "raw_results": [asdict(hit)],
                                "message": "Discovered 1 raw hit, 1 new unique.",
                            })
                        else:
                            self.stats.duplicates += 1
                            await self.emit({
                                "type": "results",
                                "results": [asdict(hit)],
                                "unique_results": [],
                                "raw_results": [asdict(hit)],
                                "message": "Discovered 1 raw duplicate hit.",
                            })
                        continue

                    if self.can_visit(c.url):
                        await self.add_task(PageTask(
                            url=c.url,
                            depth=task.depth + 1,
                            source_query=task.source_query,
                            parent_url=final_url,
                            method_hint=f"candidate:{c.text[:40]}",
                        ))

            # Browser fallback only for pages that did not reveal a link through HTTP.
            if (
                not hits
                and bool(self.settings.get("use_browser_fallback", True))
                and ((max_depth == 0) or task.depth <= max_depth)
            ):
                async with self.browser_sem:
                    self.stats.status = "rendering"
                    self.stats.browser_rendered += 1
                    await self.emit({"type": "status", "status": "rendering", "current_url": final_url})

                    piercer = BrowserPiercer(self.settings, on_event=self.on_event)
                    browser_hits = await piercer.pierce(final_url, task.source_query)

                    self.stats.raw_found += len(browser_hits)

                    new_browser_hits = []
                    duplicate_browser_hits = []
                    for hit in browser_hits:
                        if hit.normalized_url in self.found:
                            self.stats.duplicates += 1
                            duplicate_browser_hits.append(asdict(hit))
                            continue
                        self.found.add(hit.normalized_url)
                        self.stats.unique_found += 1
                        self.stats.found = self.stats.unique_found
                        new_browser_hits.append(asdict(hit))

                    emit_rows = new_browser_hits + duplicate_browser_hits
                    if emit_rows:
                        await self.emit({
                            "type": "results",
                            "results": emit_rows,
                            "unique_results": new_browser_hits,
                            "raw_results": emit_rows,
                            "message": f"Browser discovered {len(emit_rows)} raw hit(s), {len(new_browser_hits)} new unique.",
                        })

        except Exception as exc:
            self.stats.failed += 1
            await self.emit({
                "type": "log",
                "level": "ERROR",
                "message": f"Failed: {task.url} | {exc}",
                "url": task.url,
            })

        finally:
            self.stats.running = max(0, self.stats.running - 1)
            self.stats.visited = len(self.visited)
            self.stats.queued = self.queue.qsize()
            await self.emit({"type": "status", "status": "running", "current_url": self.stats.current_url})
