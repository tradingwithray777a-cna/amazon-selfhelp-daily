import csv
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup

# =========================
# SETTINGS
# =========================
BASE_URL = "https://www.amazon.com/Best-Sellers-Kindle-Store-Self-Help/zgbs/digital-text/156563011"

SUBNICHES = [
    "Abuse",
    "Affirmations",
    "Aging",
    "Anger Management",
    "Anxieties & Phobias",
    "Communication & Social Skills",
    "Compulsive Behavior",
    "Creativity",
    "Eating Disorders & Body Image",
    "Emotions",
    "Fashion & Style",
    "Green Lifestyle",
    "Happiness",
    "Indigenous Mental Health & Healing",
    "Inner Child",
    "Journal Writing",
    "Journaling",
    "Memory Improvement",
    "Motivational",
    "Neuro-Linguistic Programming (NLP)",
    "Personal Transformation",
    "Self-Esteem",
    "Self-Hypnosis",
    "Self-Management",
    "Sexual Instruction",
    "Spiritual",
    "Stress Management",
    "Success",
    "Time Management",
]

# If BSR (Amazon Best Sellers Rank) number is BELOW this, we shortlist
BSR_THRESHOLD = 20000

# =========================
# WSA (WebScrapingAPI) CONFIG
# =========================
WSA_ENDPOINT = "https://api.webscrapingapi.com/v2"

WSA_API_KEY = os.getenv("WSA_API_KEY", "").strip()

# Politeness controls (tune via GitHub Actions env)
MIN_GAP_SECONDS = float(os.getenv("WSA_MIN_GAP_SECONDS", "20"))  # gap between successful calls
JITTER_SECONDS = float(os.getenv("WSA_JITTER_SECONDS", "5"))     # random extra wait
MAX_RETRIES = int(os.getenv("WSA_MAX_RETRIES", "10"))
MAX_TOTAL_WAIT_SECONDS = int(os.getenv("WSA_MAX_TOTAL_WAIT_SECONDS", "900"))

# Force JS rendering OFF (cheaper / less heavy). Default is off anyway, but be explicit.
RENDER_JS = os.getenv("WSA_RENDER_JS", "0")

_last_call_ts = 0.0


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", "and")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s()\-]", "", s)
    return s


def _sleep_polite():
    global _last_call_ts
    now = time.time()
    gap = MIN_GAP_SECONDS - (now - _last_call_ts)
    if gap > 0:
        time.sleep(gap)
    if JITTER_SECONDS > 0:
        time.sleep(random.uniform(0, JITTER_SECONDS))


def wsa_fetch_html(target_url: str) -> str:
    """
    Fetch HTML via WebScrapingAPI with strong backoff on 429.
    If 429 persists too long, raises RuntimeError.
    """
    global _last_call_ts

    params = {
        "api_key": WSA_API_KEY,
        "url": target_url,
        "render_js": RENDER_JS,  # docs: render_js 0/1 :contentReference[oaicite:1]{index=1} (for you; not used in UI)
    }

    total_waited = 0.0
    attempt = 0

    while attempt <= MAX_RETRIES:
        _sleep_polite()

        api_url = f"{WSA_ENDPOINT}?api_key={quote(WSA_API_KEY)}&url={quote(target_url)}&render_js={quote(str(RENDER_JS))}"
        try:
            r = requests.get(api_url, timeout=120)
        except Exception as e:
            attempt += 1
            backoff = min(60, 2 ** attempt) + random.uniform(0, 1.5)
            total_waited += backoff
            if total_waited > MAX_TOTAL_WAIT_SECONDS:
                raise RuntimeError(f"Network errors kept happening. Total waited ~{int(total_waited)}s. Last error: {e}")
            print(f"[WSA] Network error. Waiting {backoff:.1f}s then retrying...")
            time.sleep(backoff)
            continue

        _last_call_ts = time.time()

        if r.status_code == 200:
            return r.text

        if r.status_code == 429:
            attempt += 1

            retry_after = r.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                backoff = float(retry_after)
            else:
                # Exponential backoff with cap
                backoff = min(180, (2 ** attempt)) + random.uniform(0, 2.5)

            total_waited += backoff
            if total_waited > MAX_TOTAL_WAIT_SECONDS:
                raise RuntimeError(
                    f"RUN FAILED EARLY: WSA kept rate-limiting (429) for too long. Total waited ~{int(total_waited)}s."
                )

            print(f"[WSA] 429 Too Many Requests. Waiting {backoff:.1f}s then retrying...")
            time.sleep(backoff)
            continue

        # Other errors
        try:
            r.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"WSA request failed: HTTP {r.status_code}. {e}") from e

    raise RuntimeError("WSA kept returning 429 too many times. Reduce re-runs and slow down request rate.")


def parse_subniche_links(base_html: str) -> dict:
    """
    Extract sub-niche links from the base best-sellers page.
    Returns dict: normalized_subniche_name -> absolute_url
    """
    soup = BeautifulSoup(base_html, "lxml")
    links = {}

    # Amazon best-sellers pages typically have lots of <a> tags; we keep likely subcategory links.
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)

        if not text or not href:
            continue

        # Heuristic: subcategory links usually remain within /zgbs/digital-text/ and are not the same page
        if "/zgbs/digital-text/" not in href:
            continue

        # Make absolute URL
        abs_url = urljoin("https://www.amazon.com", href)

        # Avoid picking the base page itself as a "subcategory"
        if abs_url.rstrip("/") == BASE_URL.rstrip("/"):
            continue

        # Keep only links that look like within Self-Help Kindle Store best-sellers
        # (This is a heuristic; Amazon changes markup often.)
        ntext = _norm(text)
        if len(ntext) < 3:
            continue

        links[ntext] = abs_url

    return links


def extract_ranked_items(bestseller_html: str):
    """
    Return a list of dict items with at least: rank (int), asin (str|None), title (str), product_url (str|None)
    """
    soup = BeautifulSoup(bestseller_html, "lxml")

    items = []
    # Common patterns: elements with data-asin
    for el in soup.select("[data-asin]"):
        asin = (el.get("data-asin") or "").strip()
        if not asin:
            continue

        # Find rank text nearby like "#1"
        rank = None
        rank_el = el.select_one(".zg-bdg-text")
        if rank_el:
            m = re.search(r"#\s*(\d+)", rank_el.get_text(strip=True))
            if m:
                rank = int(m.group(1))

        # Title: sometimes in img alt or a link title
        title = ""
        img = el.select_one("img[alt]")
        if img and img.get("alt"):
            title = img.get("alt", "").strip()

        if not title:
            a = el.select_one("a.a-link-normal[href]")
            if a:
                title = a.get_text(" ", strip=True)

        # Product URL: prefer /dp/ASIN
        product_url = None
        dp = f"https://www.amazon.com/dp/{asin}"
        product_url = dp

        if rank is not None:
            items.append({"rank": rank, "asin": asin, "title": title, "product_url": product_url})

    # Deduplicate by rank (keep first)
    by_rank = {}
    for it in items:
        r = it["rank"]
        if r not in by_rank:
            by_rank[r] = it

    ordered = [by_rank[r] for r in sorted(by_rank.keys())]
    return ordered


def extract_bsr_number(product_html: str):
    """
    Extract the first numeric "Amazon Best Sellers Rank" value from product page.
    Returns int or None.
    """
    text = BeautifulSoup(product_html, "lxml").get_text("\n", strip=True)

    # Find chunk around "Amazon Best Sellers Rank"
    idx = text.lower().find("amazon best sellers rank")
    if idx == -1:
        return None

    window = text[idx: idx + 1200]
    m = re.search(r"#\s*([\d,]{1,20})", window)
    if not m:
        return None

    num = m.group(1).replace(",", "")
    if num.isdigit():
        return int(num)
    return None


def infer_topic(title: str) -> str:
    """
    Simple, readable 'topic' based on title/subtitle keywords.
    """
    t = (title or "").strip()
    if not t:
        return ""

    # Prefer subtitle after colon if present
    if ":" in t:
        after = t.split(":", 1)[1].strip()
        if after:
            return after[:120]

    tlow = t.lower()

    keywords = [
        ("anxiety", "Anxiety relief"),
        ("phobia", "Phobias"),
        ("anger", "Anger management"),
        ("stress", "Stress management"),
        ("habit", "Habits & behavior change"),
        ("nlp", "NLP"),
        ("confidence", "Confidence & self-esteem"),
        ("self-esteem", "Self-esteem"),
        ("trauma", "Trauma healing"),
        ("inner child", "Inner child healing"),
        ("journ", "Journaling / prompts)
