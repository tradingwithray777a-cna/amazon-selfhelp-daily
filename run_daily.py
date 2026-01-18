import os
import re
import csv
import time
import random
import datetime
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

# -----------------------------
# CONFIG
# -----------------------------
BSR_THRESHOLD = 20000
OUT_DIR = "output"

SUB_NICHES = [
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

# WebScrapingAPI
WSA_API_KEY = os.getenv("WSA_API_KEY", "").strip()
WSA_ENDPOINT = "https://api.webscrapingapi.com/v2"

# Rate-limit controls (tune in GitHub Actions env)
MIN_GAP_SECONDS = float(os.getenv("WSA_MIN_GAP_SECONDS", "12"))
JITTER_SECONDS = float(os.getenv("WSA_JITTER_SECONDS", "3"))
MAX_TOTAL_WAIT_SECONDS = int(os.getenv("WSA_MAX_TOTAL_WAIT_SECONDS", "900"))

# Helper endpoint to find category_id
HELPER_CATEGORIES = "https://amazon-helpers.webscrapingapi.com/categories"

_last_call_ts = 0.0


def _sleep_gap():
    global _last_call_ts
    now = time.time()
    gap = MIN_GAP_SECONDS - (now - _last_call_ts)
    if gap > 0:
        time.sleep(gap)
    if JITTER_SECONDS > 0:
        time.sleep(random.uniform(0, JITTER_SECONDS))
    _last_call_ts = time.time()


def looks_blocked(html: str) -> bool:
    if not html:
        return True
    t = html.lower()
    # Common Amazon / anti-bot patterns
    needles = [
        "robot check",
        "enter the characters you see below",
        "type the characters you see in this image",
        "captcha",
        "sorry, we just need to make sure you're not a robot",
    ]
    return any(n in t for n in needles)


def wsa_fetch_html(url: str, render_js: int = 0) -> str:
    """
    Fetch URL via WebScrapingAPI. Retries on 429 until MAX_TOTAL_WAIT_SECONDS.
    """
    if not WSA_API_KEY:
        raise RuntimeError("Missing WSA_API_KEY secret (GitHub Settings → Secrets and variables → Actions).")

    start = time.time()
    attempt = 0

    while True:
        _sleep_gap()
        api_url = (
            f"{WSA_ENDPOINT}?api_key={quote_plus(WSA_API_KEY)}"
            f"&url={quote_plus(url)}&render_js={render_js}"
        )
        r = requests.get(api_url, timeout=120)

        if r.status_code == 200:
            return r.text

        if r.status_code == 429:
            attempt += 1
            backoff = min(120, 5 * (2 ** min(attempt, 5))) + random.uniform(0.5, 2.5)
            waited = time.time() - start
            print(f"[WSA] 429 rate limit. Backoff {backoff:.1f}s (waited {int(waited)}s total)")
            time.sleep(backoff)
            if waited + backoff > MAX_TOTAL_WAIT_SECONDS:
                raise RuntimeError(f"WSA kept rate-limiting (429) for too long. Total waited ~{int(waited + backoff)}s.")
            continue

        # Other errors
        try:
            r.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"WSA error HTTP {r.status_code} for {url}. Body sample: {r.text[:200]}") from e


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def score_match(query: str, text: str) -> float:
    q = set(re.findall(r"[a-z0-9]+", norm(query)))
    t = set(re.findall(r"[a-z0-9]+", norm(text)))
    if not q or not t:
        return 0.0
    return len(q & t) / len(q | t)


def resolve_category_id(subniche: str) -> str | None:
    """
    Uses helper to find a category_id. We then construct the best-seller URL ourselves.
    """
    q = f"{subniche} self help kindle"
    helper_url = f"{HELPER_CATEGORIES}?q={quote_plus(q)}&limit=25"

    r = requests.get(helper_url, timeout=60)
    r.raise_for_status()
    items = r.json() if isinstance(r.json(), list) else []
    if not items:
        return None

    best_id = None
    best_score = 0.0

    for it in items:
        cid = it.get("category_id")
        link = (it.get("link") or "")
        page_title = (it.get("page_title") or "")
        cat_title = (it.get("category_title") or "")
        combined = f"{page_title} {cat_title} {link}"

        # Prefer anything that hints Kindle/digital-text
        bonus = 0.0
        low = combined.lower()
        if "kindle" in low or "digital-text" in low:
            bonus += 0.2
        if "self-help" in low or "self help" in low:
            bonus += 0.15

        sc = score_match(subniche, combined) + bonus

        if cid and sc > best_score:
            best_score = sc
            best_id = str(cid)

    return best_id


def build_bestseller_url(category_id: str) -> str:
    # Construct Kindle best seller page for that node
    return f"https://www.amazon.com/Best-Sellers-Kindle-Store/zgbs/digital-text/{category_id}"


def extract_5th_asin(list_html: str) -> str | None:
    """
    Extract #5 ASIN from a best-seller list page.
    Works across multiple layouts by using unique /dp/ ASIN extraction.
    """
    # First try ordered list
    soup = BeautifulSoup(list_html, "lxml")
    li_items = soup.select("ol#zg-ordered-list > li")
    if len(li_items) >= 5:
        block = str(li_items[4])
        m = re.search(r"/dp/([A-Z0-9]{10})", block)
        if m:
            return m.group(1)

    # Fallback: 5th unique /dp/ASIN on the page
    asins = []
    for m in re.finditer(r"/dp/([A-Z0-9]{10})", list_html):
        a = m.group(1)
        if a not in asins:
            asins.append(a)
    return asins[4] if len(asins) >= 5 else None


def extract_title_author(product_html: str) -> tuple[str, str]:
    soup = BeautifulSoup(product_html, "lxml")

    title = ""
    t = soup.select_one("#productTitle")
    if t:
        title = t.get_text(" ", strip=True)

    author = ""
    by = soup.select_one("#bylineInfo")
    if by:
        author = by.get_text(" ", strip=True)

    return title, author


def extract_bsr(product_html: str) -> int | None:
    """
    Extract the first numeric Best Sellers Rank from full page text.
    Using JS-rendered HTML helps when it’s behind “See all details”.
    """
    soup = BeautifulSoup(product_html, "lxml")
    text = soup.get_text("\n", strip=True)

    idx = text.lower().find("best sellers rank")
    if idx == -1:
        idx = text.lower().find("amazon best sellers rank")
    if idx == -1:
        return None

    window = text[idx: idx + 5000]
    m = re.search(r"#\s*([\d,]+)", window)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def topic_keywords(title: str) -> str:
    stop = {
        "the", "and", "for", "with", "your", "you", "how", "to", "a", "an", "of", "in", "on", "at", "from",
        "book", "guide", "workbook", "journal", "edition", "revised", "ultimate", "complete"
    }
    words = re.findall(r"[A-Za-z']{3,}", (title or "").lower())
    out, seen = [], set()
    for w in words:
        if w in stop or w in seen:
            continue
        seen.add(w)
        out.append(w)
    return ", ".join(out[:6])


def write_csv(path: str, rows: list[dict], headers: list[str]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)


def main():
    today = datetime.date.today().isoformat()

    headers = [
        "Date", "SubNiche", "SubNicheRank", "Title", "Author", "ASIN",
        "OverallBestSellersRank", "Shortlisted(<20000)", "TopicKeywords", "ProductURL", "Notes"
    ]

    all_rows = []
    shortlist_rows = []

    for sub in SUB_NICHES:
        row = {
            "Date": today,
            "SubNiche": sub,
            "SubNicheRank": 5,
            "Title": "",
            "Author": "",
            "ASIN": "",
            "OverallBestSellersRank": "",
            "Shortlisted(<20000)": "N",
            "TopicKeywords": "",
            "ProductURL": "",
            "Notes": "",
        }

        try:
            cid = resolve_category_id(sub)
            if not cid:
                row["Notes"] = "Category ID not found via helper"
                all_rows.append(row)
                continue

            list_url = build_bestseller_url(cid)

            # Fetch list page (try no-JS first, then JS if layout missing)
            list_html = wsa_fetch_html(list_url, render_js=0)
            if looks_blocked(list_html) or ("/dp/" not in list_html and "zg-ordered-list" not in list_html):
                list_html = wsa_fetch_html(list_url, render_js=1)

            if looks_blocked(list_html):
                row["Notes"] = "Blocked/Captcha page received for list page"
                all_rows.append(row)
                continue

            asin = extract_5th_asin(list_html)
            if not asin:
                row["Notes"] = "Could not extract #5 ASIN from list page"
                all_rows.append(row)
                continue

            product_url = f"https://www.amazon.com/dp/{asin}"
            row["ASIN"] = asin
            row["ProductURL"] = product_url

            # Product page: use JS rendering (helps “See all details” / hidden BSR)
            prod_html = wsa_fetch_html(product_url, render_js=1)

            if looks_blocked(prod_html):
                row["Notes"] = "Blocked/Captcha page received for product page"
                all_rows.append(row)
                continue

            title, author = extract_title_author(prod_html)
            bsr = extract_bsr(prod_html)

            row["Title"] = title
            row["Author"] = author
            row["OverallBestSellersRank"] = bsr if bsr is not None else ""
            row["TopicKeywords"] = topic_keywords(title)

            if not title:
                row["Notes"] = "Title not found on product page (layout mismatch)"
            if bsr is None:
                row["Notes"] = (row["Notes"] + " | " if row["Notes"] else "") + "BSR not found (may still be hidden)"

            if isinstance(bsr, int) and bsr < BSR_THRESHOLD:
                row["Shortlisted(<20000)"] = "Y"
                shortlist_rows.append(row.copy())

            all_rows.append(row)

        except Exception as e:
            row["Notes"] = f"Error: {e}"
            all_rows.append(row)

    os.makedirs(OUT_DIR, exist_ok=True)
    write_csv(f"{OUT_DIR}/{today}_all.csv", all_rows, headers)
    write_csv(f"{OUT_DIR}/{today}_shortlist.csv", shortlist_rows, headers)

    titles_count = sum(1 for r in all_rows if (r.get("Title") or "").strip())
    print(f"Done. Titles captured: {titles_count}. Shortlisted: {len(shortlist_rows)}.")

    # If nothing captured at all, fail the run so you notice.
    if titles_count == 0:
        raise RuntimeError("No titles captured across all sub-niches. Likely blocked HTML or wrong category IDs.")


if __name__ == "__main__":
    main()
