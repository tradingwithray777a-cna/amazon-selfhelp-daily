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
# SETTINGS
# -----------------------------
BASE_SELF_HELP = "https://www.amazon.com/Best-Sellers-Kindle-Store-Self-Help/zgbs/digital-text/156563011"
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

# Throttling / rate limit handling (tune via GitHub Actions env if needed)
MIN_GAP_SECONDS = float(os.getenv("WSA_MIN_GAP_SECONDS", "10"))  # minimum gap between WSA calls
JITTER_SECONDS = float(os.getenv("WSA_JITTER_SECONDS", "3"))
MAX_TOTAL_WAIT_SECONDS = int(os.getenv("WSA_MAX_TOTAL_WAIT_SECONDS", "900"))  # total wait for 429 before giving up

# Category helper endpoint (free helper)
HELPER_CATEGORIES = "https://amazon-helpers.webscrapingapi.com/categories"


_last_call_ts = 0.0


def _sleep_gap():
    """Ensure we don't hammer the API."""
    global _last_call_ts
    now = time.time()
    gap = MIN_GAP_SECONDS - (now - _last_call_ts)
    if gap > 0:
        time.sleep(gap)
    if JITTER_SECONDS > 0:
        time.sleep(random.uniform(0, JITTER_SECONDS))
    _last_call_ts = time.time()


def wsa_fetch_html(url: str, render_js: int = 0) -> str:
    """
    Fetch a URL via WebScrapingAPI. Retries on 429 with backoff until MAX_TOTAL_WAIT_SECONDS reached.
    """
    if not WSA_API_KEY:
        raise RuntimeError("Missing WSA_API_KEY. Add it to GitHub Secrets (Settings → Secrets and variables → Actions).")

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
            # Backoff: 5, 10, 20, 40, 80... capped
            backoff = min(120, 5 * (2 ** min(attempt, 5))) + random.uniform(0.5, 2.5)
            waited = time.time() - start
            print(f"[WSA] 429 rate limit. Backing off {backoff:.1f}s (waited {int(waited)}s total)...")
            time.sleep(backoff)
            if waited + backoff > MAX_TOTAL_WAIT_SECONDS:
                raise RuntimeError(f"RUN FAILED EARLY: WSA kept rate-limiting (429) for too long. Total waited ~{int(waited + backoff)}s.")
            continue

        # Other errors
        try:
            r.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"WSA error HTTP {r.status_code} for {url}. {e}. Body sample: {r.text[:200]}") from e


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def score_match(query: str, title: str) -> float:
    """Simple token overlap score for choosing best category candidate."""
    q = set(re.findall(r"[a-z0-9]+", norm(query)))
    t = set(re.findall(r"[a-z0-9]+", norm(title)))
    if not q or not t:
        return 0.0
    return len(q & t) / len(q | t)


def resolve_subniche_bestseller_url(subniche: str) -> str | None:
    """
    Use helper endpoint to find the best candidate category link for this sub-niche.
    We prefer links that look like Kindle best sellers under digital-text.
    """
    q = f"{subniche} Self-Help Kindle best sellers"
    helper_url = f"{HELPER_CATEGORIES}?q={quote_plus(q)}&limit=25"

    r = requests.get(helper_url, timeout=60)
    r.raise_for_status()
    items = r.json() if isinstance(r.json(), list) else []

    best = None
    best_score = 0.0

    for it in items:
        link = (it.get("link") or "").strip()
        page_title = (it.get("page_title") or "").strip()
        cat_title = (it.get("category_title") or "").strip()

        combined = f"{page_title} {cat_title} {link}"

        # Prefer Kindle digital-text best seller pages
        looks_right = ("/zgbs/digital-text/" in link) or ("/gp/bestsellers/digital-text/" in link)
        if not looks_right:
            continue

        sc = score_match(subniche, combined)
        # Extra boost if mentions self-help
        if "self-help" in combined.lower() or "self help" in combined.lower():
            sc += 0.15

        if sc > best_score:
            best_score = sc
            best = link

    if not best:
        return None

    # Make absolute
    if best.startswith("/"):
        best = "https://www.amazon.com" + best

    return best


def extract_5th_asin(bestseller_html: str) -> str | None:
    """
    Extract ASIN of #5 on a bestseller list page.
    """
    soup = BeautifulSoup(bestseller_html, "lxml")

    # Common ordered list
    li_items = soup.select("ol#zg-ordered-list > li")
    if len(li_items) >= 5:
        block = str(li_items[4])
        m = re.search(r"/dp/([A-Z0-9]{10})", block)
        if m:
            return m.group(1)

    # Fallback: first 5 unique /dp/ASIN links
    asins = []
    for m in re.finditer(r"/dp/([A-Z0-9]{10})", bestseller_html):
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
    Extract the first number after 'Best Sellers Rank' / 'Amazon Best Sellers Rank'.
    This should work even if the section is visually collapsed.
    """
    soup = BeautifulSoup(product_html, "lxml")
    text = soup.get_text("\n", strip=True)

    idx = text.lower().find("best sellers rank")
    if idx == -1:
        idx = text.lower().find("amazon best sellers rank")
    if idx == -1:
        return None

    window = text[idx: idx + 4000]
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

    # IMPORTANT:
    # We only use render_js=1 for PRODUCT pages (to help with “See all details” / hidden BSR)
    # List pages are fetched with render_js=0 (faster/cheaper)
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
            sub_url = resolve_subniche_bestseller_url(sub)
            if not sub_url:
                row["Notes"] = "Sub-niche URL not found via helper lookup"
                all_rows.append(row)
                continue

            best_html = wsa_fetch_html(sub_url, render_js=0)
            asin = extract_5th_asin(best_html)
            if not asin:
                row["Notes"] = "Could not extract #5 ASIN from sub-niche best-seller page"
                all_rows.append(row)
                continue

            product_url = f"https://www.amazon.com/dp/{asin}"
            row["ASIN"] = asin
            row["ProductURL"] = product_url

            prod_html = wsa_fetch_html(product_url, render_js=1)  # render_js=1 helps capture BSR section
            title, author = extract_title_author(prod_html)
            bsr = extract_bsr(prod_html)

            row["Title"] = title
            row["Author"] = author
            row["OverallBestSellersRank"] = bsr if bsr is not None else ""
            row["TopicKeywords"] = topic_keywords(title)
            row["Shortlisted(<20000)"] = "Y" if isinstance(bsr, int) and bsr < BSR_THRESHOLD else "N"

            if bsr is None:
                row["Notes"] = "BSR not found on product page (may require different layout)"
            elif bsr < BSR_THRESHOLD:
                shortlist_rows.append(row.copy())

            all_rows.append(row)

        except Exception as e:
            row["Notes"] = f"Error: {e}"
            all_rows.append(row)

    os.makedirs(OUT_DIR, exist_ok=True)
    write_csv(f"{OUT_DIR}/{today}_all.csv", all_rows, headers)
    write_csv(f"{OUT_DIR}/{today}_shortlist.csv", shortlist_rows, headers)

    # Make it obvious if the whole run returned zero titles (likely blocked)
    titles_count = sum(1 for r in all_rows if (r.get("Title") or "").strip())
    print(f"Done. Titles captured: {titles_count}. Shortlisted: {len(shortlist_rows)}.")
    if titles_count == 0:
        # exit non-zero so you notice it
        raise RuntimeError("No titles captured across all sub-niches. Likely layout mismatch or blocked HTML.")


if __name__ == "__main__":
    main()
