import os
import re
import csv
import time
import random
import datetime
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

# =========================
# CONFIG
# =========================
BASE_URL = "https://www.amazon.com/Best-Sellers-Kindle-Store-Self-Help/zgbs/digital-text/156563011"
OUT_DIR = "output"
BSR_THRESHOLD = 20000

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

# Throttle (override via GitHub Actions env)
MIN_GAP_SECONDS = float(os.getenv("WSA_MIN_GAP_SECONDS", "15"))
JITTER_SECONDS = float(os.getenv("WSA_JITTER_SECONDS", "3"))
MAX_TOTAL_WAIT_SECONDS = int(os.getenv("WSA_MAX_TOTAL_WAIT_SECONDS", "1200"))

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
    needles = [
        "robot check",
        "enter the characters you see below",
        "type the characters you see in this image",
        "captcha",
        "sorry, we just need to make sure you're not a robot",
    ]
    return any(n in t for n in needles)


def wsa_fetch_html(url: str) -> str:
    """
    Fetch URL via WebScrapingAPI WITHOUT JS rendering.
    Retries on 429 until MAX_TOTAL_WAIT_SECONDS.
    """
    if not WSA_API_KEY:
        raise RuntimeError("Missing WSA_API_KEY secret (GitHub Settings → Secrets and variables → Actions).")

    start = time.time()
    attempt = 0

    while True:
        _sleep_gap()
        api_url = f"{WSA_ENDPOINT}?api_key={quote_plus(WSA_API_KEY)}&url={quote_plus(url)}&render_js=0"
        r = requests.get(api_url, timeout=120)

        if r.status_code == 200:
            return r.text

        if r.status_code == 429:
            attempt += 1
            backoff = min(180, 5 * (2 ** min(attempt, 6))) + random.uniform(0.5, 2.5)
            waited = time.time() - start
            print(f"[WSA] 429 rate limit. Backoff {backoff:.1f}s (waited {int(waited)}s total)")
            time.sleep(backoff)
            if waited + backoff > MAX_TOTAL_WAIT_SECONDS:
                raise RuntimeError(f"WSA kept rate-limiting (429) too long. Total waited ~{int(waited + backoff)}s.")
            continue

        try:
            r.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"WSA error HTTP {r.status_code} for {url}. Body sample: {r.text[:200]}") from e


def norm_key(s: str) -> str:
    # Normalize for matching: keep alnum only
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def token_set(s: str) -> set:
    return set(re.findall(r"[a-z0-9]+", (s or "").lower()))


def extract_subniche_links(base_html: str) -> dict:
    """
    Extract sub-niche links from the left navigation (browse tree) if present.
    Returns: normalized_label -> absolute_url
    """
    soup = BeautifulSoup(base_html, "lxml")
    root = soup.select_one("#zg_browseRoot")
    links = {}

    # If left nav exists, use it
    if root:
        for a in root.select("a[href]"):
            label = a.get_text(" ", strip=True)
            href = a.get("href", "").strip()
            if not label or not href:
                continue
            abs_url = urljoin("https://www.amazon.com", href)
            links[norm_key(label)] = abs_url

    return links


def match_subniche_url(sub: str, link_map: dict) -> str | None:
    """
    Exact match on normalized label; otherwise token overlap matching.
    """
    nk = norm_key(sub)
    if nk in link_map:
        return link_map[nk]

    target = token_set(sub)
    best_url, best_score = None, 0.0

    # compare against link_map keys (already normalized, so we use tokens from the raw sub name only)
    for k, url in link_map.items():
        # k is normalized; tokens from k are weak, but still workable
        k_tokens = set(re.findall(r"[a-z0-9]+", k))
        if not k_tokens:
            continue
        score = len(target & k_tokens) / max(1, len(target | k_tokens))
        if score > best_score:
            best_score = score
            best_url = url

    return best_url if best_score >= 0.25 else None


def extract_5th_asin(list_html: str) -> str | None:
    """
    Extract ASIN of the #5 item on a bestseller list page.
    Strategy: collect unique /dp/ASIN in order and take the 5th.
    """
    asins = []
    for m in re.finditer(r"/dp/([A-Z0-9]{10})", list_html):
        a = m.group(1)
        if a not in asins:
            asins.append(a)
        if len(asins) >= 5:
            break
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
    Even if the UI says 'See all details', the BSR text is often in the HTML.
    We search the full page text for the first '#12,345' after 'Best Sellers Rank'.
    """
    soup = BeautifulSoup(product_html, "lxml")
    text = soup.get_text("\n", strip=True)

    idx = text.lower().find("best sellers rank")
    if idx == -1:
        idx = text.lower().find("amazon best sellers rank")
    if idx == -1:
        return None

    window = text[idx: idx + 7000]
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
    run_iso = datetime.date.today().isoformat()
    run_display = datetime.date.today().strftime("%-d/%-m/%Y")  # Ubuntu supports %-d/%-m

    headers = [
        "Date", "SubNiche", "SubNicheRank", "Title", "Author", "ASIN",
        "OverallBestSellersRank", "Shortlisted(<20000)", "TopicKeywords", "ProductURL", "Notes"
    ]

    all_rows, shortlist_rows = [], []

    # 1) Fetch base page (NO JS)
    base_html = wsa_fetch_html(BASE_URL)
    if looks_blocked(base_html):
        raise RuntimeError("Blocked/Captcha on BASE page.")

    link_map = extract_subniche_links(base_html)
    if not link_map:
        raise RuntimeError("Base page missing #zg_browseRoot (left nav not found in returned HTML).")

    # 2) Process each sub-niche
    for i, sub in enumerate(SUB_NICHES, start=1):
        print(f"=== {i}/{len(SUB_NICHES)}: {sub} ===")

        row = {
            "Date": run_display,
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

        sub_url = match_subniche_url(sub, link_map)
        if not sub_url:
            row["Notes"] = "Sub-niche link not found on base page nav"
            all_rows.append(row)
            continue

        # list page
        list_html = wsa_fetch_html(sub_url)
        if looks_blocked(list_html):
            row["Notes"] = "Blocked/Captcha on list page"
            all_rows.append(row)
            continue

        asin = extract_5th_asin(list_html)
        if not asin:
            row["Notes"] = "Could not extract #5 ASIN (list layout mismatch)"
            all_rows.append(row)
            continue

        product_url = f"https://www.amazon.com/dp/{asin}"
        row["ASIN"] = asin
        row["ProductURL"] = product_url

        # product page (NO JS)
        prod_html = wsa_fetch_html(product_url)
        if looks_blocked(prod_html):
            row["Notes"] = "Blocked/Captcha on product page"
            all_rows.append(row)
            continue

        title, author = extract_title_author(prod_html)
        bsr = extract_bsr(prod_html)

        row["Title"] = title
        row["Author"] = author
        row["OverallBestSellersRank"] = bsr if bsr is not None else ""
        row["TopicKeywords"] = topic_keywords(title)

        if not title:
            row["Notes"] = "Title not found (product layout mismatch)"
        if bsr is None:
            row["Notes"] = (row["Notes"] + " | " if row["Notes"] else "") + "BSR not found"

        if isinstance(bsr, int) and bsr < BSR_THRESHOLD:
            row["Shortlisted(<20000)"] = "Y"
            shortlist_rows.append(row.copy())

        all_rows.append(row)

    # 3) Write outputs
    os.makedirs(OUT_DIR, exist_ok=True)
    write_csv(f"{OUT_DIR}/{run_iso}_all.csv", all_rows, headers)
    write_csv(f"{OUT_DIR}/{run_iso}_shortlist.csv", shortlist_rows, headers)

    titles_count = sum(1 for r in all_rows if (r.get("Title") or "").strip())
    print(f"Done. Titles captured: {titles_count}. Shortlisted: {len(shortlist_rows)}.")

    if titles_count == 0:
        raise RuntimeError("No titles captured across all sub-niches. Likely blocked HTML or layout mismatch.")


if __name__ == "__main__":
    main()
