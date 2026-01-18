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

# Rate control (override via GitHub Actions env)
MIN_GAP_SECONDS = float(os.getenv("WSA_MIN_GAP_SECONDS", "12"))
JITTER_SECONDS = float(os.getenv("WSA_JITTER_SECONDS", "3"))
MAX_TOTAL_WAIT_SECONDS = int(os.getenv("WSA_MAX_TOTAL_WAIT_SECONDS", "900"))

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


def wsa_fetch_html(url: str, render_js: int = 0) -> str:
    """Fetch URL via WebScrapingAPI with 429 backoff."""
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
                raise RuntimeError(f"WSA kept rate-limiting (429) too long. Total waited ~{int(waited + backoff)}s.")
            continue

        try:
            r.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"WSA error HTTP {r.status_code} for {url}. Body sample: {r.text[:200]}") from e


def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def token_set(s: str) -> set:
    return set(re.findall(r"[a-z0-9]+", (s or "").lower()))


def best_match_subniche(sub: str, link_map: dict) -> str | None:
    """Exact match first, then token overlap."""
    k = norm(sub)
    if k in link_map:
        return link_map[k]

    ts = token_set(sub)
    best_url = None
    best_score = 0

    for kk, url in link_map.items():
        # reconstruct tokens from normalized key is hard; compare on original stored label map in value tuple
        pass

    # If exact not found, we’ll do a second map that stores original labels
    return None


def extract_subniche_links_from_base(base_html: str) -> tuple[dict, dict]:
    """
    Return:
      - link_map: normalized_label -> url
      - label_map: normalized_label -> original_label (for debugging)
    """
    soup = BeautifulSoup(base_html, "lxml")
    root = soup.select_one("#zg_browseRoot")
    if not root:
        return {}, {}

    link_map = {}
    label_map = {}

    for a in root.select("a[href]"):
        label = a.get_text(" ", strip=True)
        href = a.get("href", "").strip()
        if not label or not href:
            continue

        abs_url = urljoin("https://www.amazon.com", href)
        nk = norm(label)
        link_map[nk] = abs_url
        label_map[nk] = label

    return link_map, label_map


def match_subniche_url(sub: str, link_map: dict, label_map: dict) -> str | None:
    """Exact match by normalized label; else token overlap on labels."""
    nk = norm(sub)
    if nk in link_map:
        return link_map[nk]

    target_tokens = token_set(sub)
    best_url = None
    best_score = 0.0

    for k, url in link_map.items():
        label = label_map.get(k, "")
        tokens = token_set(label)
        if not tokens:
            continue
        score = len(target_tokens & tokens) / max(1, len(target_tokens | tokens))
        if score > best_score:
            best_score = score
            best_url = url

    return best_url if best_score >= 0.4 else None


def extract_5th_asin(list_html: str) -> str | None:
    """Extract ASIN of the #5 item on the bestseller list page."""
    soup = BeautifulSoup(list_html, "lxml")
    li_items = soup.select("ol#zg-ordered-list > li")
    if len(li_items) >= 5:
        block = str(li_items[4])
        m = re.search(r"/dp/([A-Z0-9]{10})", block)
        if m:
            return m.group(1)

    # fallback: 5th unique /dp/ASIN
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
    Extract first Best Sellers Rank number from full page text.
    Using render_js=1 helps when it’s behind “See all details”.
    """
    soup = BeautifulSoup(product_html, "lxml")
    text = soup.get_text("\n", strip=True)

    idx = text.lower().find("best sellers rank")
    if idx == -1:
        idx = text.lower().find("amazon best sellers rank")
    if idx == -1:
        return None

    window = text[idx: idx + 6000]
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
    today = datetime.date.today().strftime("%-d/%-m/%Y")  # matches your earlier CSV style like 18/1/2026
    out_date = datetime.date.today().isoformat()

    headers = [
        "Date", "SubNiche", "SubNicheRank", "Title", "Author", "ASIN",
        "OverallBestSellersRank", "Shortlisted(<20000)", "TopicKeywords", "ProductURL", "Notes"
    ]

    all_rows = []
    shortlist_rows = []

    # 1) Fetch base page WITH JS so left nav loads
    base_html = wsa_fetch_html(BASE_URL, render_js=1)
    if looks_blocked(base_html):
        # If base is blocked, everything will fail — write rows with that note
        for sub in SUB_NICHES:
            all_rows.append({
                "Date": today, "SubNiche": sub, "SubNicheRank": 5,
                "Title": "", "Author": "", "ASIN": "", "OverallBestSellersRank": "",
                "Shortlisted(<20000)": "N", "TopicKeywords": "", "ProductURL": "",
                "Notes": "Blocked/Captcha on BASE page"
            })
        write_csv(f"{OUT_DIR}/{out_date}_all.csv", all_rows, headers)
        write_csv(f"{OUT_DIR}/{out_date}_shortlist.csv", shortlist_rows, headers)
        raise RuntimeError("Base page blocked/captcha. No titles captured.")

    link_map, label_map = extract_subniche_links_from_base(base_html)
    if not link_map:
        for sub in SUB_NICHES:
            all_rows.append({
                "Date": today, "SubNiche": sub, "SubNicheRank": 5,
                "Title": "", "Author": "", "ASIN": "", "OverallBestSellersRank": "",
                "Shortlisted(<20000)": "N", "TopicKeywords": "", "ProductURL": "",
                "Notes": "Base page missing left nav (#zg_browseRoot not found)"
            })
        write_csv(f"{OUT_DIR}/{out_date}_all.csv", all_rows, headers)
        write_csv(f"{OUT_DIR}/{out_date}_shortlist.csv", shortlist_rows, headers)
        raise RuntimeError("Base page left nav not found. No titles captured.")

    # 2) For each subniche, use its actual URL from the base page nav
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

        sub_url = match_subniche_url(sub, link_map, label_map)
        if not sub_url:
            row["Notes"] = "Sub-niche link not found on base page nav"
            all_rows.append(row)
            continue

        # 3) Fetch list page; retry with JS if needed
        list_html = wsa_fetch_html(sub_url, render_js=0)
        if looks_blocked(list_html) or ("/dp/" not in list_html and "zg-ordered-list" not in list_html):
            list_html = wsa_fetch_html(sub_url, render_js=1)

        if looks_blocked(list_html):
            row["Notes"] = "Blocked/Captcha on sub-niche list page"
            all_rows.append(row)
            continue

        asin = extract_5th_asin(list_html)
        if not asin:
            row["Notes"] = "Could not extract #5 ASIN from list page (layout changed)"
            all_rows.append(row)
            continue

        product_url = f"https://www.amazon.com/dp/{asin}"
        row["ASIN"] = asin
        row["ProductURL"] = product_url

        # 4) Product page with JS to capture “See all details” / BSR section
        prod_html = wsa_fetch_html(product_url, render_js=1)
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
            row["Notes"] = "Title not found on product page (layout mismatch)"
        if bsr is None:
            row["Notes"] = (row["Notes"] + " | " if row["Notes"] else "") + "BSR not found (may still be hidden)"

        if isinstance(bsr, int) and bsr < BSR_THRESHOLD:
            row["Shortlisted(<20000)"] = "Y"
            shortlist_rows.append(row.copy())

        all_rows.append(row)

    os.makedirs(OUT_DIR, exist_ok=True)
    write_csv(f"{OUT_DIR}/{out_date}_all.csv", all_rows, headers)
    write_csv(f"{OUT_DIR}/{out_date}_shortlist.csv", shortlist_rows, headers)

    titles_count = sum(1 for r in all_rows if (r.get("Title") or "").strip())
    print(f"Done. Titles captured: {titles_count}. Shortlisted: {len(shortlist_rows)}.")

    if titles_count == 0:
        raise RuntimeError("No titles captured across all sub-niches. Likely blocked HTML or layout mismatch.")


if __name__ == "__main__":
    main()
