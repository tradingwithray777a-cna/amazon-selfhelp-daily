import os
import re
import csv
import time
import random
import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# -----------------------------
# CONFIG
# -----------------------------
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

# WebScrapingAPI Scraper API (v2) endpoint format:
# https://api.webscrapingapi.com/v2?api_key=<YOUR_API_KEY>&url=<TARGETED_URL> :contentReference[oaicite:1]{index=1}
WSA_ENDPOINT = "https://api.webscrapingapi.com/v2"

# Tuning knobs (you can also set these as GitHub Action env vars)
WSA_TIMEOUT_SECONDS = int(os.getenv("WSA_TIMEOUT_SECONDS", "60"))

# Minimum spacing between ANY two WSA calls (prevents “short succession” rate limit)
WSA_MIN_GAP_SECONDS = float(os.getenv("WSA_MIN_GAP_SECONDS", "6.0"))

# How long we are willing to “sit inside the workflow” retrying 429 before giving up for the day
WSA_MAX_TOTAL_WAIT_SECONDS = int(os.getenv("WSA_MAX_TOTAL_WAIT_SECONDS", "2400"))  # 40 min

# Rank filter
MAX_BSR = int(os.getenv("MAX_BSR", "20000"))

OUT_DIR = "output"


# -----------------------------
# HELPERS
# -----------------------------
def norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", "and")
    s = re.sub(r"[’'`]", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def token_score(a: str, b: str) -> float:
    """Simple token overlap score for fuzzy matching."""
    ta = set(norm_text(a).split())
    tb = set(norm_text(b).split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(1, len(ta | tb))


_last_wsa_call_ts = 0.0


def _throttle():
    """Enforce minimum spacing between WSA calls."""
    global _last_wsa_call_ts
    now = time.time()
    gap = now - _last_wsa_call_ts
    if gap < WSA_MIN_GAP_SECONDS:
        sleep_for = (WSA_MIN_GAP_SECONDS - gap) + random.uniform(0.2, 1.0)
        time.sleep(sleep_for)
    _last_wsa_call_ts = time.time()


def wsa_fetch_html(session: requests.Session, target_url: str) -> str:
    """
    Fetch a page via WebScrapingAPI.
    Retries hard on 429 with long backoff so your workflow doesn't just die.
    """
    api_key = os.getenv("WSA_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing WSA_API_KEY. Add it in GitHub: Settings → Secrets and variables → Actions.")

    start = time.time()
    attempt = 0

    while True:
        # Stop if we have already waited too long overall
        elapsed = time.time() - start
        if elapsed > WSA_MAX_TOTAL_WAIT_SECONDS:
            raise RuntimeError(
                f"WSA kept rate-limiting (429) for too long. Total waited ~{int(elapsed)}s."
            )

        _throttle()

        params = {
            "api_key": api_key,
            "url": target_url,
        }

        try:
            r = session.get(WSA_ENDPOINT, params=params, timeout=WSA_TIMEOUT_SECONDS)
        except requests.RequestException as e:
            # Network blip: backoff and retry
            wait = min(180, (2 ** min(attempt, 6)) * 2 + random.uniform(0.5, 2.5))
            print(f"[WSA] Network error: {e}. Waiting {wait:.1f}s then retrying...")
            time.sleep(wait)
            attempt += 1
            continue

        if r.status_code == 200 and r.text:
            return r.text

        # 429 handling (main issue)
        if r.status_code == 429:
            # Prefer server hint if present
            ra = r.headers.get("Retry-After", "").strip()
            if ra.isdigit():
                wait = int(ra) + random.uniform(0.5, 2.0)
            else:
                # long exponential backoff with cap
                wait = min(300, (2 ** min(attempt, 7)) * 2 + random.uniform(1.0, 4.0))
            print(f"[WSA] 429 Too Many Requests. Waiting {wait:.1f}s then retrying...")
            time.sleep(wait)
            attempt += 1
            continue

        # transient server errors
        if r.status_code in (500, 503):
            wait = min(180, (2 ** min(attempt, 6)) * 2 + random.uniform(0.5, 2.5))
            print(f"[WSA] {r.status_code} Server error. Waiting {wait:.1f}s then retrying...")
            time.sleep(wait)
            attempt += 1
            continue

        # other errors: raise with detail
        try:
            r.raise_for_status()
        except Exception:
            raise RuntimeError(f"WSA error {r.status_code} for {target_url}. Body (first 200): {r.text[:200]}")


def extract_subniche_links(base_html: str) -> dict:
    """
    Pull the left-nav subcategory links from the Best Sellers page.
    Returns mapping: normalized subniche text -> absolute url
    """
    soup = BeautifulSoup(base_html, "lxml")
    root = soup.find(id="zg_browseRoot")

    links = {}
    if root:
        for a in root.select("a[href]"):
            txt = a.get_text(" ", strip=True)
            href = a.get("href", "").strip()
            if not txt or not href:
                continue
            abs_url = urljoin("https://www.amazon.com", href)
            links[norm_text(txt)] = abs_url

    return links


def pick_best_match(target: str, link_map: dict) -> str | None:
    """
    Find the best matching link for a target subniche name.
    """
    nt = norm_text(target)
    if nt in link_map:
        return link_map[nt]

    best = (0.0, None)
    for k, v in link_map.items():
        sc = token_score(nt, k)
        if sc > best[0]:
            best = (sc, v)

    # Require a decent match, otherwise return None
    return best[1] if best[0] >= 0.5 else None


def parse_bestseller_ranked_items(html: str) -> list[dict]:
    """
    Parse the ordered list of best sellers.
    Returns list of dicts: {rank, title, url, asin}
    """
    soup = BeautifulSoup(html, "lxml")

    # Most best-seller pages use this:
    ol = soup.select_one("ol#zg-ordered-list")
    candidates = []
    if ol:
        lis = ol.select(":scope > li")
        for idx, li in enumerate(lis, start=1):
            a = li.select_one("a.a-link-normal[href*='/dp/'], a.a-link-normal[href*='/gp/']")
            if not a:
                continue
            href = urljoin("https://www.amazon.com", a.get("href", ""))
            title = None

            t = li.select_one("div.p13n-sc-truncated")
            if t and t.get_text(strip=True):
                title = t.get_text(" ", strip=True)
            if not title:
                img = li.select_one("img[alt]")
                if img and img.get("alt"):
                    title = img.get("alt").strip()
            if not title:
                title = a.get_text(" ", strip=True) or ""

            asin = None
            m = re.search(r"/dp/([A-Z0-9]{10})", href)
            if not m:
                m = re.search(r"/gp/product/([A-Z0-9]{10})", href)
            if m:
                asin = m.group(1)

            candidates.append({"rank": idx, "title": title, "url": href, "asin": asin})

    # Fallback: if structure changes, grab dp links and keep first 100 unique
    if not candidates:
        seen = set()
        for a in soup.select("a[href*='/dp/']"):
            href = urljoin("https://www.amazon.com", a.get("href", ""))
            m = re.search(r"/dp/([A-Z0-9]{10})", href)
            if not m:
                continue
            asin = m.group(1)
            if asin in seen:
                continue
            seen.add(asin)

            title = a.get_text(" ", strip=True)
            if not title:
                img = a.select_one("img[alt]")
                if img and img.get("alt"):
                    title = img.get("alt").strip()

            candidates.append({"rank": len(candidates) + 1, "title": title or "", "url": href, "asin": asin})
            if len(candidates) >= 100:
                break

    return candidates


def parse_best_sellers_rank_number(product_html: str) -> tuple[int | None, str]:
    """
    Extract the first numeric "Best Sellers Rank" value from product page.
    Returns (bsr_number, context_text)
    """
    soup = BeautifulSoup(product_html, "lxml")

    # Try targeted areas first
    sections = []
    for sec_id in ["detailBullets_feature_div", "detailBulletsWrapper_feature_div", "prodDetails", "bookDetails_container"]:
        s = soup.find(id=sec_id)
        if s:
            sections.append(s.get_text("\n", strip=True))

    # Add full text as last resort (can be large)
    sections.append(soup.get_text("\n", strip=True))

    joined = "\n".join(sections)

    # Match either "Amazon Best Sellers Rank" or "Best Sellers Rank"
    # Capture first "#12,345" style number after that label
    m = re.search(r"(Amazon\s+Best\s+Sellers\s+Rank|Best\s+Sellers\s+Rank)\s*[:\-]?\s*#\s*([\d,]+)", joined, re.IGNORECASE)
    if not m:
        # Sometimes formatted like "Best Sellers Rank #12,345 in Kindle Store"
        m = re.search(r"(Amazon\s+Best\s+Sellers\s+Rank|Best\s+Sellers\s+Rank).*?#\s*([\d,]+)", joined, re.IGNORECASE)

    if m:
        num = int(m.group(2).replace(",", ""))
        # Take a short nearby context snippet for debugging
        start = max(0, m.start() - 60)
        end = min(len(joined), m.end() + 120)
        ctx = joined[start:end].replace("\n", " ")
        ctx = re.sub(r"\s+", " ", ctx).strip()
        return num, ctx

    return None, ""


def infer_topic(title: str, subniche: str) -> str:
    """
    Simple, non-AI topic inference (since you want something understandable).
    """
    t = (title or "").lower()

    # Lightweight keyword rules
    rules = [
        ("anxiety", "Anxiety / Panic / Worry"),
        ("phobia", "Phobias"),
        ("anger", "Anger Management"),
        ("stress", "Stress / Burnout"),
        ("trauma", "Trauma / Healing"),
        ("narciss", "Narcissism / Toxic Relationships"),
        ("adhd", "ADHD / Focus"),
        ("habit", "Habits / Behavior Change"),
        ("mindset", "Mindset / Growth"),
        ("confidence", "Confidence / Self-Esteem"),
        ("self esteem", "Self-Esteem"),
        ("time management", "Time Management"),
        ("productivity", "Productivity"),
        ("affirmation", "Affirmations"),
        ("journ", "Journaling / Writing"),
        ("inner child", "Inner Child Work"),
        ("nlp", "NLP"),
        ("meditat", "Meditation / Mindfulness"),
        ("spiritual", "Spiritual Growth"),
    ]
    for key, topic in rules:
        if key in t:
            return topic

    # Fallback: subniche itself is already a good “topic”
    return subniche


def write_csv(path: str, rows: list[dict], fieldnames: list[str]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


# -----------------------------
# MAIN
# -----------------------------
def main():
    today = datetime.date.today().isoformat()
    all_path = os.path.join(OUT_DIR, f"{today}_all.csv")
    shortlist_path = os.path.join(OUT_DIR, f"{today}_shortlist.csv")

    fieldnames = [
        "date",
        "subniche",
        "subniche_url",
        "picked_rank_in_subniche",
        "title",
        "book_url",
        "asin",
        "amazon_best_sellers_rank_number",
        "shortlisted",
        "topic",
        "notes",
    ]

    all_rows = []
    shortlist_rows = []

    session = requests.Session()

    try:
        base_html = wsa_fetch_html(session, BASE_URL)
        link_map = extract_subniche_links(base_html)

        for sub in SUBNICHES:
            row = {
                "date": today,
                "subniche": sub,
                "subniche_url": "",
                "picked_rank_in_subniche": 5,
                "title": "",
                "book_url": "",
                "asin": "",
                "amazon_best_sellers_rank_number": "",
                "shortlisted": "N",
                "topic": "",
                "notes": "",
            }

            sub_url = pick_best_match(sub, link_map)
            if not sub_url:
                row["notes"] = "sub-niche link not found on base page"
                all_rows.append(row)
                continue

            row["subniche_url"] = sub_url

            try:
                sub_html = wsa_fetch_html(session, sub_url)
                items = parse_bestseller_ranked_items(sub_html)
            except Exception as e:
                row["notes"] = f"failed to fetch/parse subniche list: {e}"
                all_rows.append(row)
                continue

            if len(items) < 5:
                row["notes"] = f"subniche list has < 5 items (found {len(items)})"
                all_rows.append(row)
                continue

            fifth = items[4]
            row["title"] = fifth.get("title", "")
            row["book_url"] = fifth.get("url", "")
            row["asin"] = fifth.get("asin", "")

            if not row["book_url"]:
                row["notes"] = "could not extract book url for #5"
                all_rows.append(row)
                continue

            try:
                prod_html = wsa_fetch_html(session, row["book_url"])
                bsr_num, ctx = parse_best_sellers_rank_number(prod_html)
            except Exception as e:
                row["notes"] = f"failed to fetch/parse product page: {e}"
                all_rows.append(row)
                continue

            if bsr_num is None:
                row["notes"] = "Best Sellers Rank not found on product page"
                all_rows.append(row)
                continue

            row["amazon_best_sellers_rank_number"] = bsr_num
            row["topic"] = infer_topic(row["title"], sub)

            if bsr_num < MAX_BSR:
                row["shortlisted"] = "Y"
                shortlist_rows.append(row.copy())

            all_rows.append(row)

    except Exception as e:
        # If BASE_URL fetch fails (like 429 for too long), we still write CSVs so you get artifacts.
        all_rows = [{
            "date": today,
            "subniche": "",
            "subniche_url": "",
            "picked_rank_in_subniche": "",
            "title": "",
            "book_url": "",
            "asin": "",
            "amazon_best_sellers_rank_number": "",
            "shortlisted": "",
            "topic": "",
            "notes": f"RUN FAILED EARLY: {e}",
        }]
        shortlist_rows = []

    # Always write output files (so GitHub Artifacts will show something)
    write_csv(all_path, all_rows, fieldnames)
    write_csv(shortlist_path, shortlist_rows, fieldnames)

    print(f"Wrote: {all_path}")
    print(f"Wrote: {shortlist_path}")


if __name__ == "__main__":
    main()
