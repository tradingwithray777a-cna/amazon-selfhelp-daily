import os
import re
import datetime
from urllib.parse import quote_plus

import requests
import pandas as pd
from bs4 import BeautifulSoup

# 1) Your main Self-Help bestseller page
BASE_URL = "https://www.amazon.com/Best-Sellers-Kindle-Store-Self-Help/zgbs/digital-text/156563011"

# 2) Your shortlist rule (overall Amazon Best Sellers Rank number)
RANK_THRESHOLD = 20000

# 3) Your sub-niches list
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

OUTPUT_DIR = "output"

# WebScrapingAPI endpoint style:
# https://api.webscrapingapi.com/v2?api_key=<YOUR_API_KEY>&url=<ENCODED_URL>
# (per their GET request documentation) :contentReference[oaicite:3]{index=3}
WSA_API_KEY = os.getenv("WSA_API_KEY", "").strip()


def simplify(s: str) -> str:
    # lower + keep only letters/numbers (helps match labels)
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def wsa_fetch_html(url: str) -> str:
    """Fetch HTML via WebScrapingAPI (more reliable on Amazon from GitHub runners)."""
    if not WSA_API_KEY:
        raise RuntimeError("Missing WSA_API_KEY secret. Add it in GitHub repo settings.")
    api = f"https://api.webscrapingapi.com/v2?api_key={WSA_API_KEY}&url={quote_plus(url)}"
    r = requests.get(api, timeout=90)
    r.raise_for_status()
    return r.text


def extract_left_nav_links(html: str) -> dict:
    """Parse the left browse tree and return {simplified_text: url}."""
    soup = BeautifulSoup(html, "lxml")
    root = soup.select_one("#zg_browseRoot") or soup
    links = {}
    for a in root.select("a"):
        txt = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        if not txt or not href:
            continue
        if href.startswith("/"):
            href = "https://www.amazon.com" + href
        links[simplify(txt)] = href
    return links


def best_match_link(subniche: str, link_map: dict) -> str | None:
    """Find the best link for a subniche name from the left-nav link map."""
    key = simplify(subniche)

    # Exact match first
    if key in link_map:
        return link_map[key]

    # Try removing NLP parentheses if needed
    key2 = simplify(subniche.replace("(NLP)", "").replace("NLP", ""))
    if key2 in link_map:
        return link_map[key2]

    # Fuzzy: choose the link key with max overlap
    best_url = None
    best_score = 0
    for k, url in link_map.items():
        # simple overlap scoring
        score = 0
        for ch in set(key):
            if ch in k:
                score += 1
        if score > best_score:
            best_score = score
            best_url = url

    # Require a minimum score so we don't pick nonsense
    return best_url if best_score >= max(8, len(key) // 2) else None


def extract_5th_asin(bestseller_html: str) -> str | None:
    """Get ASIN of the #5 item on a bestseller list page."""
    soup = BeautifulSoup(bestseller_html, "lxml")
    items = soup.select("ol#zg-ordered-list > li")
    if len(items) >= 5:
        block = str(items[4])
        m = re.search(r"/dp/([A-Z0-9]{10})", block)
        if m:
            return m.group(1)

    # fallback: 5th unique /dp/ASIN on the page
    asins = []
    for m in re.finditer(r"/dp/([A-Z0-9]{10})", bestseller_html):
        a = m.group(1)
        if a not in asins:
            asins.append(a)
    return asins[4] if len(asins) >= 5 else None


def extract_title(product_html: str) -> str:
    soup = BeautifulSoup(product_html, "lxml")
    t = soup.select_one("#productTitle")
    if t:
        return t.get_text(" ", strip=True)
    tt = soup.find("title")
    return tt.get_text(" ", strip=True) if tt else ""


def extract_best_sellers_rank(product_html: str) -> int | None:
    """
    Extract the first number after 'Best Sellers Rank' from common product-detail sections.
    """
    soup = BeautifulSoup(product_html, "lxml")

    candidates = []
    for sel in [
        "#detailBulletsWrapper_feature_div",
        "#detailBullets_feature_div",
        "#productDetails_detailBullets_sections1",
        "#productDetails_db_sections",
        "#bookDetails_feature_div",
    ]:
        node = soup.select_one(sel)
        if node:
            candidates.append(node.get_text("\n", strip=True))

    candidates.append(soup.get_text("\n", strip=True))

    for text in candidates:
        idx = text.lower().find("best sellers rank")
        snippet = text[idx: idx + 7000] if idx != -1 else text
        m = re.search(r"Best\s*Sellers\s*Rank.*?#\s*([\d,]+)", snippet, flags=re.IGNORECASE | re.DOTALL)
        if m:
            return int(m.group(1).replace(",", ""))
    return None


def topic_keywords(title: str) -> str:
    stop = {"the","and","for","with","your","you","how","to","a","an","of","in","on","at","from",
            "book","guide","workbook","journal","edition","revised","ultimate","complete"}
    words = re.findall(r"[A-Za-z']{3,}", (title or "").lower())
    out = []
    seen = set()
    for w in words:
        if w in stop or w in seen:
            continue
        seen.add(w)
        out.append(w)
    return ", ".join(out[:6])


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    run_date = datetime.date.today().isoformat()

    # Fetch main page via WebScrapingAPI and parse left nav links
    base_html = wsa_fetch_html(BASE_URL)
    link_map = extract_left_nav_links(base_html)

    rows = []
    shortlist_rows = []

    for sub in SUB_NICHES:
        sub_url = best_match_link(sub, link_map)
        if not sub_url:
            rows.append({
                "Date": run_date,
                "SubNiche": sub,
                "SubNicheRank": 5,
                "Title": "",
                "ASIN": "",
                "OverallBestSellersRank": "",
                "Shortlisted(<20000)": "N",
                "TopicKeywords": "",
                "ProductURL": "",
                "Notes": "Sub-niche link not found in left nav (page may have changed)"
            })
            continue

        try:
            best_html = wsa_fetch_html(sub_url)
        except Exception as e:
            rows.append({
                "Date": run_date,
                "SubNiche": sub,
                "SubNicheRank": 5,
                "Title": "",
                "ASIN": "",
                "OverallBestSellersRank": "",
                "Shortlisted(<20000)": "N",
                "TopicKeywords": "",
                "ProductURL": "",
                "Notes": f"Failed to load sub-niche page: {e}"
            })
            continue

        asin = extract_5th_asin(best_html)
        if not asin:
            rows.append({
                "Date": run_date,
                "SubNiche": sub,
                "SubNicheRank": 5,
                "Title": "",
                "ASIN": "",
                "OverallBestSellersRank": "",
                "Shortlisted(<20000)": "N",
                "TopicKeywords": "",
                "ProductURL": "",
                "Notes": "Could not extract #5 ASIN (layout changed or blocked HTML)"
            })
            continue

        product_url = f"https://www.amazon.com/dp/{asin}"

        try:
            prod_html = wsa_fetch_html(product_url)
        except Exception as e:
            rows.append({
                "Date": run_date,
                "SubNiche": sub,
                "SubNicheRank": 5,
                "Title": "",
                "ASIN": asin,
                "OverallBestSellersRank": "",
                "Shortlisted(<20000)": "N",
                "TopicKeywords": "",
                "ProductURL": product_url,
                "Notes": f"Failed to load product page: {e}"
            })
            continue

        title = extract_title(prod_html)
        rank = extract_best_sellers_rank(prod_html)
        shortlisted = "Y" if isinstance(rank, int) and rank < RANK_THRESHOLD else "N"

        row = {
            "Date": run_date,
            "SubNiche": sub,
            "SubNicheRank": 5,
            "Title": title,
            "ASIN": asin,
            "OverallBestSellersRank": rank if rank is not None else "",
            "Shortlisted(<20000)": shortlisted,
            "TopicKeywords": topic_keywords(title),
            "ProductURL": product_url,
            "Notes": "" if rank is not None else "Best Sellers Rank not found on product page"
        }
        rows.append(row)
        if shortlisted == "Y":
            shortlist_rows.append(row)

    pd.DataFrame(rows).to_csv(f"{OUTPUT_DIR}/{run_date}_all.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(shortlist_rows).to_csv(f"{OUTPUT_DIR}/{run_date}_shortlist.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
