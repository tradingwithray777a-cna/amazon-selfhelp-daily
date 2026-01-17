import os
import re
import random
import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE_SELF_HELP = "https://www.amazon.com/Best-Sellers-Kindle-Store-Self-Help/zgbs/digital-text/156563011"
RANK_THRESHOLD = 20000

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


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


def ensure_abs_url(href: str) -> str:
    if href.startswith("/"):
        return "https://www.amazon.com" + href
    return href


def safe_wait(page) -> None:
    page.wait_for_timeout(random.randint(700, 1400))


def maybe_accept_cookies(page) -> None:
    # Amazon cookie banner often uses this id
    try:
        btn = page.locator("#sp-cc-accept")
        if btn.count() > 0:
            btn.first.click(timeout=1500)
            safe_wait(page)
    except Exception:
        pass


def fetch_html(page, url: str) -> str:
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    safe_wait(page)
    maybe_accept_cookies(page)

    # Let dynamic sections finish loading
    try:
        page.wait_for_load_state("networkidle", timeout=30000)
    except Exception:
        pass

    # Trigger lazy-loaded product details
    try:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1200)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(400)
    except Exception:
        pass

    return page.content()


def extract_subniche_links(selfhelp_html: str) -> Dict[str, str]:
    soup = BeautifulSoup(selfhelp_html, "lxml")
    root = soup.select_one("#zg_browseRoot") or soup
    links: Dict[str, str] = {}
    for a in root.select("a"):
        name = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        if not name or not href:
            continue
        links[norm(name)] = ensure_abs_url(href)
    return links


def extract_5th_asin(bestseller_html: str) -> Optional[str]:
    soup = BeautifulSoup(bestseller_html, "lxml")
    li_items = soup.select("ol#zg-ordered-list > li")
    if len(li_items) >= 5:
        block = str(li_items[4])
        m = re.search(r"/dp/([A-Z0-9]{10})", block)
        if m:
            return m.group(1)

    # fallback: 5th unique /dp/ASIN in page
    asins: List[str] = []
    for m in re.finditer(r"/dp/([A-Z0-9]{10})", bestseller_html):
        a = m.group(1)
        if a not in asins:
            asins.append(a)
    return asins[4] if len(asins) >= 5 else None


def extract_title_author(product_html: str) -> Tuple[str, str]:
    soup = BeautifulSoup(product_html, "lxml")
    title = ""
    t = soup.select_one("#productTitle")
    if t:
        title = t.get_text(" ", strip=True)
    else:
        tt = soup.find("title")
        title = tt.get_text(" ", strip=True) if tt else ""

    author = ""
    by = soup.select_one("#bylineInfo")
    if by:
        author = by.get_text(" ", strip=True)

    return title, author


def parse_best_sellers_rank(product_html: str) -> Optional[int]:
    """
    Extract the first number after "Best Sellers Rank" from common product-detail sections.
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

    # Fallback: whole page text
    candidates.append(soup.get_text("\n", strip=True))

    for text in candidates:
        idx = text.lower().find("best sellers rank")
        snippet = text[idx: idx + 6000] if idx != -1 else text
        m = re.search(r"Best\s*Sellers\s*Rank.*?#\s*([\d,]+)", snippet, flags=re.IGNORECASE | re.DOTALL)
        if m:
            return int(m.group(1).replace(",", ""))

    return None



def topic_from_title(title: str) -> str:
    stop = {
        "the","and","for","with","your","you","how","to","a","an","of","in","on","at","from",
        "book","guide","workbook","journal","edition","revised","ultimate","complete"
    }
    words = re.findall(r"[A-Za-z']{3,}", (title or "").lower())
    keywords, seen = [], set()
    for w in words:
        if w in stop:
            continue
        if w not in seen:
            seen.add(w)
            keywords.append(w)
    return ", ".join(keywords[:6])


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    run_date = datetime.date.today().isoformat()

    rows = []
    short_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        base_html = fetch_html(page, BASE_SELF_HELP)
        link_map = extract_subniche_links(base_html)

        for sub in SUB_NICHES:
            sub_key = norm(sub)
            sub_url = link_map.get(sub_key)

            # fuzzy match (Amazon labels sometimes differ slightly)
            if not sub_url:
                for k, v in link_map.items():
                    if sub_key in k or k in sub_key:
                        sub_url = v
                        break

            if not sub_url:
                rows.append({
                    "Date": run_date,
                    "SubNiche": sub,
                    "SubNicheRank": 5,
                    "Title": "",
                    "Author": "",
                    "ASIN": "",
                    "OverallBestSellersRank": "",
                    "Shortlisted(<20000)": "N",
                    "TopicKeywords": "",
                    "ProductURL": "",
                    "Notes": "Sub-niche link not found"
                })
                continue

            try:
                best_html = fetch_html(page, sub_url)
            except PWTimeout:
                best_html = ""

            asin = extract_5th_asin(best_html) if best_html else None
            if not asin:
                rows.append({
                    "Date": run_date,
                    "SubNiche": sub,
                    "SubNicheRank": 5,
                    "Title": "",
                    "Author": "",
                    "ASIN": "",
                    "OverallBestSellersRank": "",
                    "Shortlisted(<20000)": "N",
                    "TopicKeywords": "",
                    "ProductURL": "",
                    "Notes": "Could not extract #5 ASIN (layout/bot check)"
                })
                continue

            product_url = f"https://www.amazon.com/dp/{asin}"

            try:
                prod_html = fetch_html(page, product_url)
            except PWTimeout:
                prod_html = ""

            title, author = extract_title_author(prod_html)
            overall_rank = parse_best_sellers_rank(prod_html)
            shortlisted = "Y" if isinstance(overall_rank, int) and overall_rank < RANK_THRESHOLD else "N"

            row = {
                "Date": run_date,
                "SubNiche": sub,
                "SubNicheRank": 5,
                "Title": title,
                "Author": author,
                "ASIN": asin,
                "OverallBestSellersRank": overall_rank if overall_rank is not None else "",
                "Shortlisted(<20000)": shortlisted,
                "TopicKeywords": topic_from_title(title),
                "ProductURL": product_url,
                "Notes": ""
            }
            rows.append(row)
            if shortlisted == "Y":
                short_rows.append(row)

        browser.close()

    all_path = os.path.join(OUTPUT_DIR, f"{run_date}_all.csv")
    short_path = os.path.join(OUTPUT_DIR, f"{run_date}_shortlist.csv")

    pd.DataFrame(rows).to_csv(all_path, index=False, encoding="utf-8-sig")
    pd.DataFrame(short_rows).to_csv(short_path, index=False, encoding="utf-8-sig")

    print("Saved:", all_path)
    print("Saved:", short_path)


if __name__ == "__main__":
    main()
