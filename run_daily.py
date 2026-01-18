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
    helper_url = f"{HELPER_CATEGORI
