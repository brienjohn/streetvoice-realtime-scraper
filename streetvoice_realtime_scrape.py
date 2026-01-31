from pathlib import Path
script_path = Path("/mnt/data/streetvoice_realtime_scraper.py")
req_path = Path("/mnt/data/requirements.txt")
workflow_path = Path("/mnt/data/.github_workflows_streetvoice_realtime.yml")
script = r'''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StreetVoice realtime chart scraper (hourly) -> CSV

Key improvements for your reported issues:
- genre: extract from <a href="/music/browse/<id>/recommend/latest/">TEXT</a>
- album_title + album_url: extract if present on song page
- comments_count: extract from <span id="comment-counts">34</span> or fallback "留言（34）"
- likes_count / play_count + artist_*_count: avoid "0" placeholders caused by cookie-disabled/SSR;
  try API, try __NEXT_DATA__ fuzzy search, finally optional Playwright (JS-rendered) fallback.
- collaborators: only from "合作音樂人" section (never from comments)
- description/lyrics: only from their sections; never include comment area
- critic_review_url: ONLY if "達人推薦" appears on the song page, else blank
- social links: ignore StreetVoice official accounts/footers when selecting artist socials
- output filename includes timestamp; suitable for 24 files/day.

Usage:
  python streetvoice_realtime_scraper.py --out-dir data --limit 50

Optional (recommended) for accurate counts:
  pip install playwright && python -m playwright install --with-deps chromium

Notes:
- This script is designed to run in GitHub Actions (Ubuntu).
- It will still run without Playwright; counts may remain blank if API is blocked/changed.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# -----------------------------
# Config
# -----------------------------
BASE = "https://streetvoice.com"
CHART_URL = "https://streetvoice.com/music/charts/realtime/all/"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)

HTML_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
    "Connection": "keep-alive",
}

API_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": BASE,
    "Referer": BASE + "/",
    "Connection": "keep-alive",
}

# Filter out obvious StreetVoice official socials (these polluted your CSV)
SOCIAL_BLACKLIST_SUBSTR = {
    "facebook.com/StreetVoiceTaiwan",
    "m.facebook.com/StreetVoiceTaiwan",
    "instagram.com/streetvoice",
    "instagram.com/streetvoice_taiwan",
    "youtube.com/StreetVoiceTV",
    "youtube.com/@StreetVoiceTV",
    "youtube.com/channel/UC",  # keep, but we'll prefer non-blacklisted
}

# -----------------------------
# Optional Playwright
# -----------------------------
HAVE_PLAYWRIGHT = False
try:
    from playwright.sync_api import sync_playwright  # type: ignore
    HAVE_PLAYWRIGHT = True
except Exception:
    HAVE_PLAYWRIGHT = False


# -----------------------------
# Data Model
# -----------------------------
@dataclass
class Row:
    # chart-level
    snapshot_time: str
    rank: int
    song_title: str
    artist_name: str
    likes_count: Optional[int]
    song_url: str
    artist_url: str
    cover_image_url: Optional[str]

    # artist extra
    artist_handle: Optional[str]
    artist_identity: Optional[str]
    artist_city: Optional[str]
    artist_joined_date: Optional[str]  # YYYY-MM-01 (month precision)
    artist_music_count: Optional[int]
    artist_fans_count: Optional[int]
    artist_following_count: Optional[int]
    artist_facebook_url: Optional[str]
    artist_instagram_url: Optional[str]
    artist_youtube_url: Optional[str]
    artist_accredited_datetime: Optional[str]  # YYYY-MM-DD HH:MM

    # song extra
    play_count: Optional[int]
    genre: Optional[str]
    description: Optional[str]
    lyrics: Optional[str]
    release_date: Optional[str]  # YYYY-MM-DD
    collaborators: Optional[str]
    album_title: Optional[str]
    album_url: Optional[str]
    comments_count: Optional[int]
    is_editor_recommended: Optional[bool]
    is_song_of_the_day: Optional[bool]
    critic_review_url: Optional[str]
    song_accredited_datetime: Optional[str]  # YYYY-MM-DD HH:MM


# -----------------------------
# Helpers
# -----------------------------
def now_taipei_iso() -> str:
    # GitHub Actions uses UTC; if you want Asia/Taipei timestamp, apply +8
    t = dt.datetime.utcnow() + dt.timedelta(hours=8)
    return t.strftime("%Y-%m-%d %H:%M:%S")


def filename_ts() -> str:
    t = dt.datetime.utcnow() + dt.timedelta(hours=8)
    return t.strftime("%Y-%m-%d_%H%M")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def to_int(s: str) -> Optional[int]:
    if s is None:
        return None
    s2 = re.sub(r"[^\d]", "", str(s))
    return int(s2) if s2 else None


def clean_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = re.sub(r"\s+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    s = s.strip()
    return s if s else None


def absolute_url(href: str) -> str:
    return urljoin(BASE + "/", href)


def is_blacklisted_social(url: str) -> bool:
    if not url:
        return True
    for bad in SOCIAL_BLACKLIST_SUBSTR:
        if bad.lower() in url.lower():
            return True
    return False


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    data: Optional[bytes] = None,
    timeout: int = 30,
    max_tries: int = 3,
    sleep_base: float = 0.8,
) -> Optional[requests.Response]:
    for i in range(max_tries):
        try:
            resp = session.request(method, url, headers=headers, data=data, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(sleep_base * (2 ** i))
                continue
            return resp
        except requests.RequestException:
            time.sleep(sleep_base * (2 ** i))
    return None


def get_html(session: requests.Session, url: str) -> Optional[str]:
    resp = request_with_retries(session, "GET", url, headers=HTML_HEADERS)
    if not resp:
        return None
    if resp.status_code != 200:
        return None
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def get_soup(session: requests.Session, url: str) -> Optional[BeautifulSoup]:
    html = get_html(session, url)
    if not html:
        return None
    return BeautifulSoup(html, "lxml")


def deep_iter_dict(obj: Any) -> Iterable[Tuple[str, Any]]:
    """Yield (keypath, value) for nested dict/list structures."""
    stack: List[Tuple[str, Any]] = [("", obj)]
    while stack:
        path, cur = stack.pop()
        if isinstance(cur, dict):
            for k, v in cur.items():
                kp = f"{path}.{k}" if path else str(k)
                stack.append((kp, v))
        elif isinstance(cur, list):
            for idx, v in enumerate(cur):
                kp = f"{path}[{idx}]"
                stack.append((kp, v))
        else:
            yield path, cur


def fuzzy_find_int(
    obj: Any,
    include: List[str],
    exclude: Optional[List[str]] = None,
) -> Optional[int]:
    """
    Find an int by scanning keys (case-insensitive substring matching).
    include: list of patterns that MUST match keypath.
    exclude: patterns that MUST NOT match keypath.
    """
    exclude = exclude or []
    candidates: List[int] = []
    for keypath, value in deep_iter_dict(obj):
        kp = keypath.lower()
        if any(p.lower() in kp for p in exclude):
            continue
        if not all(p.lower() in kp for p in include):
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if value >= 0 and value < 10**10:
                candidates.append(int(value))
        elif isinstance(value, str):
            iv = to_int(value)
            if iv is not None:
                candidates.append(iv)
    if not candidates:
        return None
    # Heuristic: prefer larger numbers (play count usually larger than likes)
    return max(candidates)


def parse_zh_month_joined(text: str) -> Optional[str]:
    """
    Parse strings like:
      "新北市・於 2014 年 10 月 加入"
      "於 2020 年 3 月 加入"
      "於 2007 年 6 月 加入"
    -> "YYYY-MM-01"
    """
    if not text:
        return None
    m = re.search(r"於\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*加入", text)
    if not m:
        return None
    y = int(m.group(1))
    mo = int(m.group(2))
    return f"{y:04d}-{mo:02d}-01"


def parse_zh_datetime(text: str) -> Optional[str]:
    """
    Parse "2021 年 4 月 6 日 17:27" -> "2021-04-06 17:27"
    """
    if not text:
        return None
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*(\d{1,2}):(\d{2})", text)
    if not m:
        return None
    y, mo, d, hh, mm = map(int, m.groups())
    return f"{y:04d}-{mo:02d}-{d:02d} {hh:02d}:{mm:02d}"


def parse_release_date(text: str) -> Optional[str]:
    # Matches "發布時間 2026-01-29"
    m = re.search(r"發布時間\s*(\d{4}-\d{2}-\d{2})", text)
    return m.group(1) if m else None


def parse_comments_count(soup: BeautifulSoup) -> Optional[int]:
    # Preferred: <span id="comment-counts">34</span>
    span = soup.select_one("#comment-counts")
    if span and span.get_text(strip=True):
        v = to_int(span.get_text(strip=True))
        if v is not None:
            return v
    # Fallback: "留言（14）"
    h2 = soup.find(lambda tag: tag.name in ("h1","h2","h3") and tag.get_text(strip=True).startswith("留言（"))
    if h2:
        v = to_int(h2.get_text(strip=True))
        return v
    # Another fallback: any text containing "留言（<n>）"
    txt = soup.get_text("\n", strip=True)
    m = re.search(r"留言（\s*(\d+)\s*）", txt)
    return int(m.group(1)) if m else None


def section_text_by_h2(soup: BeautifulSoup, h2_startswith: str) -> Optional[str]:
    """
    Collect text under a section header <h2> that begins with h2_startswith,
    until next <h2>.
    """
    h2 = soup.find("h2", string=lambda s: isinstance(s, str) and s.strip().startswith(h2_startswith))
    if not h2:
        # Sometimes it's <h2>歌詞 動態歌詞</h2> (no exact match)
        h2 = soup.find(lambda tag: tag.name == "h2" and tag.get_text(strip=True).startswith(h2_startswith))
    if not h2:
        return None

    parts: List[str] = []
    for sib in h2.find_all_next():
        if sib == h2:
            continue
        if getattr(sib, "name", None) == "h2":
            break
        # stop before comments area aggressively
        if getattr(sib, "name", None) in ("h1","h2") and "留言（" in sib.get_text(strip=True):
            break
        txt = sib.get_text(" ", strip=True) if hasattr(sib, "get_text") else ""
        if txt:
            parts.append(txt)

    return clean_text("\n".join(parts))


def extract_genre(soup: BeautifulSoup) -> Optional[str]:
    """
    Your hint:
      <a href="/music/browse/2/recommend/latest/">Hip hop / Rap</a>
    We'll match that href pattern and read anchor text.
    """
    a = soup.select_one(r'a[href^="/music/browse/"][href$="/recommend/latest/"]')
    if a:
        g = a.get_text(strip=True)
        return g or None

    # Fallback: sometimes the browse link might not be "recommend/latest"
    a2 = soup.select_one(r'a[href^="/music/browse/"]')
    if a2:
        g = a2.get_text(strip=True)
        return g or None

    return None


def extract_album(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    """
    Find album info and url if present.
    Many pages include something like "收錄於專輯" near an <a>.
    As fallback, look for links that look like album pages.
    """
    # Strategy 1: label + next link
    txt = soup.get_text("\n", strip=True)
    if "收錄於專輯" in txt or "收錄於" in txt:
        # look for an anchor near the label in DOM
        label = soup.find(string=lambda s: isinstance(s, str) and ("收錄於專輯" in s or s.strip() == "收錄於"))
        if label:
            # try next anchor
            parent = label.parent if hasattr(label, "parent") else None
            if parent:
                a = parent.find_next("a")
                if a and a.get("href"):
                    title = a.get_text(strip=True) or None
                    return title, absolute_url(a["href"])

    # Strategy 2: any obvious album link patterns
    for sel in [r'a[href*="/albums/"]', r'a[href*="/album/"]', r'a[href*="/release/"]']:
        a = soup.select_one(sel)
        if a and a.get("href"):
            title = a.get_text(strip=True) or None
            return title, absolute_url(a["href"])

    return None, None


def extract_critic_review_url(soup: BeautifulSoup) -> Optional[str]:
    """
    Requirement: only if page contains the literal "達人推薦".
    If present, take the first link in that section (best effort).
    """
    full_text = soup.get_text("\n", strip=True)
    if "達人推薦" not in full_text:
        return None

    # Try find a section header containing "達人推薦"
    header = soup.find(lambda tag: tag.name in ("h2", "h3") and "達人推薦" in tag.get_text(strip=True))
    if header:
        a = header.find_next("a")
        if a and a.get("href"):
            return absolute_url(a["href"])

    # fallback: any anchor with text containing "達人推薦"
    a2 = soup.find("a", string=lambda s: isinstance(s, str) and "達人推薦" in s)
    if a2 and a2.get("href"):
        return absolute_url(a2["href"])

    return None


def extract_accredited_datetime_from_song(soup: BeautifulSoup) -> Optional[str]:
    """
    Requirement:
      <a ... class="js-accredited" data-accredited-datetime="2021 年 4 月 6 日 17:27">
    """
    a = soup.select_one("a.js-accredited[data-accredited-datetime]")
    if not a:
        return None
    raw = a.get("data-accredited-datetime")
    return parse_zh_datetime(raw or "")


def extract_editor_flags(soup: BeautifulSoup) -> Tuple[Optional[bool], Optional[bool]]:
    text = soup.get_text("\n", strip=True)
    is_editor = "編輯推薦" in text
    # Some pages may show English or Chinese variants. Best-effort:
    is_sotd = ("Song of the Day" in text) or ("本日之歌" in text) or ("今日之歌" in text)
    return is_editor, is_sotd


def extract_collaborators(soup: BeautifulSoup) -> Optional[str]:
    """
    Only from the "合作音樂人" section.
    Never include comment area, never include semicolons preceding "最相關留言".
    """
    h2 = soup.find("h2", string=lambda s: isinstance(s, str) and s.strip() == "合作音樂人")
    if not h2:
        h2 = soup.find(lambda tag: tag.name == "h2" and tag.get_text(strip=True) == "合作音樂人")
    if not h2:
        return None

    names: List[str] = []
    for a in h2.find_all_next("a"):
        # stop at next section header
        nxt_h2 = a.find_previous("h2")
        if nxt_h2 and nxt_h2 != h2:
            break
        t = a.get_text(strip=True)
        if t and t not in names:
            names.append(t)

    return "、".join(names) if names else None


def api_public_song(session: requests.Session, song_id: int, song_url: str) -> Optional[dict]:
    """
    Try the public API that youtube-dl/yt-dlp historically used:
      https://streetvoice.com/api/v1/public/song/<id>/
    It sometimes returns 403; we do POST then GET with referer set to the song page.
    """
    api_url = f"{BASE}/api/v1/public/song/{song_id}/"
    headers = dict(API_HEADERS)
    headers["Referer"] = song_url

    for method in ("POST", "GET"):
        resp = request_with_retries(session, method, api_url, headers=headers, data=(b"" if method == "POST" else None))
        if not resp:
            continue
        if resp.status_code != 200:
            continue
        ctype = resp.headers.get("content-type", "")
        if "json" not in ctype:
            # sometimes HTML/challenge is returned
            continue
        try:
            return resp.json()
        except Exception:
            continue
    return None


def parse_counts_from_api_or_nextdata(song_api: Optional[dict], nextdata: Optional[dict]) -> Tuple[Optional[int], Optional[int]]:
    """
    Return (likes_count, play_count).
    Use fuzzy scanning because key names vary.
    """
    likes = None
    plays = None

    if song_api:
        likes = (
            fuzzy_find_int(song_api, include=["like"], exclude=["comment", "reply", "dislike"]) or
            fuzzy_find_int(song_api, include=["favorite"], exclude=["comment", "reply"]) or
            fuzzy_find_int(song_api, include=["fav"], exclude=["comment", "reply"])
        )
        plays = (
            fuzzy_find_int(song_api, include=["play"], exclude=["display"]) or
            fuzzy_find_int(song_api, include=["listen"], exclude=[]) or
            fuzzy_find_int(song_api, include=["view"], exclude=["review"])
        )

    if not likes and nextdata:
        likes = (
            fuzzy_find_int(nextdata, include=["like"], exclude=["comment", "reply", "dislike"]) or
            fuzzy_find_int(nextdata, include=["favorite"], exclude=["comment", "reply"])
        )
    if not plays and nextdata:
        plays = (
            fuzzy_find_int(nextdata, include=["play"], exclude=["display"]) or
            fuzzy_find_int(nextdata, include=["listen"], exclude=[]) or
            fuzzy_find_int(nextdata, include=["view"], exclude=["review"])
        )

    return likes, plays


def extract_next_data_json(html: str) -> Optional[dict]:
    """
    If StreetVoice is Next.js, sometimes pages contain <script id="__NEXT_DATA__">JSON</script>.
    We try to parse it if present.
    """
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>\s*(\{.*?\})\s*</script>', html, re.S)
    if not m:
        return None
    raw = m.group(1)
    try:
        return json.loads(raw)
    except Exception:
        return None


def playwright_fetch_counts_song(page_text: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Parse rendered page text for play/like counts in the *header stats* area.
    Avoid matching comment likes (e.g. "1 個喜歡") by restricting to content before "發布時間".
    """
    head = page_text.split("發布時間", 1)[0]
    # Plays
    pm = re.search(r"播放次數\s*([0-9,]+)", head, re.S)
    plays = int(pm.group(1).replace(",", "")) if pm else None

    # Likes: match the first "喜歡 <number>" near top;
    # ensure it's not "個喜歡" style in comments by matching immediately after label.
    lm = re.search(r"\b喜歡\s*([0-9,]+)\b", head, re.S)
    likes = int(lm.group(1).replace(",", "")) if lm else None
    return likes, plays


def playwright_fetch_counts_artist(page_text: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """
    Parse rendered artist page text for 音樂/粉絲/追蹤中 counts.
    Restrict to header block before navigation tabs ("主頁" / "關於") to avoid noise.
    """
    head = page_text.split("主頁", 1)[0]
    music = to_int(re.search(r"音樂\s*([0-9,]+)", head, re.S).group(1)) if re.search(r"音樂\s*([0-9,]+)", head, re.S) else None
    fans = to_int(re.search(r"粉絲\s*([0-9,]+)", head, re.S).group(1)) if re.search(r"粉絲\s*([0-9,]+)", head, re.S) else None
    following = to_int(re.search(r"追蹤中\s*([0-9,]+)", head, re.S).group(1)) if re.search(r"追蹤中\s*([0-9,]+)", head, re.S) else None
    return music, fans, following


def scrape_chart(session: requests.Session, limit: int) -> List[Tuple[int, str, str, str, str]]:
    """
    Returns list of (rank, song_title, artist_name, song_url, artist_url)
    """
    soup = get_soup(session, CHART_URL)
    if not soup:
        raise RuntimeError("Failed to load chart page.")

    items = []
    # The chart DOM can change; best-effort: collect all song links containing '/songs/<id>/'
    for a in soup.select('a[href*="/songs/"]'):
        href = a.get("href")
        if not href:
            continue
        song_url = absolute_url(href)
        # Find song id
        m = re.search(r"/songs/(\d+)/", song_url)
        if not m:
            continue

        # Try to capture title/artist from nearby text
        # We'll look at the nearest container (li/div) and pick first 2 lines
        container = a.find_parent(["li", "div", "tr"]) or a.parent
        text_lines = []
        if container:
            for t in container.stripped_strings:
                t = str(t).strip()
                if t:
                    text_lines.append(t)
        # Heuristic: within container, song title is usually present
        song_title = a.get_text(strip=True) or (text_lines[0] if text_lines else "")
        # Artist might be another link to profile
        artist_url = ""
        artist_name = ""
        if container:
            artist_a = container.select_one('a[href^="/"][href$="/"]:not([href*="/songs/"])')
            if artist_a and artist_a.get("href"):
                artist_url = absolute_url(artist_a["href"])
                artist_name = artist_a.get_text(strip=True) or ""
        items.append((0, song_title, artist_name, song_url, artist_url))
        if len(items) >= limit:
            break

    # If rank not on page reliably, set rank sequentially.
    final = []
    for idx, (r, st, an, su, au) in enumerate(items, start=1):
        final.append((idx, st, an, su, au))
    return final


def scrape_artist_profile(
    session: requests.Session,
    artist_url: str,
    *,
    pw_page=None,
) -> Dict[str, Any]:
    """
    Return dict with:
      handle, identity, city, joined_date, music_count, fans_count, following_count,
      fb/ig/yt urls, accredited_datetime (from data-accredited-datetime if present on page)
    """
    out: Dict[str, Any] = {
        "artist_handle": None,
        "artist_identity": None,
        "artist_city": None,
        "artist_joined_date": None,
        "artist_music_count": None,
        "artist_fans_count": None,
        "artist_following_count": None,
        "artist_facebook_url": None,
        "artist_instagram_url": None,
        "artist_youtube_url": None,
        "artist_accredited_datetime": None,
    }

    soup = get_soup(session, artist_url)
    if not soup:
        return out

    page_text = soup.get_text("\n", strip=True)

    # Handle
    # Example line: "@Cliff949・音樂人"
    h5 = soup.find(lambda tag: tag.name in ("h5", "h4") and tag.get_text(strip=True).startswith("@"))
    if h5:
        raw = h5.get_text(strip=True)
        m = re.match(r"(@[^\s・]+)", raw)
        if m:
            out["artist_handle"] = m.group(1)

        # Identity is after "・"
        if "・" in raw:
            parts = raw.split("・", 1)
            if len(parts) == 2:
                out["artist_identity"] = parts[1].strip() or None

    # City + joined date
    # Example: "新北市・於 2014 年 10 月 加入"
    h5b = soup.find(lambda tag: tag.name in ("h5", "h4") and "加入" in tag.get_text(strip=True) and "年" in tag.get_text(strip=True))
    if h5b:
        raw = h5b.get_text(strip=True)
        if "・" in raw:
            city, rest = raw.split("・", 1)
            out["artist_city"] = city.strip() or None
            out["artist_joined_date"] = parse_zh_month_joined(rest)
        else:
            out["artist_joined_date"] = parse_zh_month_joined(raw)

    # Accredited datetime on artist pages (if any)
    a_acc = soup.select_one("a.js-accredited[data-accredited-datetime]")
    if a_acc:
        out["artist_accredited_datetime"] = parse_zh_datetime(a_acc.get("data-accredited-datetime") or "")

    # Social links: prefer links in header block (they usually appear near follow button)
    socials = soup.select('a[href*="facebook.com"], a[href*="instagram.com"], a[href*="youtube.com"], a[href*="youtu.be"]')
    fb = ig = yt = None
    for a in socials:
        href = a.get("href")
        if not href:
            continue
        u = href if href.startswith("http") else absolute_url(href)
        # Skip story URLs etc.
        if "instagram.com/stories" in u:
            continue
        if is_blacklisted_social(u):
            continue
        if ("facebook.com" in u or "m.facebook.com" in u) and not fb:
            fb = u
        elif ("instagram.com" in u) and not ig:
            ig = u.split("?")[0]
        elif ("youtube.com" in u or "youtu.be" in u) and not yt:
            yt = u.split("?")[0]
    out["artist_facebook_url"] = fb
    out["artist_instagram_url"] = ig
    out["artist_youtube_url"] = yt

    # Counts in SSR HTML appear as 0 when cookie-disabled
    cookie_disabled = "Cookie 已被禁用" in page_text
    music = fans = following = None

    # Try parse from SSR (may be 0 placeholder)
    def stat_after(label: str) -> Optional[int]:
        # find "#### 音樂" then next text numeric
        lab = soup.find(lambda tag: tag.name in ("h3","h4","h5","div") and tag.get_text(strip=True) == label)
        if not lab:
            return None
        nxt = lab.find_next(string=True)
        if not nxt:
            return None
        return to_int(str(nxt))

    music0 = stat_after("音樂")
    fans0 = stat_after("粉絲")
    following0 = stat_after("追蹤中")

    # If cookie_disabled and these are 0, treat as unknown
    if not (cookie_disabled and (music0 == 0 or fans0 == 0 or following0 == 0)):
        music, fans, following = music0, fans0, following0

    # Playwright fallback
    if (music is None or fans is None or following is None) and pw_page is not None:
        try:
            pw_page.goto(artist_url, wait_until="networkidle", timeout=60000)
            txt = pw_page.locator("body").inner_text()
            m2, f2, fo2 = playwright_fetch_counts_artist(txt)
            music = music if music is not None else m2
            fans = fans if fans is not None else f2
            following = following if following is not None else fo2
        except Exception:
            pass

    out["artist_music_count"] = music
    out["artist_fans_count"] = fans
    out["artist_following_count"] = following

    return out


def scrape_song_page(
    session: requests.Session,
    song_url: str,
    *,
    pw_page=None,
) -> Dict[str, Any]:
    """
    Extract song-level fields:
      likes_count, play_count, genre, description, lyrics, release_date,
      collaborators, album_title, album_url, comments_count,
      is_editor_recommended, is_song_of_the_day, critic_review_url,
      song_accredited_datetime, cover_image_url
    """
    out: Dict[str, Any] = {
        "likes_count": None,
        "play_count": None,
        "genre": None,
        "description": None,
        "lyrics": None,
        "release_date": None,
        "collaborators": None,
        "album_title": None,
        "album_url": None,
        "comments_count": None,
        "is_editor_recommended": None,
        "is_song_of_the_day": None,
        "critic_review_url": None,
        "song_accredited_datetime": None,
        "cover_image_url": None,
    }

    html = get_html(session, song_url)
    if not html:
        return out
    soup = BeautifulSoup(html, "lxml")

    page_text = soup.get_text("\n", strip=True)
    cookie_disabled = "Cookie 已被禁用" in page_text

    # Cover image
    og = soup.select_one('meta[property="og:image"]')
    if og and og.get("content"):
        out["cover_image_url"] = og["content"]

    # Comments count
    out["comments_count"] = parse_comments_count(soup)

    # Release date
    out["release_date"] = parse_release_date(page_text)

    # Genre
    out["genre"] = extract_genre(soup)

    # Description / Lyrics
    out["description"] = section_text_by_h2(soup, "介紹")
    out["lyrics"] = section_text_by_h2(soup, "歌詞")

    # Collaborators
    out["collaborators"] = extract_collaborators(soup)

    # Album
    at, au = extract_album(soup)
    out["album_title"] = at
    out["album_url"] = au

    # Flags
    ed, sotd = extract_editor_flags(soup)
    out["is_editor_recommended"] = ed
    out["is_song_of_the_day"] = sotd

    # Critic review
    out["critic_review_url"] = extract_critic_review_url(soup)

    # Accredited datetime
    out["song_accredited_datetime"] = extract_accredited_datetime_from_song(soup)

    # Likes / plays:
    # 1) Try to parse from API
    m = re.search(r"/songs/(\d+)/", song_url)
    song_id = int(m.group(1)) if m else None

    nextdata = extract_next_data_json(html)

    likes = plays = None
    song_api = None
    if song_id is not None:
        song_api = api_public_song(session, song_id, song_url)

    likes, plays = parse_counts_from_api_or_nextdata(song_api, nextdata)

    # 2) SSR HTML stats often show "0" when cookie-disabled; treat as unknown.
    if (likes is None or plays is None) and not cookie_disabled:
        # Try SSR extraction (only if not cookie_disabled)
        # Find blocks like "播放次數" then the next numeric
        def stat_after_label(label: str) -> Optional[int]:
            lab = soup.find(string=lambda s: isinstance(s, str) and s.strip() == label)
            if not lab:
                # sometimes inside headings
                lab = soup.find(lambda tag: tag.name in ("h4","h5","div") and tag.get_text(strip=True) == label)
            if not lab:
                return None
            nxt = lab.parent.find_next(string=True) if hasattr(lab, "parent") else None
            return to_int(str(nxt)) if nxt else None

        plays0 = stat_after_label("播放次數")
        likes0 = stat_after_label("喜歡")
        # If values are 0, still consider possibly placeholder; keep only if >0
        if plays0 and plays0 > 0:
            plays = plays0
        if likes0 and likes0 > 0:
            likes = likes0

    # 3) Playwright fallback (recommended) to execute JS and read the true counts.
    if (likes is None or plays is None) and pw_page is not None:
        try:
            pw_page.goto(song_url, wait_until="networkidle", timeout=60000)
            txt = pw_page.locator("body").inner_text()
            l2, p2 = playwright_fetch_counts_song(txt)
            likes = likes if likes is not None else l2
            plays = plays if plays is not None else p2
        except Exception:
            pass

    out["likes_count"] = likes
    out["play_count"] = plays

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data", help="Output directory for CSV files")
    ap.add_argument("--limit", type=int, default=50, help="How many chart items to scrape")
    args = ap.parse_args()

    ensure_dir(args.out_dir)

    session = requests.Session()
    session.headers.update(HTML_HEADERS)

    snapshot_time = now_taipei_iso()

    chart_items = scrape_chart(session, limit=args.limit)

    # Optional Playwright context (reuse one page for performance)
    pw = None
    browser = None
    page = None
    if HAVE_PLAYWRIGHT:
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_extra_http_headers({"Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7"})
        except Exception:
            page = None

    rows: List[Row] = []

    for rank, song_title, artist_name, song_url, artist_url in chart_items:
        # Song page extraction
        song_extra = scrape_song_page(session, song_url, pw_page=page)

        # Artist page extraction
        artist_extra = {}
        if artist_url:
            artist_extra = scrape_artist_profile(session, artist_url, pw_page=page)
        else:
            artist_extra = {
                "artist_handle": None,
                "artist_identity": None,
                "artist_city": None,
                "artist_joined_date": None,
                "artist_music_count": None,
                "artist_fans_count": None,
                "artist_following_count": None,
                "artist_facebook_url": None,
                "artist_instagram_url": None,
                "artist_youtube_url": None,
                "artist_accredited_datetime": None,
            }

        row = Row(
            snapshot_time=snapshot_time,
            rank=rank,
            song_title=song_title,
            artist_name=artist_name,
            likes_count=song_extra.get("likes_count"),
            song_url=song_url,
            artist_url=artist_url,
            cover_image_url=song_extra.get("cover_image_url"),
            artist_handle=artist_extra.get("artist_handle"),
            artist_identity=artist_extra.get("artist_identity"),
            artist_city=artist_extra.get("artist_city"),
            artist_joined_date=artist_extra.get("artist_joined_date"),
            artist_music_count=artist_extra.get("artist_music_count"),
            artist_fans_count=artist_extra.get("artist_fans_count"),
            artist_following_count=artist_extra.get("artist_following_count"),
            artist_facebook_url=artist_extra.get("artist_facebook_url"),
            artist_instagram_url=artist_extra.get("artist_instagram_url"),
            artist_youtube_url=artist_extra.get("artist_youtube_url"),
            artist_accredited_datetime=artist_extra.get("artist_accredited_datetime"),
            play_count=song_extra.get("play_count"),
            genre=song_extra.get("genre"),
            description=song_extra.get("description"),
            lyrics=song_extra.get("lyrics"),
            release_date=song_extra.get("release_date"),
            collaborators=song_extra.get("collaborators"),
            album_title=song_extra.get("album_title"),
            album_url=song_extra.get("album_url"),
            comments_count=song_extra.get("comments_count"),
            is_editor_recommended=song_extra.get("is_editor_recommended"),
            is_song_of_the_day=song_extra.get("is_song_of_the_day"),
            critic_review_url=song_extra.get("critic_review_url"),
            song_accredited_datetime=song_extra.get("song_accredited_datetime"),
        )

        rows.append(row)

    if browser:
        try:
            browser.close()
        except Exception:
            pass
    if pw:
        try:
            pw.stop()
        except Exception:
            pass

    out_file = os.path.join(args.out_dir, f"streetvoice_realtime_all_{filename_ts()}.csv")
    fieldnames = list(asdict(rows[0]).keys()) if rows else [f.name for f in Row.__dataclass_fields__.values()]  # type: ignore

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            d = asdict(r)
            # Normalize empty strings to None
            for k, v in list(d.items()):
                if isinstance(v, str):
                    d[k] = clean_text(v)
            w.writerow(d)

    print(f"Wrote {len(rows)} rows -> {out_file}")
    print("NOTE: For accurate counts, ensure Playwright is installed and chromium is available.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
'''
requirements = """requests>=2.31.0
beautifulsoup4>=4.12.0
lxml>=5.0.0
playwright>=1.40.0
"""
workflow = """name: StreetVoice Realtime Chart (hourly)

on:
  schedule:
    - cron: "5 * * * *"  # every hour at minute 05 (UTC). This is 13:05 Asia/Taipei.
  workflow_dispatch:

jobs:
  scrape:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
          python -m playwright install --with-deps chromium

      - name: Run scraper
        run: |
          python streetvoice_realtime_scraper.py --out-dir data --limit 50

      - name: Commit CSV
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/*.csv
          git commit -m "Add StreetVoice realtime CSV" || echo "No changes to commit"
          git push

      - name: Upload artifact (easy download)
        uses: actions/upload-artifact@v4
        with:
          name: streetvoice-realtime-csv
          path: data/*.csv
"""

script_path.write_text(script, encoding="utf-8")
req_path.write_text(requirements, encoding="utf-8")
workflow_path.write_text(workflow, encoding="utf-8")
(str(script_path), str(req_path), str(workflow_path))

