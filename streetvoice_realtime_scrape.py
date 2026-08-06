#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Optional Playwright (recommended for accurate counts)
HAVE_PLAYWRIGHT = False
try:
    from playwright.sync_api import sync_playwright  # type: ignore
    HAVE_PLAYWRIGHT = True
except Exception:
    HAVE_PLAYWRIGHT = False

BASE = "https://streetvoice.com"

GENRES_REALTIME = ["all", "rock", "folk", "hip_hop", "urban", "electronic", "explore", "ai_generated"]
GENRES_WEEKLY = ["all", "rock", "folk", "hip_hop", "urban", "electronic", "explore"]

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

HTML_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
    "Connection": "keep-alive",
}

API_HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": BASE,
    "Referer": BASE + "/",
    "Connection": "keep-alive",
}

SOCIAL_BLACKLIST_SUBSTR = {
    "facebook.com/streetvoicetaiwan",
    "m.facebook.com/streetvoicetaiwan",
    "instagram.com/streetvoice",
    "instagram.com/streetvoice_taiwan",
    "youtube.com/streetvoicetv",
    "youtube.com/@streetvoicetv",
}


@dataclass
class Row:
    snapshot_time: str

    chart_timeframe: str                          # realtime / weekly
    chart_genre: str
    chart_year: Optional[int]
    chart_week: Optional[int]

    rank: int

    artist_name: str
    song_title: str

    likes_count: Optional[int]
    play_count: Optional[int]
    comments_count: Optional[int]

    song_url: str
    artist_url: str
    cover_image_url: Optional[str]
    cover_image_local_path: Optional[str]

    # artist
    artist_handle: Optional[str]
    artist_identity: Optional[str]
    artist_city: Optional[str]
    artist_joined_date: Optional[str]            # YYYY-MM-01
    artist_accredited_datetime: Optional[str]    # YYYY-MM-DD HH:MM

    artist_music_count: Optional[int]
    artist_fans_count: Optional[int]
    artist_following_count: Optional[int]

    artist_facebook_url: Optional[str]
    artist_instagram_url: Optional[str]
    artist_youtube_url: Optional[str]

    related_news: Optional[str]           # "title|||url;;;title2|||url2"
    big_thing_appearances: Optional[str]  # "title|||date|||url;;;..."

    # song
    genre: Optional[str]
    album_title: Optional[str]
    album_url: Optional[str]

    collaborators: Optional[str]
    description: Optional[str]
    lyrics: Optional[str]
    release_date: Optional[str]                  # YYYY-MM-DD
    song_accredited_datetime: Optional[str]      # YYYY-MM-DD HH:MM

    honors: Optional[str]                        # 全部榮譽徽章，分號分隔，不預設種類
    is_editor_recommended: Optional[bool]
    is_song_of_the_day: Optional[bool]
    critic_review_url: Optional[str]


# ---- Time helpers (Asia/Taipei) ----
def taipei_now() -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(hours=8)

def snapshot_time_str() -> str:
    return taipei_now().strftime("%Y-%m-%d %H:%M:%S")

def filename_ts() -> str:
    return taipei_now().strftime("%Y-%m-%d_%H%M")

def iso_week_info(d: dt.date) -> Tuple[int, int]:
    y, w, _ = d.isocalendar()
    return y, w


# ---- Generic helpers ----
def abs_url(href: str) -> str:
    return urljoin(BASE + "/", href)

def to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    m = re.search(r"(\d[\d,]*)", str(x))
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except Exception:
        return None

def clean_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    return s or None

def is_blacklisted_social(url: str) -> bool:
    if not url:
        return True
    u = url.lower()
    return any(bad in u for bad in SOCIAL_BLACKLIST_SUBSTR)

def request_retry(
    session: requests.Session,
    method: str,
    url: str,
    headers: Dict[str, str],
    data: Optional[bytes] = None,
    tries: int = 3,
    timeout: int = 35,
) -> Optional[requests.Response]:
    for i in range(tries):
        try:
            r = session.request(method, url, headers=headers, data=data, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.8 * (2 ** i))
                continue
            return r
        except requests.RequestException:
            time.sleep(0.8 * (2 ** i))
    return None

def get_html(session: requests.Session, url: str) -> Optional[str]:
    r = request_retry(session, "GET", url, headers=HTML_HEADERS)
    if not r or r.status_code != 200:
        return None
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

def soup_of(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


# ---- Chart ----
def build_chart_url(timeframe: str, genre: str, year: Optional[int] = None, week: Optional[int] = None) -> str:
    if timeframe == "realtime":
        return f"{BASE}/music/charts/realtime/{genre}/"
    return f"{BASE}/music/charts/weekly/{year}/{week}/{genre}/"

def parse_chart(chart_html: str, limit: int) -> List[Tuple[int, str, str, str, str]]:
    soup = soup_of(chart_html)
    out: List[Tuple[int, str, str, str, str]] = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        m = re.match(r"^/([^/]+)/songs/(\d+)/?$", href)
        if not m:
            continue
        artist_slug, song_id = m.group(1), m.group(2)
        key = (artist_slug, song_id)
        if key in seen:
            continue

        song_title_guess = a.get_text(" ", strip=True) or ""
        song_url = abs_url(href)
        artist_url = abs_url(f"/{artist_slug}/")

        artist_name_guess = ""
        container = a.find_parent(["li", "div", "tr"]) or a.parent
        if container:
            aa = container.select_one('a[href^="/"][href$="/"]:not([href*="/songs/"])')
            if aa:
                artist_name_guess = aa.get_text(" ", strip=True) or ""

        out.append((len(out) + 1, song_title_guess, artist_name_guess, song_url, artist_url))
        seen.add(key)
        if len(out) >= limit:
            break

    return out


# ---- Song extractors ----
def extract_genre(soup: BeautifulSoup) -> Optional[str]:
    a = soup.select_one(r'a[href^="/music/browse/"][href$="/recommend/latest/"]')
    if a:
        return clean_text(a.get_text(strip=True))
    a2 = soup.select_one(r'a[href^="/music/browse/"]')
    if a2:
        return clean_text(a2.get_text(strip=True))
    return None

def extract_album(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    for sel in [r'a[href*="/albums/"]', r'a[href*="/album/"]', r'a[href*="/release/"]', r'a[href*="/releases/"]']:
        a = soup.select_one(sel)
        if a and a.get("href"):
            return clean_text(a.get_text(" ", strip=True)), abs_url(a["href"])
    label = soup.find(string=lambda s: isinstance(s, str) and ("收錄於專輯" in s or s.strip() == "收錄於"))
    if label and hasattr(label, "parent"):
        a = label.parent.find_next("a", href=True)
        if a:
            return clean_text(a.get_text(" ", strip=True)), abs_url(a["href"])
    return None, None

def extract_critic_review_url(soup: BeautifulSoup) -> Optional[str]:
    text = soup.get_text("\n", strip=True)
    if "達人推薦" not in text:
        return None
    header = soup.find(lambda t: getattr(t, "name", None) in ("h2", "h3") and "達人推薦" in t.get_text(" ", strip=True))
    if header:
        a = header.find_next("a", href=True)
        if a:
            return abs_url(a["href"])
    a2 = soup.find("a", href=True, string=lambda s: isinstance(s, str) and "達人推薦" in s)
    if a2:
        return abs_url(a2["href"])
    return None

def extract_song_accredited_datetime(soup: BeautifulSoup) -> Optional[str]:
    a = soup.select_one("a.js-accredited[data-accredited-datetime]")
    if not a:
        return None
    raw = a.get("data-accredited-datetime") or ""
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*(\d{1,2}):(\d{2})", raw)
    if not m:
        return None
    y, mo, d, hh, mm = map(int, m.groups())
    return f"{y:04d}-{mo:02d}-{d:02d} {hh:02d}:{mm:02d}"

def extract_release_date(page_text: str) -> Optional[str]:
    m = re.search(r"發布時間\s*(\d{4}-\d{2}-\d{2})", page_text)
    return m.group(1) if m else None

def extract_comments_count(soup: BeautifulSoup) -> Optional[int]:
    span = soup.select_one("#comment-counts")
    if span:
        v = to_int(span.get_text(strip=True))
        if v is not None:
            return v
    text = soup.get_text("\n", strip=True)
    m = re.search(r"留言（\s*(\d+)\s*）", text)
    return int(m.group(1)) if m else None

def collect_section_text(soup: BeautifulSoup, title_prefix: str) -> Optional[str]:
    h2 = soup.find(lambda t: getattr(t, "name", None) == "h2" and t.get_text(" ", strip=True).startswith(title_prefix))
    if not h2:
        return None
    parts: List[str] = []
    for node in h2.find_all_next():
        if node == h2:
            continue
        if getattr(node, "name", None) == "h2":
            break
        if getattr(node, "name", None) in ("h1", "h2", "h3") and "留言（" in node.get_text(" ", strip=True):
            break
        if getattr(node, "name", None) in ("script", "style"):
            continue
        txt = node.get_text(" ", strip=True) if hasattr(node, "get_text") else ""
        txt = txt.strip()
        if not txt or txt in ("...查看更多", "收合", "查看更多", "...查看更多 收合"):
            continue
        parts.append(txt)
    out = "\n".join(parts).strip()
    out = out.replace("...查看更多 收合", "").replace("...查看更多", "").replace("收合", "")
    return clean_text(out)

def extract_collaborators(soup: BeautifulSoup) -> Optional[str]:
    h2 = soup.find(lambda t: getattr(t, "name", None) == "h2" and t.get_text(" ", strip=True) == "合作音樂人")
    if not h2:
        return None
    names: List[str] = []
    for node in h2.find_all_next():
        if getattr(node, "name", None) == "h2" and node is not h2:
            break
        if getattr(node, "name", None) == "a" and node.get("href"):
            t = node.get_text(" ", strip=True)
            if t and t not in names:
                names.append(t)
        if len(names) >= 80:
            break
    return "、".join(names) if names else None

def extract_flags(soup: BeautifulSoup) -> Tuple[Optional[bool], Optional[bool]]:
    text = soup.get_text("\n", strip=True)
    return ("編輯推薦" in text), (("Song of the Day" in text) or ("今日之歌" in text) or ("本日之歌" in text))

def extract_honors(soup: BeautifulSoup) -> List[str]:
    """讀榮譽徽章清單本身的 DOM 結構，不預設種類，清單裡有什麼就抓什麼。"""
    honors: List[str] = []
    for h3 in soup.select("h3"):
        badge = h3.select_one(".badge, .icon-trophy")
        if not badge:
            continue
        label = h3.get_text(" ", strip=True)
        if label and label not in honors:
            honors.append(label)
    return honors

def song_id_from_url(song_url: str) -> Optional[int]:
    m = re.search(r"/songs/(\d+)/", song_url)
    return int(m.group(1)) if m else None

def api_public_song(session: requests.Session, song_id: int, song_url: str) -> Optional[dict]:
    api_url = f"{BASE}/api/v1/public/song/{song_id}/"
    headers = dict(API_HEADERS)
    headers["Referer"] = song_url
    for method in ("POST", "GET"):
        r = request_retry(session, method, api_url, headers=headers, data=(b"" if method == "POST" else None))
        if not r or r.status_code != 200:
            continue
        if "json" not in (r.headers.get("content-type") or ""):
            continue
        try:
            return r.json()
        except Exception:
            continue
    return None

def extract_next_data(html: str) -> Optional[dict]:
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>\s*(\{.*?\})\s*</script>', html, flags=re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None

def deep_find_int(obj: Any, key_substrs: List[str]) -> Optional[int]:
    if obj is None:
        return None
    best: Optional[int] = None

    def rec(cur: Any, path: str = ""):
        nonlocal best
        if isinstance(cur, dict):
            for k, v in cur.items():
                rec(v, (path + "." + k) if path else k)
        elif isinstance(cur, list):
            for i, v in enumerate(cur):
                rec(v, f"{path}[{i}]")
        else:
            kp = path.lower()
            if all(s.lower() in kp for s in key_substrs):
                iv = to_int(cur)
                if iv is not None:
                    best = iv if best is None else max(best, iv)

    rec(obj, "")
    return best

def playwright_counts_song(body_text: str) -> Tuple[Optional[int], Optional[int]]:
    head = body_text.split("發布時間", 1)[0]
    plays = None
    likes = None
    m = re.search(r"播放次數\s*([0-9,]+)", head)
    if m:
        plays = int(m.group(1).replace(",", ""))
    m = re.search(r"\b喜歡\s*([0-9,]+)\b", head)
    if m:
        likes = int(m.group(1).replace(",", ""))
    return likes, plays

def download_image(session: requests.Session, url: Optional[str], song_id: Optional[int], images_dir: str) -> Optional[str]:
    if not url or not song_id:
        return None
    ext = ".jpg"
    m = re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", url, re.I)
    if m:
        ext = "." + m.group(1).lower()
    local_path = os.path.join(images_dir, f"{song_id}{ext}")
    if os.path.exists(local_path):
        return local_path
    r = request_retry(session, "GET", url, headers=HTML_HEADERS, tries=2, timeout=20)
    if r and r.status_code == 200 and r.content:
        os.makedirs(images_dir, exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(r.content)
        return local_path
    return None

def scrape_song(session: requests.Session, song_url: str, pw_page=None, images_dir: str = "images") -> Dict[str, Any]:
    html = get_html(session, song_url)
    if not html:
        return {}

    soup = soup_of(html)
    text = soup.get_text("\n", strip=True)

    cover = None
    og = soup.select_one('meta[property="og:image"]')
    if og and og.get("content"):
        cover = og["content"]

    genre = extract_genre(soup)
    album_title, album_url = extract_album(soup)
    collaborators = extract_collaborators(soup)
    description = collect_section_text(soup, "介紹")
    lyrics = collect_section_text(soup, "歌詞")
    release_date = extract_release_date(text)
    comments_count = extract_comments_count(soup)
    song_accredited_datetime = extract_song_accredited_datetime(soup)
    is_editor, is_sotd = extract_flags(soup)
    critic_review_url = extract_critic_review_url(soup)
    honors = extract_honors(soup)

    sid = song_id_from_url(song_url)
    cover_local = download_image(session, cover, sid, images_dir)

    likes = None
    plays = None

    song_api = api_public_song(session, sid, song_url) if sid is not None else None
    next_data = extract_next_data(html)

    if song_api:
        likes = deep_find_int(song_api, ["like"]) or deep_find_int(song_api, ["favorite"])
        plays = deep_find_int(song_api, ["play"]) or deep_find_int(song_api, ["listen"])

    if likes is None and next_data:
        likes = deep_find_int(next_data, ["like"]) or deep_find_int(next_data, ["favorite"])

    if plays is None and next_data:
        plays = deep_find_int(next_data, ["play"]) or deep_find_int(next_data, ["listen"])

    if (likes is None or plays is None) and pw_page is not None:
        try:
            pw_page.goto(song_url, wait_until="domcontentloaded", timeout=25000)
            try:
                pw_page.wait_for_selector("text=播放次數", timeout=8000)
            except Exception:
                pass
            body_text = pw_page.locator("body").inner_text()
            l2, p2 = playwright_counts_song(body_text)
            likes = likes if likes is not None else l2
            plays = plays if plays is not None else p2
        except Exception:
            pass

    return {
        "cover_image_url": cover,
        "cover_image_local_path": cover_local,
        "genre": genre,
        "album_title": album_title,
        "album_url": album_url,
        "collaborators": collaborators,
        "description": description,
        "lyrics": lyrics,
        "release_date": release_date,
        "comments_count": comments_count,
        "song_accredited_datetime": song_accredited_datetime,
        "is_editor_recommended": is_editor,
        "is_song_of_the_day": is_sotd,
        "critic_review_url": critic_review_url,
        "honors": "; ".join(honors) if honors else None,
        "likes_count": likes,
        "play_count": plays,
    }


# ---- Artist ----
def parse_artist_joined_line(text: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(r"([^\n]{1,30})\s*・於\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*加入", text)
    if not m:
        return None, None
    city = m.group(1).strip()
    y = int(m.group(2))
    mo = int(m.group(3))
    return city, f"{y:04d}-{mo:02d}-01"

def parse_artist_handle_identity(text: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(r"(@[A-Za-z0-9_\.]+)\s*・\s*([^\n]{1,30})", text)
    if not m:
        return None, None
    return m.group(1).strip(), m.group(2).strip()

def parse_accredited_datetime_from_html(html: str) -> Optional[str]:
    m = re.search(r'data-accredited-datetime="([^"]+)"', html)
    if not m:
        return None
    raw = m.group(1)
    mm = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*(\d{1,2}):(\d{2})", raw)
    if not mm:
        return None
    y, mo, d, hh, mi = map(int, mm.groups())
    return f"{y:04d}-{mo:02d}-{d:02d} {hh:02d}:{mi:02d}"

def playwright_counts_artist(body_text: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    head = body_text.split("主頁", 1)[0]
    music = fans = following = None
    m = re.search(r"音樂\s*([0-9,]+)", head)
    if m: music = int(m.group(1).replace(",", ""))
    m = re.search(r"粉絲\s*([0-9,]+)", head)
    if m: fans = int(m.group(1).replace(",", ""))
    m = re.search(r"追蹤中\s*([0-9,]+)", head)
    if m: following = int(m.group(1).replace(",", ""))
    return music, fans, following

def extract_related_news(soup: BeautifulSoup) -> List[Tuple[str, str]]:
    h2 = soup.find(lambda t: getattr(t, "name", None) == "h2" and t.get_text(" ", strip=True) == "相關新聞")
    if not h2:
        return []
    news: List[Tuple[str, str]] = []
    for node in h2.find_all_next():
        if getattr(node, "name", None) == "h2" and node is not h2:
            break
        if getattr(node, "name", None) == "a" and node.get("href"):
            href = node["href"]
            if "blow.streetvoice.com" in href:
                title = node.get_text(" ", strip=True)
                if title:
                    pair = (title, href)
                    if pair not in news:
                        news.append(pair)
    return news

def extract_big_thing_appearances(soup: BeautifulSoup) -> List[Tuple[str, str, str]]:
    results: List[Tuple[str, str, str]] = []
    for a in soup.select('a[href^="/gigs/"]'):
        title = a.get_text(" ", strip=True)
        if "大團誕生" not in title and "Next Big Thing" not in title:
            continue
        href = abs_url(a["href"])
        container = a.find_parent(["div", "li"])
        date_text = ""
        if container:
            dm = re.search(r"\d{4}-\d{2}-\d{2}|\d{1,2}\s*月\s*\d{1,2}", container.get_text(" ", strip=True))
            date_text = dm.group(0) if dm else ""
        item = (title, date_text, href)
        if item not in results:
            results.append(item)
    return results

def scrape_artist(session: requests.Session, artist_url: str, pw_page=None) -> Dict[str, Any]:
    html = get_html(session, artist_url)
    if not html:
        return {}

    soup = soup_of(html)
    text = soup.get_text("\n", strip=True)

    handle, identity = parse_artist_handle_identity(text)
    city, joined = parse_artist_joined_line(text)
    accredited = parse_accredited_datetime_from_html(html)
    related_news = extract_related_news(soup)
    big_thing = extract_big_thing_appearances(soup)

    fb = ig = yt = None
    for a in soup.select('a[href*="facebook.com"], a[href*="instagram.com"], a[href*="youtube.com"], a[href*="youtu.be"]'):
        href = a.get("href") or ""
        u = href if href.startswith("http") else abs_url(href)
        if is_blacklisted_social(u):
            continue
        if ("facebook.com" in u or "m.facebook.com" in u) and fb is None:
            fb = u
        elif "instagram.com" in u and ig is None:
            ig = u.split("?")[0]
        elif ("youtube.com" in u or "youtu.be" in u) and yt is None:
            yt = u.split("?")[0]

    music = fans = following = None

    if (music is None or fans is None or following is None) and pw_page is not None:
        try:
            pw_page.goto(artist_url, wait_until="domcontentloaded", timeout=25000)
            try:
                pw_page.wait_for_selector("text=粉絲", timeout=8000)
            except Exception:
                pass
            body_text = pw_page.locator("body").inner_text()
            m2, f2, fo2 = playwright_counts_artist(body_text)
            music = m2 if m2 not in (None, 0) else music
            fans = f2 if f2 not in (None, 0) else fans
            following = fo2 if fo2 not in (None, 0) else following
        except Exception:
            pass

    return {
        "artist_handle": handle,
        "artist_identity": identity,
        "artist_city": city,
        "artist_joined_date": joined,
        "artist_accredited_datetime": accredited,
        "artist_music_count": music,
        "artist_fans_count": fans,
        "artist_following_count": following,
        "artist_facebook_url": fb,
        "artist_instagram_url": ig,
        "artist_youtube_url": yt,
        "related_news": "; ".join(f"{t}|||{u}" for t, u in related_news) if related_news else None,
        "big_thing_appearances": "; ".join(f"{t}|||{d}|||{u}" for t, d, u in big_thing) if big_thing else None,
    }


def write_csv(out_file: str, rows: List[Row]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()))
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["realtime", "weekly", "weekly-backfill"], default="realtime")
    ap.add_argument("--out-dir", default="data")
    ap.add_argument("--images-dir", default="images")
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--backfill-limit", type=int, default=20, help="回溯模式每首歌抓取上限，建議調低減少負擔")
    ap.add_argument("--max-targets", type=int, default=10, help="回溯模式單次最多處理幾個「曲風+週次」組合")
    ap.add_argument("--no-playwright", action="store_true")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs(args.images_dir, exist_ok=True)

    session = requests.Session()

    pw = browser = page = None
    if HAVE_PLAYWRIGHT and not args.no_playwright:
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({"Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7"})

        def _route(route, request):
            if request.resource_type in ("image", "media", "font"):
                return route.abort()
            return route.continue_()
        page.route("**/*", _route)

    song_cache: Dict[str, Dict[str, Any]] = {}
    artist_cache: Dict[str, Dict[str, Any]] = {}

    def get_song_extra(song_url: str) -> Dict[str, Any]:
        if song_url not in song_cache:
            song_cache[song_url] = scrape_song(session, song_url, pw_page=page, images_dir=args.images_dir)
        return song_cache[song_url]

    def get_artist_extra(artist_url: str) -> Dict[str, Any]:
        if artist_url not in artist_cache:
            artist_cache[artist_url] = scrape_artist(session, artist_url, pw_page=page)
        return artist_cache[artist_url]

    today = taipei_now().date()
    cur_y, cur_w = iso_week_info(today)

    targets: List[Tuple[str, str, Optional[int], Optional[int]]] = []
    limit = args.limit

    if args.mode == "realtime":
        targets = [("realtime", g, None, None) for g in GENRES_REALTIME]
    elif args.mode == "weekly":
        targets = [("weekly", g, cur_y, cur_w) for g in GENRES_WEEKLY]
    else:  # weekly-backfill
        limit = min(args.limit, args.backfill_limit)
        for g in GENRES_WEEKLY:
            for w in range(1, cur_w):
                out_check = os.path.join(args.out_dir, f"streetvoice_weekly_{g}_{cur_y}_{w:02d}.csv")
                if os.path.exists(out_check):
                    continue
                targets.append(("weekly", g, cur_y, w))
        targets = targets[: args.max_targets]
        print(f"[backfill] 這次會處理 {len(targets)} 個組合（還有更多要之後幾次陸續補）", flush=True)

    for timeframe, genre, year, week in targets:
        url = build_chart_url(timeframe, genre, year, week)
        label = f"{timeframe}/{genre}" + (f"/{year}-W{week}" if week else "")
        print(f"=== {label} : {url} ===", flush=True)

        chart_html = get_html(session, url)
        if not chart_html:
            print(f"[warn] 抓不到 {url}，略過", flush=True)
            continue

        chart_items = parse_chart(chart_html, limit)
        rows: List[Row] = []
        snapshot_time = snapshot_time_str()

        for rank, song_title_guess, artist_name_guess, song_url, artist_url in chart_items:
            print(f"  [{rank}/{len(chart_items)}] {song_url}", flush=True)

            song_extra = get_song_extra(song_url)
            artist_extra = get_artist_extra(artist_url) if artist_url else {}

            rows.append(Row(
                snapshot_time=snapshot_time,
                chart_timeframe=timeframe,
                chart_genre=genre,
                chart_year=year,
                chart_week=week,
                rank=rank,
                artist_name=clean_text(artist_name_guess) or "",
                song_title=clean_text(song_title_guess) or "",
                likes_count=song_extra.get("likes_count"),
                play_count=song_extra.get("play_count"),
                comments_count=song_extra.get("comments_count"),
                song_url=song_url,
                artist_url=artist_url,
                cover_image_url=song_extra.get("cover_image_url"),
                cover_image_local_path=song_extra.get("cover_image_local_path"),
                artist_handle=artist_extra.get("artist_handle"),
                artist_identity=artist_extra.get("artist_identity"),
                artist_city=artist_extra.get("artist_city"),
                artist_joined_date=artist_extra.get("artist_joined_date"),
                artist_accredited_datetime=artist_extra.get("artist_accredited_datetime"),
                artist_music_count=artist_extra.get("artist_music_count"),
                artist_fans_count=artist_extra.get("artist_fans_count"),
                artist_following_count=artist_extra.get("artist_following_count"),
                artist_facebook_url=artist_extra.get("artist_facebook_url"),
                artist_instagram_url=artist_extra.get("artist_instagram_url"),
                artist_youtube_url=artist_extra.get("artist_youtube_url"),
                related_news=artist_extra.get("related_news"),
                big_thing_appearances=artist_extra.get("big_thing_appearances"),
                genre=song_extra.get("genre"),
                album_title=song_extra.get("album_title"),
                album_url=song_extra.get("album_url"),
                collaborators=song_extra.get("collaborators"),
                description=song_extra.get("description"),
                lyrics=song_extra.get("lyrics"),
                release_date=song_extra.get("release_date"),
                song_accredited_datetime=song_extra.get("song_accredited_datetime"),
                honors=song_extra.get("honors"),
                is_editor_recommended=song_extra.get("is_editor_recommended"),
                is_song_of_the_day=song_extra.get("is_song_of_the_day"),
                critic_review_url=song_extra.get("critic_review_url"),
            ))

            time.sleep(0.15)

        if timeframe == "realtime":
            out_file = os.path.join(args.out_dir, f"streetvoice_realtime_{genre}_{filename_ts()}.csv")
        else:
            out_file = os.path.join(args.out_dir, f"streetvoice_weekly_{genre}_{year}_{week:02d}.csv")

        write_csv(out_file, rows)
        print(f"[OK] wrote {len(rows)} rows -> {out_file}", flush=True)

    if browser:
        browser.close()
    if pw:
        pw.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
