#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
StreetVoice realtime chart scraper (All)

Fixes requested:
1) critic_review_url: ONLY set when song page contains "達人推薦" (otherwise blank)
2) album_title: improved extraction (API + HTML fallbacks)
3) collaborators: extracted ONLY from "合作音樂人" section (won't leak into comments / "最相關留言")
4) lyrics/description: extracted by section boundaries, stops before "留言"
5) likes_count / cover_image_url / artist counts / play_count: API-first, then Next.js JSON, then regex, then visible text
6) output filenames include scrape time (Asia/Taipei), accumulate hourly snapshots

Notes:
- Tries StreetVoice public JSON endpoint /api/v1/public/song/<id>/ with POST data=b'' (API-first strategy).
"""

import argparse
import csv
import json
import os
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # GH Actions py3.11 should have it


BASE = "https://streetvoice.com"
CHART_URL = "https://streetvoice.com/music/charts/realtime/all/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

API_HEADERS = {
    **HEADERS,
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": BASE,
}

STOP_UI_PHRASES = {
    "...查看更多 收合",
    "...查看更多",
    "收合",
    "收起來",
    "查看更多",
}


@dataclass
class Row:
    scraped_at: str
    rank: int

    artist_name: str
    song_title: str

    likes_count: Optional[int]
    play_count: Optional[int]
    comments_count: Optional[int]

    song_url: str
    artist_url: str
    cover_image_url: str

    # artist page
    artist_handle: str
    artist_identity: str
    artist_city: str
    artist_joined_date: str  # YYYY-MM-01
    artist_accredited_datetime: str  # YYYY-MM-DD HH:MM:SS
    artist_music_count: Optional[int]
    artist_fans_count: Optional[int]
    artist_following_count: Optional[int]
    artist_facebook_url: str
    artist_instagram_url: str
    artist_youtube_url: str

    # song page
    genre: str
    album_title: str
    collaborators: str
    description: str
    lyrics: str
    published_date: str  # YYYY-MM-DD
    is_editorial_pick: bool
    is_song_of_the_day: bool
    critic_review_url: str


def tz_now_taipei() -> datetime:
    if ZoneInfo is None:
        return datetime.utcnow()
    return datetime.now(ZoneInfo("Asia/Taipei"))


def fmt_scraped_at(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def clean_int_from_text(s: str) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"(\d[\d,]*)", s)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except Exception:
        return None


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def fetch_html(session: requests.Session, url: str) -> str:
    r = session.get(url, headers={**HEADERS, "Referer": BASE}, timeout=35)
    r.raise_for_status()
    return r.text


def post_json(session: requests.Session, url: str, data: bytes = b"") -> Optional[Dict[str, Any]]:
    try:
        r = session.post(url, headers=API_HEADERS, data=data, timeout=35)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def soup_of(html: str) -> BeautifulSoup:
    # Prefer lxml for speed; fall back to built-in parser if lxml isn't installed.
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


def pick_og_image(soup: BeautifulSoup) -> str:
    for attrs in (
        {"property": "og:image"},
        {"name": "twitter:image"},
    ):
        tag = soup.find("meta", attrs=attrs)
        if tag and tag.get("content"):
            return tag["content"].strip()
    return ""


def extract_next_data(raw_html: str) -> Optional[Dict[str, Any]]:
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', raw_html, flags=re.S)
    if not m:
        return None
    blob = (m.group(1) or "").strip()
    if not blob:
        return None
    try:
        return json.loads(blob)
    except Exception:
        return None


def deep_find_values(obj: Any, keys: Iterable[str], max_hits: int = 120) -> List[Any]:
    keys_set = set(keys)
    hits: List[Any] = []

    def rec(x: Any):
        nonlocal hits
        if len(hits) >= max_hits:
            return
        if isinstance(x, dict):
            for k, v in x.items():
                if k in keys_set:
                    hits.append(v)
                    if len(hits) >= max_hits:
                        return
                rec(v)
        elif isinstance(x, list):
            for v in x:
                rec(v)

    rec(obj)
    return hits


def best_int_candidate(cands: Iterable[Any], min_value: int = 0) -> Optional[int]:
    vals: List[int] = []
    for v in cands:
        if v is None or isinstance(v, bool):
            continue
        if isinstance(v, (int, float)):
            try:
                iv = int(v)
                if iv >= min_value:
                    vals.append(iv)
            except Exception:
                pass
        elif isinstance(v, str):
            iv = clean_int_from_text(v)
            if iv is not None and iv >= min_value:
                vals.append(iv)
    return max(vals) if vals else None


def regex_int_from_html(raw_html: str, patterns: List[str]) -> Optional[int]:
    for pat in patterns:
        m = re.search(pat, raw_html, flags=re.S)
        if m:
            iv = clean_int_from_text(m.group(1))
            if iv is not None:
                return iv
    return None


def parse_chart_items(chart_html: str, limit: int) -> List[Tuple[int, str, str]]:
    """
    Return list of (rank, song_url, artist_url) ordered as in the realtime chart.
    """
    soup = soup_of(chart_html)
    anchors = soup.find_all("a", href=True)

    items: List[Tuple[str, str]] = []
    seen = set()

    for a in anchors:
        href = a["href"].strip()
        m = re.match(r"^/([^/]+)/songs/(\d+)/?$", href)
        if not m:
            continue
        artist_slug = m.group(1)
        song_id = m.group(2)
        key = (artist_slug, song_id)
        if key in seen:
            continue

        # Only accept anchors that actually contain song title text
        title_txt = a.get_text(strip=True)
        if not title_txt:
            continue

        seen.add(key)
        items.append((href, artist_slug))
        if len(items) >= limit:
            break

    out: List[Tuple[int, str, str]] = []
    for i, (href, artist_slug) in enumerate(items, start=1):
        song_url = urljoin(BASE, href)
        artist_url = urljoin(BASE, f"/{artist_slug}/")
        out.append((i, song_url, artist_url))
    return out


def heading_tag_matches(tag: Any, pattern: re.Pattern) -> bool:
    if not tag or not getattr(tag, "name", None):
        return False
    if tag.name not in ("h2", "h3", "h4"):
        return False
    txt = tag.get_text(" ", strip=True)
    return bool(pattern.search(txt))


def collect_section_text(
    soup: BeautifulSoup,
    start_heading_re: str,
    stop_heading_res: List[str],
    max_chars: int = 12000,
) -> str:
    """
    Collect text after a heading (h2/h3/h4) until a stop heading is met.
    This prevents leaking into comments section.
    """
    start_pat = re.compile(start_heading_re)
    stop_pats = [re.compile(x) for x in stop_heading_res]

    start = soup.find(lambda t: heading_tag_matches(t, start_pat))
    if not start:
        return ""

    parts: List[str] = []
    for node in start.find_all_next():
        # stop at next heading
        if getattr(node, "name", None) in ("h2", "h3", "h4") and node is not start:
            txt = node.get_text(" ", strip=True)
            if any(p.search(txt) for p in stop_pats):
                break
            if txt.startswith("留言"):
                break

        if getattr(node, "name", None) in ("script", "style"):
            continue

        txt = node.get_text(" ", strip=True) if hasattr(node, "get_text") else ""
        txt = normalize_ws(txt)
        if not txt:
            continue
        if txt in STOP_UI_PHRASES:
            continue

        parts.append(txt)
        if sum(len(p) for p in parts) >= max_chars:
            break

    # Deduplicate consecutive repeats
    cleaned: List[str] = []
    last = None
    for t in parts:
        if t == last:
            continue
        cleaned.append(t)
        last = t

    out = "\n".join(cleaned).strip()
    for p in STOP_UI_PHRASES:
        out = out.replace(p, "")
    return out.strip()


def parse_ymd(text: str) -> str:
    """
    Convert 'YYYY 年 M 月 D 日' to 'YYYY-MM-DD' if possible.
    """
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", text)
    if not m:
        return ""
    y, mo, d = map(int, m.groups())
    return f"{y:04d}-{mo:02d}-{d:02d}"


def song_id_from_url(song_url: str) -> Optional[str]:
    m = re.search(r"/songs/(\d+)", song_url)
    return m.group(1) if m else None


def parse_song_page(session: requests.Session, song_url: str) -> Dict[str, Any]:
    raw = fetch_html(session, song_url)
    soup = soup_of(raw)
    page_text = soup.get_text("\n", strip=True)

    # --- API first (most reliable for counts + album) ---
    api = None
    sid = song_id_from_url(song_url)
    if sid:
        api = post_json(session, f"{BASE}/api/v1/public/song/{sid}/", data=b"")

    # Title / artist
    song_title = ""
    artist_name = ""

    if isinstance(api, dict):
        song_title = normalize_ws(str(api.get("name") or ""))
        user = api.get("user") or {}
        if isinstance(user, dict):
            artist_name = normalize_ws(str(user.get("nickname") or user.get("name") or ""))
    if not song_title:
        h1 = soup.find("h1")
        if h1:
            song_title = h1.get_text(" ", strip=True)
    if not artist_name:
        a_artist = soup.find("a", href=re.compile(r"^/[^/]+/?$"))
        if a_artist:
            artist_name = a_artist.get_text(" ", strip=True)

    # Genre
    genre = ""
    if isinstance(api, dict) and api.get("genre"):
        genre = normalize_ws(str(api.get("genre")))
    if not genre:
        a_genre = soup.find("a", href=re.compile(r"^/music/genres/|^/genres/"))
        if a_genre:
            genre = a_genre.get_text(" ", strip=True)

    cover_image_url = pick_og_image(soup)

    # published date
    published_date = ""
    if isinstance(api, dict):
        for k in ("published_at", "released_at", "created_at", "created"):
            if api.get(k):
                published_date = parse_ymd(str(api.get(k))) or str(api.get(k))[:10]
                break
    if not published_date:
        m = re.search(r"發布時間\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", page_text)
        if m:
            published_date = m.group(1)

    # comments count (your hint: <span id="comment-counts">34</span>)
    comments_count = None
    if isinstance(api, dict):
        comments_count = best_int_candidate(
            [
                api.get("comment_count"),
                api.get("comments_count"),
                api.get("comment_counts"),
            ],
            min_value=0,
        )
    if comments_count is None:
        cc = soup.select_one("#comment-counts")
        if cc:
            comments_count = clean_int_from_text(cc.get_text(strip=True))
    if comments_count is None:
        m = re.search(r"留言（\s*(\d+)\s*）", page_text)
        if m:
            comments_count = int(m.group(1))

    # likes / play (API -> NEXT_DATA -> regex -> visible)
    likes_count = None
    play_count = None
    if isinstance(api, dict):
        likes_count = best_int_candidate(
            [
                api.get("likes_count"),
                api.get("like_count"),
                api.get("likes"),
                api.get("favorite_count"),
                api.get("favorites_count"),
                api.get("favourite_count"),
            ],
            min_value=0,
        )
        play_count = best_int_candidate(
            [
                api.get("play_count"),
                api.get("plays_count"),
                api.get("plays"),
                api.get("listen_count"),
                api.get("played_count"),
            ],
            min_value=0,
        )

    next_data = extract_next_data(raw) or {}
    if likes_count is None:
        likes_count = best_int_candidate(
            deep_find_values(next_data, ["likes_count", "like_count", "likes", "favorite_count", "favorites_count", "favourite_count"]),
            min_value=0,
        )
    if play_count is None:
        play_count = best_int_candidate(
            deep_find_values(next_data, ["play_count", "plays_count", "plays", "listen_count", "played_count"]),
            min_value=0,
        )

    if likes_count is None:
        likes_count = regex_int_from_html(raw, [
            r'"likes_count"\s*:\s*([0-9,]+)',
            r'"like_count"\s*:\s*([0-9,]+)',
            r'"favorite_count"\s*:\s*([0-9,]+)',
            r'"favorites_count"\s*:\s*([0-9,]+)',
            r'"likes"\s*:\s*([0-9,]+)',
        ])
    if play_count is None:
        play_count = regex_int_from_html(raw, [
            r'"play_count"\s*:\s*([0-9,]+)',
            r'"plays_count"\s*:\s*([0-9,]+)',
            r'"listen_count"\s*:\s*([0-9,]+)',
            r'"plays"\s*:\s*([0-9,]+)',
        ])

    # editorial pick / song of the day
    is_editorial_pick = ("編輯推薦" in page_text)
    is_song_of_the_day = ("Song of the Day" in page_text) or ("今日歌曲" in page_text) or ("每日歌曲" in page_text)

    # collaborators: ONLY within "合作音樂人" section
    collaborators: List[str] = []
    start = soup.find(lambda t: heading_tag_matches(t, re.compile(r"^合作音樂人$")))
    if start:
        for node in start.find_all_next():
            if getattr(node, "name", None) in ("h2", "h3", "h4") and node is not start:
                break
            if getattr(node, "name", None) == "a" and node.get("href"):
                name = node.get_text(" ", strip=True)
                if name and name not in collaborators:
                    collaborators.append(name)
            if len(collaborators) >= 60:
                break
    collaborators_text = "; ".join(collaborators)

    # description / lyrics: stop before "留言"
    description = collect_section_text(
        soup,
        start_heading_re=r"^介紹$",
        stop_heading_res=[r"^歌詞", r"^留言", r"^相信你也會喜歡$"],
    )
    lyrics = collect_section_text(
        soup,
        start_heading_re=r"^歌詞",
        stop_heading_res=[r"^留言", r"^相信你也會喜歡$"],
    )

    # album_title: API -> HTML -> regex -> text fallback
    album_title = ""
    if isinstance(api, dict):
        # common shapes
        if isinstance(api.get("album"), dict):
            album_title = normalize_ws(str(api["album"].get("name") or api["album"].get("title") or ""))
        if not album_title and isinstance(api.get("albums"), list) and api["albums"]:
            # sometimes it's a list
            first = api["albums"][0]
            if isinstance(first, dict):
                album_title = normalize_ws(str(first.get("name") or first.get("title") or ""))
        if not album_title and api.get("album_title"):
            album_title = normalize_ws(str(api.get("album_title")))
    if not album_title:
        a_album = soup.select_one('a[href*="/albums/"], a[href*="/album/"]')
        if a_album:
            album_title = a_album.get_text(" ", strip=True)
    if not album_title:
        for pat in [
            r'"album_title"\s*:\s*"([^"]{1,200})"',
            r'"album"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]{1,200})"',
            r'"album"\s*:\s*\{[^}]*"title"\s*:\s*"([^"]{1,200})"',
        ]:
            m = re.search(pat, raw, flags=re.S)
            if m:
                album_title = m.group(1).strip()
                break
    if not album_title:
        mm = re.search(r"收錄於(?:專輯)?\s*([^\n]{1,120})", page_text)
        if mm:
            album_title = mm.group(1).strip()

    # critic review url: ONLY if "達人推薦" appears on this song page
    critic_review_url = ""
    if "達人推薦" in page_text:
        # Prefer a real link in the block if present
        a = soup.find("a", href=True, string=re.compile(r"達人推薦"))
        if a and a.get("href"):
            critic_review_url = urljoin(BASE, a["href"].strip())
        else:
            # last resort: known path (works when the tab exists)
            critic_review_url = urljoin(song_url if song_url.endswith("/") else song_url + "/", "critic_reviews/")

    return {
        "song_title": song_title,
        "artist_name": artist_name,
        "genre": genre,
        "cover_image_url": cover_image_url,
        "published_date": published_date,
        "likes_count": likes_count,
        "play_count": play_count,
        "comments_count": comments_count,
        "is_editorial_pick": is_editorial_pick,
        "is_song_of_the_day": is_song_of_the_day,
        "collaborators": collaborators_text,
        "description": description,
        "lyrics": lyrics,
        "album_title": album_title,
        "critic_review_url": critic_review_url,
        "api": api or {},
        "raw_html": raw,
    }


def parse_artist_page(session: requests.Session, artist_url: str, song_api: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    raw = fetch_html(session, artist_url)
    soup = soup_of(raw)
    page_text = soup.get_text("\n", strip=True)

    artist_name = ""
    h1 = soup.find("h1")
    if h1:
        artist_name = h1.get_text(" ", strip=True)

    # handle + identity line like "@Cliff949・音樂人"
    artist_handle = ""
    artist_identity = ""
    m = re.search(r"(@[A-Za-z0-9_\.]+)\s*・\s*([^\n]{1,30})", page_text)
    if m:
        artist_handle = m.group(1).strip()
        artist_identity = m.group(2).strip()

    # city + joined "新北市・於 2014 年 10 月 加入"
    artist_city = ""
    joined_date = ""
    m = re.search(r"([^\n]{1,30})\s*・於\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*加入", page_text)
    if m:
        artist_city = m.group(1).strip()
        y, mo = int(m.group(2)), int(m.group(3))
        joined_date = f"{y:04d}-{mo:02d}-01"

    # accredited datetime data attribute
    accredited_dt = ""
    m = re.search(r'data-accredited-datetime="([^"]+)"', raw)
    if m:
        s = m.group(1)
        mm = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*(\d{1,2}):(\d{2})", s)
        if mm:
            y, mo, d, hh, mi = map(int, mm.groups())
            accredited_dt = f"{y:04d}-{mo:02d}-{d:02d} {hh:02d}:{mi:02d}:00"

    # socials
    fb = ig = yt = ""
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not fb and ("facebook.com" in href or "m.facebook.com" in href):
            fb = href
        if not ig and "instagram.com" in href:
            ig = href
        if not yt and ("youtube.com" in href or "youtu.be" in href):
            yt = href

    # counts: user fields from song_api -> NEXT_DATA -> regex -> visible labels
    music = fans = following = None

    if isinstance(song_api, dict):
        user = song_api.get("user") or {}
        if isinstance(user, dict):
            music = best_int_candidate([user.get("music_count"), user.get("songs_count"), user.get("tracks_count")], min_value=0)
            fans = best_int_candidate([user.get("fans_count"), user.get("followers_count")], min_value=0)
            following = best_int_candidate([user.get("following_count"), user.get("followings_count")], min_value=0)

    next_data = extract_next_data(raw) or {}
    if music is None:
        music = best_int_candidate(deep_find_values(next_data, ["music_count", "songs_count", "tracks_count"]), min_value=0)
    if fans is None:
        fans = best_int_candidate(deep_find_values(next_data, ["fans_count", "followers_count"]), min_value=0)
    if following is None:
        following = best_int_candidate(deep_find_values(next_data, ["following_count", "followings_count"]), min_value=0)

    if music is None:
        music = regex_int_from_html(raw, [
            r'"music_count"\s*:\s*([0-9,]+)',
            r'"songs_count"\s*:\s*([0-9,]+)',
            r'"tracks_count"\s*:\s*([0-9,]+)',
        ])
    if fans is None:
        fans = regex_int_from_html(raw, [
            r'"fans_count"\s*:\s*([0-9,]+)',
            r'"followers_count"\s*:\s*([0-9,]+)',
        ])
    if following is None:
        following = regex_int_from_html(raw, [
            r'"following_count"\s*:\s*([0-9,]+)',
            r'"followings_count"\s*:\s*([0-9,]+)',
        ])

    def count_after_label(label: str) -> Optional[int]:
        lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
        for i, ln in enumerate(lines):
            if ln == label and i + 1 < len(lines):
                v = clean_int_from_text(lines[i + 1])
                if v is not None:
                    return v
        return None

    if music is None:
        music = count_after_label("音樂")
    if fans is None:
        fans = count_after_label("粉絲")
    if following is None:
        following = count_after_label("追蹤中")

    return {
        "artist_name": artist_name,
        "artist_handle": artist_handle,
        "artist_identity": artist_identity,
        "artist_city": artist_city,
        "artist_joined_date": joined_date,
        "artist_accredited_datetime": accredited_dt,
        "artist_music_count": music,
        "artist_fans_count": fans,
        "artist_following_count": following,
        "artist_facebook_url": fb,
        "artist_instagram_url": ig,
        "artist_youtube_url": yt,
        "raw_html": raw,
    }


def write_csv(path: str, rows: List[Row]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        return
    fieldnames = list(asdict(rows[0]).keys())
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data")
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--sleep", type=float, default=0.5)
    ap.add_argument("--write-latest", action="store_true", help="also write/overwrite streetvoice_realtime_all_latest.csv")
    args = ap.parse_args()

    dt = tz_now_taipei()
    scraped_at = fmt_scraped_at(dt)
    out_name = f"streetvoice_realtime_all_{dt:%Y-%m-%d_%H%M}.csv"
    out_path = os.path.join(args.out_dir, out_name)

    session = requests.Session()

    chart_html = fetch_html(session, CHART_URL)
    chart_items = parse_chart_items(chart_html, limit=args.limit)

    rows: List[Row] = []
    for rank, song_url, artist_url in chart_items:
        try:
            song = parse_song_page(session, song_url)
            time.sleep(args.sleep)

            artist = parse_artist_page(session, artist_url, song_api=song.get("api") or {})
            time.sleep(args.sleep)

            artist_name = normalize_ws(artist.get("artist_name") or song.get("artist_name") or "")
            song_title = normalize_ws(song.get("song_title") or "")

            row = Row(
                scraped_at=scraped_at,
                rank=rank,
                artist_name=artist_name,
                song_title=song_title,
                likes_count=song.get("likes_count"),
                play_count=song.get("play_count"),
                comments_count=song.get("comments_count"),
                song_url=song_url,
                artist_url=artist_url,
                cover_image_url=song.get("cover_image_url") or "",
                artist_handle=artist.get("artist_handle") or "",
                artist_identity=artist.get("artist_identity") or "",
                artist_city=artist.get("artist_city") or "",
                artist_joined_date=artist.get("artist_joined_date") or "",
                artist_accredited_datetime=artist.get("artist_accredited_datetime") or "",
                artist_music_count=artist.get("artist_music_count"),
                artist_fans_count=artist.get("artist_fans_count"),
                artist_following_count=artist.get("artist_following_count"),
                artist_facebook_url=artist.get("artist_facebook_url") or "",
                artist_instagram_url=artist.get("artist_instagram_url") or "",
                artist_youtube_url=artist.get("artist_youtube_url") or "",
                genre=normalize_ws(song.get("genre") or ""),
                album_title=normalize_ws(song.get("album_title") or ""),
                collaborators=normalize_ws(song.get("collaborators") or ""),
                description=song.get("description") or "",
                lyrics=song.get("lyrics") or "",
                published_date=song.get("published_date") or "",
                is_editorial_pick=bool(song.get("is_editorial_pick")),
                is_song_of_the_day=bool(song.get("is_song_of_the_day")),
                critic_review_url=song.get("critic_review_url") or "",
            )
            rows.append(row)
        except Exception as e:
            print(f"[WARN] rank={rank} url={song_url} failed: {e}")

    write_csv(out_path, rows)
    print(f"[OK] wrote {len(rows)} rows -> {out_path}")

    if args.write_latest:
        latest_path = os.path.join(args.out_dir, "streetvoice_realtime_all_latest.csv")
        write_csv(latest_path, rows)
        print(f"[OK] wrote latest -> {latest_path}")


if __name__ == "__main__":
    main()
