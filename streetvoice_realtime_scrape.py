#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # py<3.9 fallback not needed on GH Actions py3.11


BASE = "https://streetvoice.com"
CHART_URL = "https://streetvoice.com/music/charts/realtime/all/"


DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}


@dataclass
class Row:
    scraped_at: str

    rank: Optional[int]
    song_title: str
    artist_name: str
    likes_count: Optional[int]
    play_count: Optional[int]

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
    comments_count: Optional[int]
    is_editorial_pick: Optional[bool]
    is_song_of_the_day: Optional[bool]
    critic_review_url: str  # only if "達人推薦" exists on song page


def now_taipei() -> datetime:
    if ZoneInfo is None:
        return datetime.utcnow()
    return datetime.now(ZoneInfo("Asia/Taipei"))


def iso_dt_taipei() -> str:
    return now_taipei().strftime("%Y-%m-%d %H:%M:%S")


def clean_int(s: str) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"(\d[\d,]*)", s)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def normalize_lines(soup: BeautifulSoup) -> List[str]:
    text = soup.get_text("\n", strip=True)
    lines = []
    for raw in text.splitlines():
        line = re.sub(r"\s+", " ", raw).strip()
        if line:
            lines.append(line)
    return lines


def extract_block(lines: List[str], start_pat: str, stop_pats: List[str]) -> Tuple[Optional[str], List[str]]:
    started = False
    out_lines: List[str] = []
    for line in lines:
        if not started:
            if re.search(start_pat, line):
                started = True
            continue
        if any(re.search(p, line) for p in stop_pats):
            break
        out_lines.append(line)
    text = "\n".join(out_lines).strip()
    return (text if text else None), out_lines


def pick_og_image(soup: BeautifulSoup) -> str:
    # fallback for cover
    tag = soup.find("meta", attrs={"property": "og:image"})
    if tag and tag.get("content"):
        return tag["content"].strip()
    tag = soup.find("meta", attrs={"name": "twitter:image"})
    if tag and tag.get("content"):
        return tag["content"].strip()
    return ""


def fetch_html(session: requests.Session, url: str) -> BeautifulSoup:
    r = session.get(url, headers={**DEFAULT_HEADERS, "Referer": BASE}, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def try_json(resp: requests.Response) -> Optional[Dict]:
    try:
        return resp.json()
    except Exception:
        return None


def fetch_song_json(session: requests.Session, song_id: int, referer: str) -> Optional[Dict]:
    # Some environments prefer POST with empty body (as older extractors did), but we try GET first.
    api = f"{BASE}/api/v1/public/song/{song_id}/"
    headers = {**DEFAULT_HEADERS, "Accept": "application/json", "Referer": referer}

    # 1) GET
    try:
        r = session.get(api, headers=headers, timeout=30)
        if r.ok:
            data = try_json(r)
            if isinstance(data, dict) and data:
                return data
    except Exception:
        pass

    # 2) POST empty
    try:
        r = session.post(api, headers=headers, data=b"", timeout=30)
        if r.ok:
            data = try_json(r)
            if isinstance(data, dict) and data:
                return data
    except Exception:
        pass

    return None


def fetch_user_json(session: requests.Session, user_id: int, referer: str) -> Optional[Dict]:
    headers = {**DEFAULT_HEADERS, "Accept": "application/json", "Referer": referer}
    candidates = [
        f"{BASE}/api/v1/public/user/{user_id}/",
        f"{BASE}/api/v1/public/users/{user_id}/",
    ]
    for url in candidates:
        try:
            r = session.get(url, headers=headers, timeout=30)
            if not r.ok:
                continue
            data = try_json(r)
            if isinstance(data, dict) and data:
                return data
        except Exception:
            continue
    return None


def parse_chart(session: requests.Session, limit: Optional[int]) -> List[Tuple[int, str, str]]:
    """
    Return list of (rank, song_url, artist_url) from realtime chart.
    """
    soup = fetch_html(session, CHART_URL)

    # Strategy: find all song links that match "/<artist>/songs/<id>/"
    # Then derive rank by looking backward for nearest "#### <number>" in text flow.
    anchors = soup.find_all("a", href=True)
    items: List[Tuple[int, str, str]] = []

    for a in anchors:
        href = a["href"].strip()
        m = re.match(r"^/([^/]+)/songs/(\d+)/?$", href)
        if not m:
            continue
        song_url = urljoin(BASE, href)
        artist_slug = m.group(1)
        artist_url = urljoin(BASE, f"/{artist_slug}/")

        # Find rank nearby: walk parents' text and look for a number line
        rank = None
        parent = a
        for _ in range(6):
            if parent is None:
                break
            txt = parent.get_text("\n", strip=True)
            mm = re.search(r"^\s*(\d{1,3})\s*$", txt, flags=re.M)
            if mm:
                rank = int(mm.group(1))
                break
            parent = parent.parent

        # fallback: rank from surrounding strings
        if rank is None:
            # search previous siblings text
            prev_text = a.find_previous(string=re.compile(r"^\s*\d{1,3}\s*$"))
            if prev_text:
                try:
                    rank = int(prev_text.strip())
                except Exception:
                    rank = None

        if rank is None:
            continue

        items.append((rank, song_url, artist_url))

    # de-dup by rank (chart sometimes repeats entries when SSR is odd)
    dedup: Dict[int, Tuple[int, str, str]] = {}
    for r, s, aurl in items:
        dedup.setdefault(r, (r, s, aurl))

    out = [dedup[k] for k in sorted(dedup.keys())]
    if limit:
        out = out[:limit]
    return out


def parse_artist_page(soup: BeautifulSoup) -> Dict:
    lines = normalize_lines(soup)

    # handle + identity: "@Cliff949・音樂人"
    handle = ""
    identity = ""
    city = ""
    joined_date = ""
    accredited_dt = ""

    for line in lines:
        m = re.search(r"(@[A-Za-z0-9_\.]+)\s*・\s*([^・]+)", line)
        if m and not handle:
            handle = m.group(1)
            identity = m.group(2).strip()
        m2 = re.search(r"^([^・]+)\s*・\s*於\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*加入", line)
        if m2 and not city:
            city = m2.group(1).strip()
            y = int(m2.group(2)); mo = int(m2.group(3))
            # use ISO date format; month precision -> YYYY-MM-01
            joined_date = f"{y:04d}-{mo:02d}-01"

    # accredited datetime attribute: data-accredited-datetime="2021 年 4 月 6 日 17:27"
    acc = soup.find(attrs={"data-accredited-datetime": True})
    if acc:
        raw = acc.get("data-accredited-datetime", "").strip()
        m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*(\d{1,2}):(\d{2})", raw)
        if m:
            y, mo, d, hh, mm = map(int, m.groups())
            accredited_dt = f"{y:04d}-{mo:02d}-{d:02d} {hh:02d}:{mm:02d}:00"

    # counts from HTML (fallback)
    def find_count(label: str) -> Optional[int]:
        # pattern: "#### 音樂" then next line is number
        for i, line in enumerate(lines):
            if re.fullmatch(rf"{label}", line):
                if i + 1 < len(lines):
                    return clean_int(lines[i + 1])
        # alternative: direct "音樂 123"
        for line in lines:
            if label in line:
                val = clean_int(line)
                if val is not None:
                    return val
        return None

    music_count = find_count("音樂")
    fans_count = find_count("粉絲")
    following_count = find_count("追蹤中")

    # socials
    fb = ig = yt = ""
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "facebook.com" in href or "m.facebook.com" in href:
            fb = fb or href
        elif "instagram.com" in href:
            ig = ig or href
        elif "youtube.com" in href or "youtu.be" in href:
            yt = yt or href

    # artist name
    artist_name = ""
    h1 = soup.find(["h1", "h2"])
    if h1:
        artist_name = h1.get_text(strip=True)

    return {
        "artist_name": artist_name,
        "artist_handle": handle,
        "artist_identity": identity,
        "artist_city": city,
        "artist_joined_date": joined_date,
        "artist_accredited_datetime": accredited_dt,
        "artist_music_count": music_count,
        "artist_fans_count": fans_count,
        "artist_following_count": following_count,
        "artist_facebook_url": fb,
        "artist_instagram_url": ig,
        "artist_youtube_url": yt,
    }


def parse_song_page(session: requests.Session, song_url: str, artist_url: str) -> Dict:
    soup = fetch_html(session, song_url)
    lines = normalize_lines(soup)
    full_text = "\n".join(lines)

    # title
    title = ""
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)

    # genre (often a link near title)
    genre = ""
    # try find a line that looks like a genre label (e.g., "Singer / Songwriter")
    for line in lines[:80]:
        if re.search(r"/", line) and len(line) <= 40 and "http" not in line and "StreetVoice" not in line:
            # weak heuristic; keep first
            genre = genre or line

    # published date: "發布時間 2026-01-29"
    published = ""
    for line in lines:
        m = re.search(r"發布時間\s*(\d{4}-\d{2}-\d{2})", line)
        if m:
            published = m.group(1)
            break

    # comments count: "留言（14）"
    comments_count = None
    for line in lines:
        m = re.search(r"留言（(\d+)）", line)
        if m:
            comments_count = int(m.group(1))
            break

    # editorial pick / SOTD
    is_editorial = ("編輯推薦" in full_text)
    is_sotd = ("Song of the Day" in full_text) or ("今日歌曲" in full_text) or ("每日歌曲" in full_text)

    # critic review url: ONLY if song page contains "達人推薦"
    critic_review_url = ""
    if "達人推薦" in full_text:
        # prefer an explicit link if present
        a = soup.find("a", string=re.compile(r"達人推薦"))
        if a and a.get("href"):
            critic_review_url = urljoin(BASE, a["href"].strip())
        else:
            critic_review_url = urljoin(song_url if song_url.endswith("/") else song_url + "/", "critic_reviews/")

    # collaborators: between "合作音樂人" and "介紹/歌詞/留言"
    collab_text, collab_lines = extract_block(
        lines,
        start_pat=r"^合作音樂人$",
        stop_pats=[r"^介紹$", r"^歌詞", r"^留言（", r"最相關留言"]
    )
    collaborators = ""
    if collab_lines:
        names: List[str] = []
        for l in collab_lines:
            if l in ("查看更多 收合", "…查看完整內容", "收起來"):
                continue
            if l.startswith("#") or l.startswith("##"):
                continue
            # strip bullets
            l2 = re.sub(r"^[•\-\*]+\s*", "", l).strip()
            if not l2:
                continue
            # avoid separators
            if l2 in ("*", "* * *"):
                continue
            names.append(l2)
        # de-dup preserve order
        seen = set()
        uniq = []
        for n in names:
            if n not in seen:
                seen.add(n)
                uniq.append(n)
        collaborators = "; ".join(uniq)

    # description: between "介紹" and "歌詞/留言"
    description, _ = extract_block(
        lines,
        start_pat=r"^介紹$",
        stop_pats=[r"^歌詞", r"^留言（", r"最相關留言"]
    )
    description = description or ""

    # lyrics: between "歌詞" and "留言"
    lyrics, _ = extract_block(
        lines,
        start_pat=r"^歌詞",
        stop_pats=[r"^留言（", r"最相關留言"]
    )
    lyrics = lyrics or ""

    # album title: try from visible lines
    album_title = ""
    for i, line in enumerate(lines):
        if "收錄於" in line:
            m = re.search(r"收錄於(?:專輯)?\s*(.+)$", line)
            if m and m.group(1).strip():
                album_title = m.group(1).strip()
                break
            # else maybe next line holds the title
            if i + 1 < len(lines) and lines[i + 1].strip():
                album_title = lines[i + 1].strip()
                break

    # likes/play from HTML (fallback)
    html_play = None
    html_like = None
    for i, line in enumerate(lines):
        if line == "播放次數" and i + 1 < len(lines):
            html_play = clean_int(lines[i + 1])
        if line == "喜歡" and i + 1 < len(lines):
            html_like = clean_int(lines[i + 1])

    cover = pick_og_image(soup)

    return {
        "song_title_html": title,
        "genre": genre,
        "published_date": published,
        "comments_count": comments_count,
        "is_editorial_pick": is_editorial,
        "is_song_of_the_day": is_sotd,
        "critic_review_url": critic_review_url,
        "collaborators": collaborators,
        "description": description,
        "lyrics": lyrics,
        "album_title_html": album_title,
        "cover_image_url_html": cover,
        "likes_count_html": html_like,
        "play_count_html": html_play,
    }


def build_row(session: requests.Session, rank: int, song_url: str, artist_url: str, sleep_s: float) -> Row:
    # parse song id
    m = re.search(r"/songs/(\d+)/?$", song_url)
    song_id = int(m.group(1)) if m else None

    song_page = parse_song_page(session, song_url, artist_url)

    # API (song)
    song_json = fetch_song_json(session, song_id, referer=song_url) if song_id else None
    api_title = ""
    api_likes = None
    api_plays = None
    api_cover = ""
    api_user_id = None
    api_album_title = ""

    if isinstance(song_json, dict):
        api_title = (song_json.get("name") or song_json.get("title") or "") if isinstance(song_json, dict) else ""
        # counts
        for key in ("plays_count", "play_count", "listen_count", "plays"):
            if key in song_json:
                api_plays = clean_int(str(song_json.get(key)))
                break
        for key in ("likes_count", "like_count", "likes"):
            if key in song_json:
                api_likes = clean_int(str(song_json.get(key)))
                break

        # cover
        for key in ("cover", "image", "picture", "cover_image", "cover_image_url"):
            v = song_json.get(key)
            if isinstance(v, str) and v:
                api_cover = v
                break
            if isinstance(v, dict):
                # common nested keys
                for k2 in ("url", "original", "large", "medium"):
                    vv = v.get(k2)
                    if isinstance(vv, str) and vv:
                        api_cover = vv
                        break
                if api_cover:
                    break

        # user id
        user = song_json.get("user") if isinstance(song_json.get("user"), dict) else None
        if user:
            api_user_id = user.get("id")

        # album title
        album = song_json.get("album")
        if isinstance(album, dict):
            api_album_title = (album.get("title") or album.get("name") or "").strip()
        else:
            api_album_title = (song_json.get("album_title") or "").strip()

    # artist page (HTML)
    artist_soup = fetch_html(session, artist_url)
    artist_info = parse_artist_page(artist_soup)

    # artist API fallback (counts more reliable if available)
    user_json = fetch_user_json(session, int(api_user_id), referer=artist_url) if api_user_id else None
    if isinstance(user_json, dict):
        # Try to overwrite counts if present
        for k, out_key in [
            ("songs_count", "artist_music_count"),
            ("music_count", "artist_music_count"),
            ("fans_count", "artist_fans_count"),
            ("followers_count", "artist_fans_count"),
            ("following_count", "artist_following_count"),
            ("followings_count", "artist_following_count"),
        ]:
            if k in user_json and artist_info.get(out_key) in (None, 0):
                artist_info[out_key] = clean_int(str(user_json.get(k)))

        # artist name fallback
        if not artist_info.get("artist_name"):
            for k in ("nickname", "name", "display_name"):
                if user_json.get(k):
                    artist_info["artist_name"] = str(user_json.get(k)).strip()
                    break

        # handle fallback
        if not artist_info.get("artist_handle"):
            h = user_json.get("handle") or user_json.get("username")
            if h:
                artist_info["artist_handle"] = "@" + str(h).lstrip("@")

    # final field resolution (API -> HTML -> empty)
    song_title = (api_title or song_page["song_title_html"] or "").strip()
    if not song_title:
        song_title = ""  # keep as empty string, not None

    artist_name = (artist_info.get("artist_name") or "").strip()

    likes_count = api_likes if api_likes is not None else song_page.get("likes_count_html")
    play_count = api_plays if api_plays is not None else song_page.get("play_count_html")

    cover = (api_cover or song_page.get("cover_image_url_html") or "").strip()

    album_title = (api_album_title or song_page.get("album_title_html") or "").strip()

    # genre
    genre = (song_page.get("genre") or "").strip()

    # enforce requirement: critic_review_url empty unless "達人推薦" exists (already done)
    critic_review_url = (song_page.get("critic_review_url") or "").strip()

    # rate limit
    if sleep_s > 0:
        time.sleep(sleep_s)

    return Row(
        scraped_at=iso_dt_taipei(),

        rank=rank,
        song_title=song_title,
        artist_name=artist_name,
        likes_count=likes_count,
        play_count=play_count,

        song_url=song_url,
        artist_url=artist_url,
        cover_image_url=cover,

        artist_handle=(artist_info.get("artist_handle") or ""),
        artist_identity=(artist_info.get("artist_identity") or ""),
        artist_city=(artist_info.get("artist_city") or ""),
        artist_joined_date=(artist_info.get("artist_joined_date") or ""),
        artist_accredited_datetime=(artist_info.get("artist_accredited_datetime") or ""),
        artist_music_count=artist_info.get("artist_music_count"),
        artist_fans_count=artist_info.get("artist_fans_count"),
        artist_following_count=artist_info.get("artist_following_count"),
        artist_facebook_url=(artist_info.get("artist_facebook_url") or ""),
        artist_instagram_url=(artist_info.get("artist_instagram_url") or ""),
        artist_youtube_url=(artist_info.get("artist_youtube_url") or ""),

        genre=genre,
        album_title=album_title,
        collaborators=(song_page.get("collaborators") or ""),
        description=(song_page.get("description") or ""),
        lyrics=(song_page.get("lyrics") or ""),
        published_date=(song_page.get("published_date") or ""),
        comments_count=song_page.get("comments_count"),
        is_editorial_pick=song_page.get("is_editorial_pick"),
        is_song_of_the_day=song_page.get("is_song_of_the_day"),
        critic_review_url=critic_review_url,
    )


def write_csv(path: str, rows: List[Row]) -> None:
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
    ap.add_argument("--sleep", type=float, default=0.6)
    args = ap.parse_args()

    import os
    os.makedirs(args.out_dir, exist_ok=True)

    tpe = now_taipei()
    out_file = f"streetvoice_realtime_all_{tpe:%Y-%m-%d_%H%M}.csv"
    out_path = os.path.join(args.out_dir, out_file)

    session = requests.Session()

    chart = parse_chart(session, limit=args.limit)

    rows: List[Row] = []
    for rank, song_url, artist_url in chart:
        try:
            row = build_row(session, rank, song_url, artist_url, sleep_s=args.sleep)
            rows.append(row)
        except Exception as e:
            # keep going; you can add a debug log file if needed
            print(f"[WARN] rank={rank} failed: {e}")

    write_csv(out_path, rows)
    print(f"[OK] wrote {len(rows)} rows -> {out_path}")


if __name__ == "__main__":
    main()
