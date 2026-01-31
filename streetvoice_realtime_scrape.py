import os
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup


BASE = "https://streetvoice.com"
CHART_URL = "https://streetvoice.com/music/charts/realtime/all/"
OUT_DIR = "data"

# 小心別打太快；你也可以調大
REQUEST_DELAY_SEC = 0.35
TIMEOUT_SEC = 30

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
}


def parse_zh_datetime(s: str):
    """
    例：2021 年 4 月 6 日 17:27 -> 2021-04-06 17:27
       ：2014 年 10 月 -> 2014-10-01
    回傳 (date_str, datetime_str) 其中一個可能為 None
    """
    if not s:
        return (None, None)

    s = s.strip()
    # YYYY 年 M 月 D 日 HH:MM
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日(?:\s*(\d{1,2}):(\d{2}))?", s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        hh = m.group(4)
        mm = m.group(5)
        date_str = f"{y:04d}-{mo:02d}-{d:02d}"
        if hh and mm:
            dt_str = f"{date_str} {int(hh):02d}:{int(mm):02d}"
            return (date_str, dt_str)
        return (date_str, None)

    # YYYY 年 M 月（只有年月）
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月", s)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        date_str = f"{y:04d}-{mo:02d}-01"
        return (date_str, None)

    return (None, None)


def clean_int(s: str):
    if s is None:
        return None
    m = re.search(r"[\d,]+", str(s))
    if not m:
        return None
    return int(m.group(0).replace(",", ""))


def http_get(session: requests.Session, url: str):
    r = session.get(url, headers=HEADERS, timeout=TIMEOUT_SEC)
    r.raise_for_status()
    return r.text


def http_get_soft(session: requests.Session, url: str):
    try:
        r = session.get(url, headers=HEADERS, timeout=TIMEOUT_SEC)
        if r.status_code >= 400:
            return None
        return r.text
    except Exception:
        return None


def http_post_json_soft(session: requests.Session, url: str):
    """
    StreetVoice 的 public song API 在既有實作中是用 POST 空 body 取 JSON。
    若 GET 也可用，這段也會自動 fallback。
    """
    try:
        r = session.post(url, headers=HEADERS, data=b"", timeout=TIMEOUT_SEC)
        if r.status_code == 405:
            r = session.get(url, headers=HEADERS, timeout=TIMEOUT_SEC)
        if r.status_code >= 400:
            return None
        return r.json()
    except Exception:
        return None


def parse_chart_song_links(chart_html: str):
    """
    從即時榜頁抓出 songs/<id> 連結，依出現順序當作 rank。
    回傳 list of dict: {rank, song_url, artist_handle_guess, song_id, cover_image_url_guess}
    """
    soup = BeautifulSoup(chart_html, "lxml")
    anchors = soup.find_all("a", href=True)

    seen = set()
    items = []

    for a in anchors:
        href = a.get("href", "")
        # 只抓 /<handle>/songs/<id>/ 這種
        m = re.match(r"^/([^/]+)/songs/(\d+)/", href)
        if not m:
            continue

        handle = m.group(1)
        song_id = m.group(2)

        # 避免 footer / 其他區塊重複
        key = (handle, song_id)
        if key in seen:
            continue

        text_title = a.get_text(strip=True)
        # 真正的歌曲 title anchor 通常有文字；沒文字的多半是圖片或空連結
        if not text_title:
            # 仍保留，但先不加入 seen，讓後面有文字的優先
            continue

        seen.add(key)
        song_url = urljoin(BASE, href)

        # 嘗試在同一個卡片/列表項裡找封面圖
        cover_url = None
        container = a.find_parent(["li", "div", "article"]) or a.parent
        if container:
            img = container.find("img")
            if img and img.get("src"):
                cover_url = img.get("src")
            else:
                # 有些封面是 background-image
                style = container.get("style") if hasattr(container, "get") else None
                if style:
                    mm = re.search(r'url\(["\']?(.*?)["\']?\)', style)
                    if mm:
                        cover_url = mm.group(1)

        items.append(
            {
                "song_id": song_id,
                "artist_handle_guess": handle,
                "song_url": song_url,
                "cover_image_url_guess": cover_url,
            }
        )

    # rank 依序編號
    for i, it in enumerate(items, start=1):
        it["rank"] = i

    return items


def fetch_song_api(session: requests.Session, song_id: str):
    api_url = f"{BASE}/api/v1/public/song/{song_id}/"
    data = http_post_json_soft(session, api_url)
    return data or {}


def pick_first(d: dict, keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def parse_song_page(song_html: str):
    soup = BeautifulSoup(song_html, "lxml")

    full_text = soup.get_text("\n", strip=True)

    # 播放次數 / 喜歡數：頁面文字可能有 0（前端占位），因此後面會優先用 API 回填 likes/play
    play_count = None
    m = re.search(r"([\d,]+)\s*播放次數", full_text)
    if m:
        play_count = clean_int(m.group(1))

    comments_count = None
    m = re.search(r"留言（\s*(\d+)\s*）", full_text)
    if m:
        comments_count = int(m.group(1))

    # 發布時間：常見格式 YYYY-MM-DD 或中文年月日
    release_date = None
    m = re.search(r"發布時間\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", full_text)
    if m:
        release_date = m.group(1)
    else:
        m = re.search(r"發布時間\s*([0-9]{4}\s*年\s*[0-9]{1,2}\s*月\s*[0-9]{1,2}\s*日)", full_text)
        if m:
            release_date, _ = parse_zh_datetime(m.group(1))

    # 介紹 / 歌詞：用標題區塊去抓
    def grab_section(title_zh: str):
        header = soup.find(lambda tag: tag.name in ["h2", "h3", "h4"] and tag.get_text(strip=True) == title_zh)
        if not header:
            return None

        parts = []
        for sib in header.find_all_next():
            if sib == header:
                continue
            if sib.name in ["h2", "h3", "h4"] and sib.get_text(strip=True) in ["介紹", "歌詞", "留言", "合作音樂人", "相關統計"]:
                break
            # 避免把整頁都抓進來
            if sib.name in ["script", "style"]:
                continue
            txt = sib.get_text(" ", strip=True)
            if txt:
                parts.append(txt)
            if len(" ".join(parts)) > 6000:
                break

        out = " ".join(parts).strip()
        return out if out else None

    description = grab_section("介紹")
    lyrics = grab_section("歌詞")

    # 合作音樂人
    collaborators = []
    collab_header = soup.find(lambda tag: tag.name in ["h2", "h3", "h4"] and tag.get_text(strip=True) == "合作音樂人")
    if collab_header:
        for a in collab_header.find_all_next("a", href=True):
            # 遇到下一個大區塊就停
            if a.find_parent(lambda t: t.name in ["h2", "h3", "h4"] and t.get_text(strip=True) in ["介紹", "歌詞", "留言"]):
                break
            name = a.get_text(strip=True)
            if name and name not in collaborators:
                collaborators.append(name)
            if len(collaborators) >= 30:
                break

    # 類型（genre）：用保守方式掃文字
    genre = None
    # 有些頁面會出現英文類型字串（如 Singer / Songwriter）
    # 你可依你的需求再加強規則（例如抓某個固定區塊）
    m = re.search(r"\n([A-Za-z][A-Za-z0-9 /&\-]{2,40})\n.*播放次數", full_text)
    if m:
        genre = m.group(1).strip()

    # 收錄於專輯
    album_title = None
    m = re.search(r"收錄於\s*([^\n]{1,80})", full_text)
    if m:
        album_title = m.group(1).strip()

    # 編輯推薦 / Song of the Day（只要頁面出現關鍵字就算）
    is_editor_pick = 1 if ("編輯推薦" in full_text) else 0
    is_song_of_the_day = 1 if ("Song of the Day" in full_text) else 0

    return {
        "play_count_page": play_count,
        "comments_count": comments_count,
        "release_date": release_date,
        "description": description,
        "lyrics": lyrics,
        "collaborators": ";".join(collaborators) if collaborators else None,
        "genre": genre,
        "album_title": album_title,
        "is_editor_pick": is_editor_pick,
        "is_song_of_the_day": is_song_of_the_day,
    }


def parse_artist_page(artist_html: str):
    soup = BeautifulSoup(artist_html, "lxml")
    full_text = soup.get_text("\n", strip=True)
    raw_html = artist_html

    # 例：@Cliff949・音樂人
    artist_handle = None
    artist_identity = None
    m = re.search(r"(@[A-Za-z0-9_\.]+)・([^\n]{1,20})", full_text)
    if m:
        artist_handle = m.group(1)
        artist_identity = m.group(2).strip()

    # 例：新北市・於 2014 年 10 月 加入
    artist_city = None
    join_date = None
    m = re.search(r"([^\n]{1,20})・於\s*([0-9]{4}\s*年\s*[0-9]{1,2}\s*月)\s*加入", full_text)
    if m:
        artist_city = m.group(1).strip()
        join_date, _ = parse_zh_datetime(m.group(2))

    # data-accredited-datetime="2021 年 4 月 6 日 17:27"
    accredited_dt = None
    m = re.search(r'data-accredited-datetime="([^"]+)"', raw_html)
    if m:
        _, dt_str = parse_zh_datetime(m.group(1))
        accredited_dt = dt_str

    # 社群連結
    fb = ig = yt = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not fb and "facebook.com" in href:
            fb = href
        if not ig and "instagram.com" in href:
            ig = href
        if not yt and ("youtube.com" in href or "youtu.be" in href):
            yt = href

    # 音樂 / 粉絲 / 追蹤中 數字：
    # 1) 先嘗試從 HTML 原始碼抓 JSON 形式的計數欄位
    def find_count_from_html(patterns):
        for pat in patterns:
            mm = re.search(pat, raw_html)
            if mm:
                v = clean_int(mm.group(1))
                if v is not None:
                    return v
        return None

    music_count = find_count_from_html([
        r'"music_count"\s*:\s*([0-9,]+)',
        r'"songs_count"\s*:\s*([0-9,]+)',
        r'"tracks_count"\s*:\s*([0-9,]+)',
    ])
    fans_count = find_count_from_html([
        r'"fans_count"\s*:\s*([0-9,]+)',
        r'"followers_count"\s*:\s*([0-9,]+)',
    ])
    following_count = find_count_from_html([
        r'"following_count"\s*:\s*([0-9,]+)',
        r'"followings_count"\s*:\s*([0-9,]+)',
    ])

    # 2) 如果 JSON 抓不到，再退而求其次：掃頁面文字（可能會拿到 0，占位）
    def find_count_from_text(label):
        mm = re.search(label + r"\s*[\n\r ]*([0-9,]+)", full_text)
        return clean_int(mm.group(1)) if mm else None

    if music_count is None:
        music_count = find_count_from_text("音樂")
    if fans_count is None:
        fans_count = find_count_from_text("粉絲")
    if following_count is None:
        following_count = find_count_from_text("追蹤中")

    return {
        "artist_handle": artist_handle,
        "artist_identity": artist_identity,
        "artist_city": artist_city,
        "artist_join_date": join_date,
        "artist_music_count": music_count,
        "artist_fans_count": fans_count,
        "artist_following_count": following_count,
        "artist_facebook_url": fb,
        "artist_instagram_url": ig,
        "artist_youtube_url": yt,
        "artist_accredited_datetime": accredited_dt,
    }


def detect_critic_reviews(session: requests.Session, song_url: str):
    # 範例路徑：/songs/<id>/critic_reviews/（:contentReference[oaicite:4]{index=4}）
    critic_url = song_url.rstrip("/") + "/critic_reviews/"
    html = http_get_soft(session, critic_url)
    if not html:
        return None
    txt = BeautifulSoup(html, "lxml").get_text("\n", strip=True)
    # 頁面若真的有內容，通常會出現「達人推薦」
    if "達人推薦" in txt:
        return critic_url
    return None


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    tz = ZoneInfo("Asia/Taipei")
    now = datetime.now(tz)
    snapshot_at = now.isoformat(timespec="seconds")
    month_key = now.strftime("%Y-%m")

    latest_path = os.path.join(OUT_DIR, "streetvoice_realtime_all_latest.csv")
    monthly_path = os.path.join(OUT_DIR, f"streetvoice_realtime_all_{month_key}.csv")

    session = requests.Session()

    chart_html = http_get(session, CHART_URL)
    chart_items = parse_chart_song_links(chart_html)

    rows = []

    for it in chart_items:
        song_id = it["song_id"]
        song_url = it["song_url"]
        rank = it["rank"]
        handle_guess = it["artist_handle_guess"]

        time.sleep(REQUEST_DELAY_SEC)

        # 1) Song API（用來補 likes / 發布時間 / 封面 / 藝人名等）
        song_api = fetch_song_api(session, song_id)

        song_title = pick_first(song_api, ["name", "title"]) or None
        image_url = pick_first(song_api, ["image", "cover", "thumbnail"]) or it["cover_image_url_guess"]

        # likes / play count 欄位名稱可能會變；多試幾種
        likes_count = clean_int(pick_first(song_api, ["likes", "like_count", "favorite_count", "favorites_count"]))
        play_count_api = clean_int(pick_first(song_api, ["play_count", "plays", "played_count"]))

        created_at = pick_first(song_api, ["created_at", "createdAt", "publish_at", "published_at"])
        release_date_api = None
        if isinstance(created_at, str):
            # 有些是 ISO，如 2026-01-29T...
            m = re.match(r"(\d{4}-\d{2}-\d{2})", created_at)
            if m:
                release_date_api = m.group(1)

        user = song_api.get("user") or {}
        artist_name = user.get("nickname") or user.get("name") or None
        artist_handle_api = user.get("username") or user.get("handle") or None

        # 2) 歌曲頁（補介紹/歌詞/留言/合作/編輯推薦/達人推薦等）
        time.sleep(REQUEST_DELAY_SEC)
        song_html = http_get_soft(session, song_url) or ""
        song_page = parse_song_page(song_html) if song_html else {}

        # 3) 藝人頁（補身分/城市/加入時間/社群連結/認證時間等）
        #    優先用 API 拿到的 handle，沒有就用 chart 的 guess
        artist_slug = artist_handle_api or handle_guess
        artist_url = urljoin(BASE, f"/{artist_slug}/")

        time.sleep(REQUEST_DELAY_SEC)
        artist_html = http_get_soft(session, artist_url) or ""
        artist_page = parse_artist_page(artist_html) if artist_html else {}

        time.sleep(REQUEST_DELAY_SEC)
        critic_url = detect_critic_reviews(session, song_url)

        row = {
            "snapshot_at": snapshot_at,
            "rank": rank,
            "song_id": song_id,
            "song_title": song_title,
            "artist_name": artist_name,
            "likes_count": likes_count,
            "song_url": song_url,
            "artist_url": artist_url,
            "cover_image_url": image_url,

            # artist fields
            **artist_page,

            # song fields
            "play_count": play_count_api if play_count_api is not None else song_page.get("play_count_page"),
            "description": song_page.get("description"),
            "lyrics": song_page.get("lyrics"),
            "release_date": release_date_api or song_page.get("release_date"),
            "collaborators": song_page.get("collaborators"),
            "genre": song_page.get("genre"),
            "album_title": song_page.get("album_title"),
            "comments_count": song_page.get("comments_count"),
            "is_editor_pick": song_page.get("is_editor_pick", 0),
            "is_song_of_the_day": song_page.get("is_song_of_the_day", 0),
            "critic_reviews_url": critic_url,
        }

        # 如果 artist_handle 沒抓到，至少用 slug 補一個 @
        if not row.get("artist_handle") and artist_slug:
            row["artist_handle"] = "@" + artist_slug

        rows.append(row)

    df = pd.DataFrame(rows)

    # Excel 友善：utf-8-sig
    df.to_csv(latest_path, index=False, encoding="utf-8-sig")

    # --- 每月累積（不想要歷史檔就把這段註解掉）---
    write_header = not os.path.exists(monthly_path)
    df.to_csv(monthly_path, mode="a", index=False, header=write_header, encoding="utf-8-sig")

    print(f"Wrote: {latest_path}")
    print(f"Appended: {monthly_path}")


if __name__ == "__main__":
    main()
