#!/usr/bin/env python3
"""
Extract locations from Jabiertzo YouTube channel livestreams and geocode them.

Uses a curated location dictionary + pattern matching on titles/descriptions,
then geocodes via Nominatim (OSM).
"""

import json
import os
import re
import time
import sys
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

INPUT_FILE = "/data/channel_raw.jsonl"
OUTPUT_FILE = "/data/videos.json"

geolocator = Nominatim(user_agent="tubemaps-jabiertzo/1.0")

_geocode_cache: dict[str, tuple[float, float] | None] = {}


def geocode(query: str) -> tuple[float, float] | None:
    if query in _geocode_cache:
        return _geocode_cache[query]
    for attempt in range(3):
        try:
            time.sleep(1.1)  # Nominatim rate limit: 1 req/s
            location = geolocator.geocode(query, timeout=10)
            if location:
                result = (location.latitude, location.longitude)
                _geocode_cache[query] = result
                return result
            _geocode_cache[query] = None
            return None
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"  Geocode retry {attempt+1} for '{query}': {e}", file=sys.stderr)
            time.sleep(2)
    _geocode_cache[query] = None
    return None


# ---------------------------------------------------------------------------
# Curated location dictionary for the Jabiertzo channel livestreams.
# Keys are matched (case-insensitive) against title+description.
# Values are Nominatim geocoding queries.
# Order matters: more specific entries should be checked first.
# ---------------------------------------------------------------------------

LOCATION_KEYWORDS: list[tuple[str, str]] = [
    # Specific places / landmarks
    ("HUAGUOYUAN", "Huaguoyuan, Guiyang, Guizhou, China"),
    ("MONTAÑAS AVATAR", "Zhangjiajie, Hunan, China"),
    ("MONTAÑAS DRAGON BALL", "Zhangjiajie, Hunan, China"),
    ("DRAGON BALL", "Zhangjiajie, Hunan, China"),
    ("GUERREROS DE TERRACOTA", "Xi'an, Shaanxi, China"),
    ("GRAN MURALLA", "Shanhaiguan, Hebei, China"),
    ("MONTAÑA TIANMEN", "Zhangjiajie, Hunan, China"),
    ("TRES GARGANTAS", "Yichang, Hubei, China"),
    ("PRESA DE LAS TRES", "Yichang, Hubei, China"),
    ("VOLCÁN CHANGBAI", "Changbaishan, Jilin, China"),
    ("RÍO XIANG", "Changsha, Hunan, China"),
    ("RÍO AMARILLO", "Lanzhou, Gansu, China"),
    ("MONTE YUELU", "Changsha, Hunan, China"),
    ("ISLA DE MAO", "Changsha, Hunan, China"),
    ("CONCESIÓN FRANCESA", "Shanghai, China"),
    ("EL BUND", "Shanghai, China"),
    ("BARRIO ALEMÁN", "Qingdao, Shandong, China"),
    ("ZONA 798", "Beijing, China"),
    ("TEMPLO DEL CIELO", "Beijing, China"),
    ("ÚLTIMO EMPERADOR", "Changchun, Jilin, China"),
    ("TIGRES SIBERIANOS", "Harbin, Heilongjiang, China"),
    ("PUEBLO DE LELE", "Dangyang, Hubei, China"),
    ("PUEBLO DRAGON BALL", "Zhangjiajie, Hunan, China"),
    ("MALUXI", "Maluxi, Hunan, China"),
    ("MARRUECOS", "Morocco"),

    # Cities from livestream titles (alphabetical)
    ("ANHUA", "Anhua, Hunan, China"),
    ("CHANGCHUN", "Changchun, Jilin, China"),
    ("CHANGSHA", "Changsha, Hunan, China"),
    ("CHENGDU", "Chengdu, Sichuan, China"),
    ("CHENZHOU", "Chenzhou, Hunan, China"),
    ("CHIBI", "Chibi, Hubei, China"),
    ("CHONGQING", "Chongqing, China"),
    ("DALIAN", "Dalian, Liaoning, China"),
    ("DANDONG", "Dandong, Liaoning, China"),
    ("DANGYANG", "Dangyang, Hubei, China"),
    ("DATONG", "Datong, Shanxi, China"),
    ("DONGGUAN", "Dongguan, Guangdong, China"),
    ("DONGXING", "Dongxing, Guangxi, China"),
    ("FENGHUANG", "Fenghuang, Hunan, China"),
    ("FOSHAN", "Foshan, Guangdong, China"),
    ("FUZHOU", "Fuzhou, Fujian, China"),
    ("FURONG", "Furong, 芙蓉镇, Yongshun, Hunan, China"),
    ("GUANGZHOU", "Guangzhou, Guangdong, China"),
    ("GUIYANG", "Guiyang, Guizhou, China"),
    ("HANGZHOU", "Hangzhou, Zhejiang, China"),
    ("HARBIN", "Harbin, Heilongjiang, China"),
    ("HEFEI", "Hefei, Anhui, China"),
    ("HOHHOT", "Hohhot, Inner Mongolia, China"),
    ("HONG KONG", "Hong Kong"),
    ("HUIZHOU", "Huizhou, Guangdong, China"),
    ("HUZHOU", "Huzhou, Zhejiang, China"),
    ("JIAN'OU", "Jianou, Fujian, China"),
    ("JINGMEN", "Jingmen, Hubei, China"),
    ("JINGZHOU", "Jingzhou, Hubei, China"),
    ("KUNMING", "Kunming, Yunnan, China"),
    ("LANZHOU", "Lanzhou, Gansu, China"),
    ("LESHAN", "Leshan, Sichuan, China"),
    ("LIJIANG", "Lijiang, Yunnan, China"),
    ("LINXIANG", "Linxiang, Hunan, China"),
    ("MACAO", "Macao"),
    ("MACAU", "Macao"),
    ("NANCHANG", "Nanchang, Jiangxi, China"),
    ("NANJING", "Nanjing, Jiangsu, China"),
    ("NANNING", "Nanning, Guangxi, China"),
    ("NANXUN", "Nanxun, Zhejiang, China"),
    ("NINGDE", "Ningde, Fujian, China"),
    ("ORDOS", "Ordos, Inner Mongolia, China"),
    ("PEKÍN", "Beijing, China"),
    ("PEKIN", "Beijing, China"),
    ("BEIJING", "Beijing, China"),
    ("PUTIAN", "Putian, Fujian, China"),
    ("QINGDAO", "Qingdao, Shandong, China"),
    ("QINGYUAN", "Qingyuan, Guangdong, China"),
    ("QIQIHAR", "Qiqihar, Heilongjiang, China"),
    ("QUANZHOU", "Quanzhou, Fujian, China"),
    ("SANYA", "Sanya, Hainan, China"),
    ("SHAOGUAN", "Shaoguan, Guangdong, China"),
    ("SHANGHAI", "Shanghai, China"),
    ("SHANWEI", "Shanwei, Guangdong, China"),
    ("SHANTOU", "Shantou, Guangdong, China"),
    ("SHENYANG", "Shenyang, Liaoning, China"),
    ("SHENZHEN", "Shenzhen, Guangdong, China"),
    ("SUQIAN", "Suqian, Jiangsu, China"),
    ("SUZHOU", "Suzhou, Jiangsu, China"),
    ("TAIYUAN", "Taiyuan, Shanxi, China"),
    ("TIANJIN", "Tianjin, China"),
    ("TONG CHENG", "Tongcheng, Anhui, China"),
    ("WUHAN", "Wuhan, Hubei, China"),
    ("XIAMEN", "Xiamen, Fujian, China"),
    ("XI'AN", "Xi'an, Shaanxi, China"),
    ("YANJI", "Yanji, Jilin, China"),
    ("YAN'AN", "Yan'an, Shaanxi, China"),
    ("YANGSHUO", "Yangshuo, Guangxi, China"),
    ("YANGZHOU", "Yangzhou, Jiangsu, China"),
    ("YICHANG", "Yichang, Hubei, China"),
    ("YINCHUAN", "Yinchuan, Ningxia, China"),
    ("YIYANG", "Yiyang, Hunan, China"),
    ("YIZHANG", "Yizhang, Hunan, China"),
    ("YOUXIAN", "Youxian, Hunan, China"),
    ("ZHANGJIAJIE", "Zhangjiajie, Hunan, China"),
    ("ZHANGZHOU", "Zhangzhou, Fujian, China"),
    ("ZHENGZHOU", "Zhengzhou, Henan, China"),
    ("ZHUHAI", "Zhuhai, Guangdong, China"),
    ("ZHUZHOU", "Zhuzhou, Hunan, China"),

    # Provinces / regions (lower priority fallback)
    ("CANTÓN", "Guangzhou, Guangdong, China"),
    ("GUANGDONG", "Guangdong, China"),
    ("FUJIAN", "Fuzhou, Fujian, China"),
    ("GUIZHOU", "Guiyang, Guizhou, China"),
    ("HAINAN", "Haikou, Hainan, China"),
    ("HENAN", "Zhengzhou, Henan, China"),
    ("HUBEI", "Wuhan, Hubei, China"),
    ("HUNAN", "Changsha, Hunan, China"),
    ("JIANGSU", "Nanjing, Jiangsu, China"),
    ("JIANGXI", "Nanchang, Jiangxi, China"),
    ("JILIN", "Jilin, China"),
    ("GANSU", "Lanzhou, Gansu, China"),
    ("MONGOLIA INTERIOR", "Hohhot, Inner Mongolia, China"),
    ("NINGXIA", "Yinchuan, Ningxia, China"),
    ("QINGHAI", "Xining, Qinghai, China"),
    ("SHAANXI", "Xi'an, Shaanxi, China"),
    ("SHANDONG", "Jinan, Shandong, China"),
    ("SHANXI", "Taiyuan, Shanxi, China"),
    ("SICHUAN", "Chengdu, Sichuan, China"),
    ("XINJIANG", "Urumqi, Xinjiang, China"),
    ("YUNNAN", "Kunming, Yunnan, China"),
    ("ZHEJIANG", "Hangzhou, Zhejiang, China"),
    ("ANHUI", "Hefei, Anhui, China"),
    ("GUANGXI", "Nanning, Guangxi, China"),
    ("HEBEI", "Shijiazhuang, Hebei, China"),
    ("TÍBET", "Lhasa, Tibet, China"),
    ("TIBET", "Lhasa, Tibet, China"),
    ("YANBIAN", "Yanbian, Jilin, China"),
    ("HEILONGJIANG", "Harbin, Heilongjiang, China"),
    ("LIAONING", "Shenyang, Liaoning, China"),

    # Generic hints from description context
    ("NORESTE", "Shenyang, Liaoning, China"),
    ("COREA DEL NORTE", "Dandong, Liaoning, China"),
]

# Short analysis/talk streams (not IRL) - skip these
SKIP_KEYWORDS_IN_TITLE = [
    "ARANCELES", "SANCIONES", "GUERRA COMERCIAL",
    "TIERRAS RARAS", "RESERVAS DE CHINA",
    "PETRÓLEO", "INFLACIÓN", "TECNOLOGÍA CHINA",
    "IRÁN", "JIANG XUEQIN",
    "AUTOCARAVANAS CHINAS DE SEGUNDA",
    "AUTOCARAVANA CHINA HÍBRIDA",
    "OCCIDENTE YA NO ENTIENDE",
    "CONSEJOS, PREGUNTAS Y RESPUESTAS",
]


def get_best_thumbnail(thumbnails: list[dict]) -> str | None:
    if not thumbnails:
        return None
    best = None
    best_w = 0
    for t in thumbnails:
        w = t.get("width", 0) or 0
        if 200 <= w <= 500:
            if w > best_w:
                best = t["url"]
                best_w = w
    if best:
        return best
    for t in sorted(thumbnails, key=lambda x: x.get("width", 0) or 0, reverse=True):
        if t.get("url"):
            return t["url"]
    return thumbnails[0].get("url") if thumbnails else None


def extract_location(title: str, description: str) -> str | None:
    title_upper = title.upper()
    desc_upper = (description or "").upper()

    for skip in SKIP_KEYWORDS_IN_TITLE:
        if skip in title_upper:
            return None

    # Check title first, then description
    for keyword, query in LOCATION_KEYWORDS:
        if keyword in title_upper:
            return query

    for keyword, query in LOCATION_KEYWORDS:
        if keyword in desc_upper:
            return query

    return None


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}", file=sys.stderr)
        sys.exit(1)

    videos = []
    with open(INPUT_FILE) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            videos.append(json.loads(line))

    print(f"Processing {len(videos)} streams...")

    results = []
    geocoded_locations: dict[str, tuple[float, float] | None] = {}
    skipped = 0
    no_location = 0

    for i, video in enumerate(videos):
        vid = video.get("id", "")
        title = video.get("title", "")
        desc = video.get("description", "") or ""
        thumbnails = video.get("thumbnails", [])

        # Clean title: remove suffixes like "| Jabiertzo", "| Jabiertzo en directo"
        clean_title = re.sub(
            r"\s*\|?\s*Jabiertzo\s*(en directo)?(\s*IRL)?\s*$", "",
            title, flags=re.IGNORECASE
        ).strip()
        # Also remove standalone suffixes
        clean_title = re.sub(
            r"\s*\|\s*Jabiertzo.*$", "", clean_title, flags=re.IGNORECASE
        ).strip()
        clean_title = re.sub(r"\s*#shortslive\s*$", "", clean_title, flags=re.IGNORECASE).strip()

        location_query = extract_location(title, desc)

        if not location_query:
            no_location += 1
            continue

        if location_query not in geocoded_locations:
            print(f"  [{i+1}/{len(videos)}] Geocoding: {location_query}")
            coords = geocode(location_query)
            geocoded_locations[location_query] = coords
        else:
            coords = geocoded_locations[location_query]

        if not coords:
            print(f"  WARNING: Could not geocode '{location_query}' for: {clean_title}")
            skipped += 1
            continue

        lat, lon = coords
        thumb = get_best_thumbnail(thumbnails)
        if not thumb:
            thumb = f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"

        # Extract date: try upload_date, then release_date, then fallback
        upload_date = video.get("upload_date") or ""
        if len(upload_date) == 8:
            date_str = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"
        else:
            date_str = ""

        view_count = video.get("view_count") or 0
        playlist_idx = video.get("playlist_autonumber") or video.get("playlist_index") or 9999

        results.append({
            "id": vid,
            "title": clean_title,
            "url": f"https://www.youtube.com/watch?v={vid}",
            "thumbnail": thumb,
            "lat": lat,
            "lon": lon,
            "location": location_query.split(",")[0],
            "views": view_count,
            "date": date_str,
            "order": playlist_idx,
        })

    # Sort by playlist order (most recent first)
    results.sort(key=lambda x: x["order"])

    output = {
        "channel": "Jabiertzo",
        "channel_url": "https://www.youtube.com/c/Jabiertzo",
        "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_videos": len(videos),
        "mapped_videos": len(results),
        "videos": results,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nDone! {len(results)} streams mapped, {no_location} without location, {skipped} geocode failures")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
