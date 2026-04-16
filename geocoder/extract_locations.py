#!/usr/bin/env python3
"""
Extract locations from Jabiertzo YouTube channel videos and geocode them.

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

# Cache geocoding results to avoid repeated API calls
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
# Curated location dictionary for the Jabiertzo channel.
# Keys are matched (case-insensitive) against title+description.
# Values are Nominatim geocoding queries.
# Order matters: more specific entries should be checked first.
# ---------------------------------------------------------------------------

LOCATION_KEYWORDS: list[tuple[str, str]] = [
    # Specific places / landmarks
    ("HUAGUOYUAN", "Huaguoyuan, Guiyang, Guizhou, China"),
    ("BAISHIZHOU", "Baishizhou, Shenzhen, China"),
    ("JINGJINJI", "Beijing, China"),
    ("QINGTIAN", "Qingtian, Zhejiang, China"),
    ("FENGHUANG", "Fenghuang, Hunan, China"),
    ("ZHANGJIAJIE", "Zhangjiajie, Hunan, China"),
    ("YANGSHUO", "Yangshuo, Guangxi, China"),
    ("PINGYAO", "Pingyao, Shanxi, China"),
    ("LESHAN", "Leshan, Sichuan, China"),
    ("PANZHIHUA", "Panzhihua, Sichuan, China"),
    ("LIJIANG", "Lijiang, Yunnan, China"),
    ("XIAHE", "Xiahe, Gansu, China"),
    ("ZHANGYE", "Zhangye, Gansu, China"),
    ("YONGTAI", "Yongtai, Gansu, China"),
    ("ZHENYUAN", "Zhenyuan, Guizhou, China"),
    ("XIJIANG", "Xijiang, Guizhou, China"),
    ("DU'AN", "Du'an, Guangxi, China"),
    ("MINGSHI", "Mingshi, Guangxi, China"),
    ("DONGXING", "Dongxing, Guangxi, China"),
    ("BEIHAI", "Beihai, Guangxi, China"),
    ("CHENGYANG", "Chengyang, Guangxi, China"),
    ("QIANYANG", "Qianyang, Hunan, China"),
    ("BIANCHENG", "Biancheng, Hunan, China"),
    ("FURONG", "Furong, Hunan, China"),
    ("SHAOXING", "Shaoxing, Zhejiang, China"),
    ("MEIZHOU", "Meizhou, Guangdong, China"),
    ("NANJIE", "Nanjie, Henan, China"),
    ("TURPAN", "Turpan, Xinjiang, China"),
    ("TURPÁN", "Turpan, Xinjiang, China"),
    ("URUMCHI", "Urumqi, Xinjiang, China"),
    ("URUMQI", "Urumqi, Xinjiang, China"),
    ("HAMI", "Hami, Xinjiang, China"),
    ("BALIKUN", "Barkol, Xinjiang, China"),
    ("BARKOL", "Barkol, Xinjiang, China"),
    ("FUKANG", "Fukang, Xinjiang, China"),
    ("XINING", "Xining, Qinghai, China"),
    ("DANGYANG", "Dangyang, Hubei, China"),
    ("YICHANG", "Yichang, Hubei, China"),
    ("JINGZHOU", "Jingzhou, Hubei, China"),
    ("SHEDIAN", "Shedian, Henan, China"),
    ("TAIYUAN", "Taiyuan, Shanxi, China"),
    ("ZHANJIANG", "Zhanjiang, Guangdong, China"),
    ("HEGANG", "Hegang, Heilongjiang, China"),
    ("ZHANGJIAKOU", "Zhangjiakou, Hebei, China"),
    ("SANXIA RENJIA", "Yichang, Hubei, China"),
    ("TRES GARGANTAS", "Yichang, Hubei, China"),
    ("SHÍTÀNJǏNG", "Shitanjing, Ningxia, China"),
    ("PROYECTO 816", "Fuling, Chongqing, China"),
    ("YUQUAN", "Dangyang, Hubei, China"),
    ("MONTAÑAS ARCOIRIS", "Zhangye, Gansu, China"),
    ("GRAN MURALLA", "Jiayuguan, Gansu, China"),
    ("TENGGELI", "Shapotou, Ningxia, China"),
    ("TIANZHU", "Tianzhu, Gansu, China"),
    ("CAOBUHU", "Hubei, China"),
    ("MONTAÑAS DRAGON BALL", "Zhangjiajie, Hunan, China"),
    ("DRAGON BALL", "Zhangjiajie, Hunan, China"),
    ("RÍO AMARILLO", "Lanzhou, Gansu, China"),

    # Cities (alphabetical)
    ("CHANGCHUN", "Changchun, Jilin, China"),
    ("CHANGSHA", "Changsha, Hunan, China"),
    ("CHENGDU", "Chengdu, Sichuan, China"),
    ("CHONGQING", "Chongqing, China"),
    ("DONGGUAN", "Dongguan, Guangdong, China"),
    ("GUANGZHOU", "Guangzhou, Guangdong, China"),
    ("GUIYANG", "Guiyang, Guizhou, China"),
    ("HANGZHOU", "Hangzhou, Zhejiang, China"),
    ("HARBIN", "Harbin, Heilongjiang, China"),
    ("HEFEI", "Hefei, Anhui, China"),
    ("HONG KONG", "Hong Kong"),
    ("KUNMING", "Kunming, Yunnan, China"),
    ("LANZHOU", "Lanzhou, Gansu, China"),
    ("MACAO", "Macao"),
    ("MACAU", "Macao"),
    ("NANJING", "Nanjing, Jiangsu, China"),
    ("NANNING", "Nanning, Guangxi, China"),
    ("PEKÍN", "Beijing, China"),
    ("PEKIN", "Beijing, China"),
    ("BEIJING", "Beijing, China"),
    ("QINGDAO", "Qingdao, Shandong, China"),
    ("SANYA", "Sanya, Hainan, China"),
    ("SHANGHAI", "Shanghai, China"),
    ("SHENZHEN", "Shenzhen, Guangdong, China"),
    ("WUHAN", "Wuhan, Hubei, China"),
    ("XIAMEN", "Xiamen, Fujian, China"),
    ("XI'AN", "Xi'an, Shaanxi, China"),
    ("XIAN", "Xi'an, Shaanxi, China"),
    ("ZHENGZHOU", "Zhengzhou, Henan, China"),

    # Provinces / regions (lower priority, used as fallback from descriptions)
    ("CANTÓN", "Guangzhou, Guangdong, China"),
    ("GUANGDONG", "Guangdong, China"),
    ("FUJIAN", "Fujian, China"),
    ("GUIZHOU", "Guizhou, China"),
    ("HAINAN", "Haikou, Hainan, China"),
    ("HENAN", "Zhengzhou, Henan, China"),
    ("HUBEI", "Wuhan, Hubei, China"),
    ("HUNAN", "Changsha, Hunan, China"),
    ("JIANGSU", "Nanjing, Jiangsu, China"),
    ("JILIN", "Jilin, China"),
    ("GANSU", "Lanzhou, Gansu, China"),
    ("MONGOLIA INTERIOR", "Hohhot, Inner Mongolia, China"),
    ("INNER MONGOLIA", "Hohhot, Inner Mongolia, China"),
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
    ("MESETA TIBETANA", "Lhasa, Tibet, China"),
    ("YANBIAN", "Yanbian, Jilin, China"),
    ("XIANGXI", "Xiangxi, Hunan, China"),
]

# Patterns to extract location from descriptions (Spanish)
DESC_LOCATION_PATTERNS = [
    r"(?:os (?:mostramos|llevamos)(?: a| por)?) (\w[\w\s']+?)(?:,| y | para | mientras | donde )",
    r"grabado en (\w[\w\s]+?)(?:,|\.|$| \()",
    r"(?:provincia de|región de) (\w[\w\s]+?)(?:,|\.|$| \()",
    r"capital de (\w[\w\s]+?)(?:,|\.|$| \()",
    r"ciudad (?:de|china de) (\w[\w\s]+?)(?:,|\.|$| \()",
    r"localidad de (\w[\w\s]+?)(?:,|\.|$| \()",
]

# Videos to explicitly skip (pure analysis, no filming location)
# These are about geopolitics, theory, etc. with no meaningful map pin
SKIP_KEYWORDS_IN_TITLE = [
    "ARANCELES", "SANCIONES", "GUERRA COMERCIAL", "DESACOPLAMIENTO",
    "TIERRAS RARAS", "NEXPERIA", "PLAN QUINQUENAL",
    "GLOBOS ESPÍA", "PROPUESTA DE CHINA PARA LA PAZ",
    "EEUU CONTRA EL AVANCE", "EEUU QUIERE GROENLANDIA",
    "POSTURA DE CHINA SOBRE VENEZUELA",
    "PROYECTO MANHATTAN DE CHINA",
    "TENSIONES JAPÓN", "RELACIONES CHINA-IRÁN",
    "TAIWÁN se está VOLVIENDO",
    "PAÍSES ÁRABES", "QATAR",
]

# Countries that are NOT in China - skip these as filming locations
NON_CHINA_ANALYSIS = {
    "EEUU", "JAPÓN", "JAPON", "INDIA", "RUSIA", "VENEZUELA",
    "GROENLANDIA", "IRÁN", "IRAN", "PANAMÁ", "PANAMA", "CUBA",
    "EUROPA", "COREA", "TAIWÁN", "TAIWAN", "VIETNAM", "UCRANIA",
}


def get_best_thumbnail(thumbnails: list[dict]) -> str | None:
    """Pick the best available thumbnail URL."""
    if not thumbnails:
        return None
    # Prefer medium quality (~320px wide)
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
    # Fallback: largest available
    for t in sorted(thumbnails, key=lambda x: x.get("width", 0) or 0, reverse=True):
        if t.get("url"):
            return t["url"]
    return thumbnails[0].get("url") if thumbnails else None


def extract_location(title: str, description: str) -> str | None:
    """Try to extract a geocoding query from the video title and description."""
    title_upper = title.upper()
    desc_upper = (description or "").upper()
    combined = title_upper + " " + desc_upper

    # Skip pure geopolitics videos
    for skip in SKIP_KEYWORDS_IN_TITLE:
        if skip in title_upper:
            return None

    # First pass: check title for specific location keywords
    for keyword, query in LOCATION_KEYWORDS:
        if keyword in title_upper:
            # Skip non-China country references that are just analysis topics
            if keyword in NON_CHINA_ANALYSIS:
                continue
            return query

    # Second pass: check description for location keywords
    for keyword, query in LOCATION_KEYWORDS:
        if keyword in desc_upper:
            if keyword in NON_CHINA_ANALYSIS:
                continue
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

    print(f"Processing {len(videos)} videos...")

    results = []
    geocoded_locations: dict[str, tuple[float, float] | None] = {}
    skipped = 0
    no_location = 0

    for i, video in enumerate(videos):
        vid = video.get("id", "")
        title = video.get("title", "")
        desc = video.get("description", "") or ""
        thumbnails = video.get("thumbnails", [])

        # Clean title: remove "| Jabiertzo" suffix
        clean_title = re.sub(r"\s*\|\s*Jabiertzo.*$", "", title).strip()
        clean_title = re.sub(r"\s*\|\s*Jabiertzo.*$", "", clean_title, flags=re.IGNORECASE).strip()

        location_query = extract_location(title, desc)

        if not location_query:
            no_location += 1
            continue

        # Geocode (with caching)
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

        # Standard YouTube thumbnail as fallback
        thumb = get_best_thumbnail(thumbnails)
        if not thumb:
            thumb = f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"

        results.append({
            "id": vid,
            "title": clean_title,
            "url": f"https://www.youtube.com/watch?v={vid}",
            "thumbnail": thumb,
            "lat": lat,
            "lon": lon,
            "location": location_query.split(",")[0],
        })

    # Sort by location for nicer grouping
    results.sort(key=lambda x: (x["lat"], x["lon"]))

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

    print(f"\nDone! {len(results)} videos mapped, {no_location} without location, {skipped} geocode failures")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
