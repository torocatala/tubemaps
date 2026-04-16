#!/usr/bin/env python3
"""Scrape video metadata from a YouTube channel using yt-dlp."""

import json
import subprocess
import sys
import os

CHANNEL_URL = os.environ.get("CHANNEL_URL", "https://www.youtube.com/c/Jabiertzo/streams")
OUTPUT_FILE = "/data/channel_raw.jsonl"


def scrape_channel(channel_url: str, output_file: str):
    print(f"Scraping channel: {channel_url}")
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--dump-json",
        channel_url,
    ]
    with open(output_file, "w") as f:
        result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        print(f"yt-dlp stderr: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    count = sum(1 for _ in open(output_file))
    print(f"Scraped {count} videos -> {output_file}")


if __name__ == "__main__":
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    scrape_channel(CHANNEL_URL, OUTPUT_FILE)
