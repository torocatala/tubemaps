# TubeMaps

Map YouTube videos by location. Currently configured for the [Jabiertzo](https://www.youtube.com/c/Jabiertzo) channel.

## Quick Start

```bash
# Create the data volume and run the full pipeline
docker volume create tubemaps_data
docker compose --profile pipeline up --build
docker compose up -d web
```

Open http://localhost:8090

## Architecture

- **scraper**: yt-dlp fetches video metadata from the YouTube channel -> `channel_raw.jsonl`
- **geocoder**: Extracts locations from titles/descriptions, geocodes via Nominatim -> `videos.json`
- **web**: nginx serves a Leaflet.js map with markers for each geolocated video

## Re-running the pipeline

```bash
docker compose --profile pipeline run --rm scraper
docker compose --profile pipeline run --rm geocoder
```

## Ports

Web server runs on **8090** to avoid conflicts with other services.
