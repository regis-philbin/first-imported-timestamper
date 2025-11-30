import os
import json
import logging
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Optional

import requests


def get_logger() -> logging.Logger:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger("first-imported-timestamper")


logger = get_logger()


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO-ish date strings from Sonarr/Radarr into timezone-aware UTC datetimes."""
    if not date_str:
        return None
    try:
        s = date_str.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        logger.exception("Failed to parse date string '%s'", date_str)
        return None


def set_file_timestamp(path: str, dt: datetime) -> bool:
    """Set atime and mtime for a given file to dt (UTC)."""
    try:
        if not os.path.exists(path):
            logger.warning("File does not exist, skipping: %s", path)
            return False
        ts = dt.timestamp()
        os.utime(path, (ts, ts))
        logger.info("Updated timestamp for %s -> %s", path, dt.isoformat())
        return True
    except Exception:
        logger.exception("Failed to update timestamp for %s", path)
        return False


# ---------- SONARR ----------

def _get_sonarr_base():
    url = os.getenv("SONARR_URL", "").rstrip("/")
    api_key = os.getenv("SONARR_API_KEY", "")
    if not url or not api_key:
        logger.info("SONARR_URL or SONARR_API_KEY not set, skipping Sonarr.")
        return None, None
    base = url + "/api/v3/"
    headers = {"X-Api-Key": api_key}
    return base, headers


def build_sonarr_episode_history() -> Dict[int, datetime]:
    """Return mapping episodeId -> earliest import date."""
    base, headers = _get_sonarr_base()
    if not base:
        return {}

    logger.info("Building Sonarr episode history (earliest import per episode)...")
    earliest: Dict[int, datetime] = {}
    page = 1
    page_size = 1000

    while True:
        params = {
            "page": page,
            "pageSize": page_size,
            "sortKey": "date",
            "sortDirection": "ascending",
            "includeSeries": "true",
            "includeEpisode": "true",
        }
        resp = requests.get(base + "history", headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        # Support both paged and unpaged responses.
        records = data.get("records") if isinstance(data, dict) else data
        if not records:
            break

        for rec in records:
            event_type = rec.get("eventType") or rec.get("event_type")
            if not event_type:
                continue
            event_type_lower = str(event_type).lower()
            if "import" not in event_type_lower:
                # Skip non-import history rows.
                continue

            episode_id = rec.get("episodeId")
            if not episode_id and rec.get("episode"):
                episode_id = rec["episode"].get("id")
            if not episode_id:
                continue

            date_str = rec.get("date") or rec.get("eventDate")
            dt = parse_date(date_str)
            if not dt:
                continue

            current = earliest.get(episode_id)
            if current is None or dt < current:
                earliest[episode_id] = dt

        total_records = data.get("totalRecords") if isinstance(data, dict) else None
        logger.debug(
            "Sonarr history page %s processed, %s records, totalRecords=%s",
            page,
            len(records),
            total_records,
        )

        if total_records is None or page * page_size >= total_records:
            break

        page += 1

    logger.info("Built Sonarr earliest-import map for %d episodes.", len(earliest))
    return earliest


def update_sonarr_file_timestamps() -> None:
    base, headers = _get_sonarr_base()
    if not base:
        return

    earliest = build_sonarr_episode_history()
    if not earliest:
        logger.warning("No Sonarr history found; nothing to update.")
        return

    logger.info("Fetching Sonarr series list...")
    resp = requests.get(base + "series", headers=headers, timeout=60)
    resp.raise_for_status()
    series_list = resp.json()
    logger.info("Found %d Sonarr series.", len(series_list))

    total_files = 0
    updated_files = 0

    for series in series_list:
        series_id = series.get("id")
        if series_id is None:
            continue

        logger.info("Processing Sonarr series '%s' (id=%s)...", series.get("title"), series_id)
        ef_resp = requests.get(
            base + "episodefile",
            headers=headers,
            params={"seriesId": series_id},
            timeout=60,
        )
        ef_resp.raise_for_status()
        episode_files = ef_resp.json()
        logger.info("  Found %d episode files.", len(episode_files))

        for ef in episode_files:
            total_files += 1
            path = ef.get("path")
            episode_ids = ef.get("episodeIds") or []
            if not path or not episode_ids:
                logger.debug("  Episode file missing path or episodeIds, skipping: %s", ef)
                continue

            # Find earliest import date among all episodes in this file.
            file_dt: Optional[datetime] = None
            for eid in episode_ids:
                dt = earliest.get(eid)
                if dt and (file_dt is None or dt < file_dt):
                    file_dt = dt

            if not file_dt:
                logger.info("  No import history for file: %s", path)
                continue

            logger.info(
                "  Updating Sonarr file timestamp for %s using earliest import %s",
                path,
                file_dt.isoformat(),
            )
            if set_file_timestamp(path, file_dt):
                updated_files += 1

    logger.info(
        "Sonarr processing complete. Examined %d files, updated %d.",
        total_files,
        updated_files,
    )


# ---------- RADARR ----------

def _get_radarr_base():
    url = os.getenv("RADARR_URL", "").rstrip("/")
    api_key = os.getenv("RADARR_API_KEY", "")
    if not url or not api_key:
        logger.info("RADARR_URL or RADARR_API_KEY not set, skipping Radarr.")
        return None, None
    base = url + "/api/v3/"
    headers = {"X-Api-Key": api_key}
    return base, headers


def build_radarr_movie_history() -> Dict[int, datetime]:
    """Return mapping movieId -> earliest import date."""
    base, headers = _get_radarr_base()
    if not base:
        return {}

    logger.info("Building Radarr movie history (earliest import per movie)...")
    earliest: Dict[int, datetime] = {}
    page = 1
    page_size = 1000

    while True:
        params = {
            "page": page,
            "pageSize": page_size,
            "sortKey": "date",
            "sortDirection": "ascending",
            "includeMovie": "true",
        }
        resp = requests.get(base + "history", headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        records = data.get("records") if isinstance(data, dict) else data
        if not records:
            break

        for rec in records:
            event_type = rec.get("eventType") or rec.get("event_type")
            if not event_type:
                continue
            event_type_lower = str(event_type).lower()
            if "import" not in event_type_lower:
                continue

            movie_id = rec.get("movieId")
            if not movie_id and rec.get("movie"):
                movie_id = rec["movie"].get("id")
            if not movie_id:
                continue

            date_str = rec.get("date") or rec.get("eventDate")
            dt = parse_date(date_str)
            if not dt:
                continue

            current = earliest.get(movie_id)
            if current is None or dt < current:
                earliest[movie_id] = dt

        total_records = data.get("totalRecords") if isinstance(data, dict) else None
        logger.debug(
            "Radarr history page %s processed, %s records, totalRecords=%s",
            page,
            len(records),
            total_records,
        )

        if total_records is None or page * page_size >= total_records:
            break

        page += 1

    logger.info("Built Radarr earliest-import map for %d movies.", len(earliest))
    return earliest


def update_radarr_file_timestamps() -> None:
    base, headers = _get_radarr_base()
    if not base:
        return

    earliest = build_radarr_movie_history()
    if not earliest:
        logger.warning("No Radarr history found; nothing to update.")
        return

    logger.info("Fetching Radarr movie list...")
    resp = requests.get(base + "movie", headers=headers, timeout=60)
    resp.raise_for_status()
    movies = resp.json()
    logger.info("Found %d Radarr movies.", len(movies))

    total_files = 0
    updated_files = 0

    for movie in movies:
        movie_id = movie.get("id")
        title = movie.get("title")
        movie_file = movie.get("movieFile")
        if not movie_id or not movie_file:
            logger.debug("Movie has no file yet, skipping: %s", title)
            continue

        path = movie_file.get("path")
        if not path:
            continue

        dt = earliest.get(movie_id)
        if not dt:
            logger.info("No import history for movie '%s' (id=%s), file=%s", title, movie_id, path)
            continue

        total_files += 1
        logger.info(
            "Updating Radarr file timestamp for '%s' (%s) -> %s",
            title,
            path,
            dt.isoformat(),
        )
        if set_file_timestamp(path, dt):
            updated_files += 1

    logger.info(
        "Radarr processing complete. Examined %d files, updated %d.",
        total_files,
        updated_files,
    )


# ---------- HTTP SERVER ----------

class TimestampHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        raw_body = self.rfile.read(content_length) if content_length > 0 else b""
        try:
            body = json.loads(raw_body.decode("utf-8") or "{}")
        except Exception:
            body = {}
        logger.info("Received %s request on %s with payload keys: %s",
                    self.command, self.path, list(body.keys()))

        if self.path.startswith("/sonarr"):
            logger.info("Triggered Sonarr full-library timestamp update.")
            try:
                update_sonarr_file_timestamps()
                self._send_json(200, {"status": "ok", "message": "Sonarr full-library update complete"})
            except Exception as e:
                logger.exception("Error while processing Sonarr update request")
                self._send_json(500, {"status": "error", "message": str(e)})
        elif self.path.startswith("/radarr"):
            logger.info("Triggered Radarr full-library timestamp update.")
            try:
                update_radarr_file_timestamps()
                self._send_json(200, {"status": "ok", "message": "Radarr full-library update complete"})
            except Exception as e:
                logger.exception("Error while processing Radarr update request")
                self._send_json(500, {"status": "error", "message": str(e)})
        elif self.path.startswith("/full"):
            logger.info("Triggered Sonarr + Radarr full-library timestamp update.")
            try:
                update_sonarr_file_timestamps()
                update_radarr_file_timestamps()
                self._send_json(200, {"status": "ok", "message": "Sonarr + Radarr full-library update complete"})
            except Exception as e:
                logger.exception("Error while processing full update request")
                self._send_json(500, {"status": "error", "message": str(e)})
        else:
            logger.warning("Unknown path requested: %s", self.path)
            self._send_json(404, {"status": "error", "message": "Unknown path"})

    # Silence default noisy logging to stderr for every request
    def log_message(self, format, *args):
        logger.info("HTTP %s - %s", self.address_string(), format % args)


def run_server():
    port = int(os.getenv("PORT", "8095"))
    server_address = ("", port)
    httpd = HTTPServer(server_address, TimestampHandler)
    logger.info("first-imported-timestamper HTTP server listening on port %d", port)

    if os.getenv("RUN_FULL_ON_START", "false").lower() == "true":
        logger.info("RUN_FULL_ON_START=true, performing one-time full Sonarr + Radarr update on startup...")
        try:
            update_sonarr_file_timestamps()
            update_radarr_file_timestamps()
        except Exception:
            logger.exception("Error during initial full-library update on startup.")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down first-imported-timestamper HTTP server...")
    finally:
        httpd.server_close()


if __name__ == "__main__":
    run_server()
