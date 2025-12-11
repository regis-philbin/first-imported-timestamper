import os
import json
import logging
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Optional, Set
import threading

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

# Global dry-run flag
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
if DRY_RUN:
    logger.info("DRY RUN MODE ENABLED: no file timestamps will actually be changed.")
else:
    logger.info("Dry run disabled: timestamps WILL be modified.")


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
    """
    Set atime and mtime for a given file to dt (UTC).

    Respects DRY_RUN:
      - If DRY_RUN is true, logs what it *would* do but does not call os.utime().
      - Returns True if the file exists (so upstream logic can treat as 'success').
    """
    try:
        if not os.path.exists(path):
            logger.warning("File does not exist, skipping: %s", path)
            return False

        if DRY_RUN:
            logger.info("[DRY RUN] Would update timestamp for %s -> %s",
                        path, dt.isoformat())
            return True

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


def get_sonarr_episode_earliest_import_from_series_history(
    episode_id: int,
    series_id: Optional[int] = None,
) -> Optional[datetime]:
    """
    Query Sonarr's /api/v3/history/series for a single episode and return the
    earliest *import* history date for that episode (UTC).

    This is much faster than scanning the entire /api/v3/history list.
    """
    base, headers = _get_sonarr_base()
    if not base:
        return None

    url = base + "history/series"
    params = {
        "page": 1,
        "pageSize": 20,          # small number; per-episode history is tiny
        "sortKey": "date",
        "sortDirection": "ascending",
        "includeSeries": "true",
        "includeEpisode": "true",
        "episodeId": episode_id,
    }
    if series_id is not None:
        params["seriesId"] = series_id

    logger.info(
        "Querying Sonarr /history/series for episodeId=%s, seriesId=%s",
        episode_id,
        series_id,
    )

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(
            "Error calling Sonarr /api/v3/history/series for episodeId=%s: %s",
            episode_id,
            e,
        )
        return None

    # Sonarr can return either:
    #  - a plain list: [ {...}, {...}, ... ]
    #  - or a paged object: { "records": [ {...}, ... ], "totalRecords": N, ... }
    data = resp.json() or []

    if isinstance(data, dict):
        records = data.get("records") or []
    else:
        records = data

    if not records:
        logger.warning(
            "No Sonarr history/series records returned for episodeId=%s (seriesId=%s)",
            episode_id,
            series_id,
        )
        return None

    # History is already sorted ascending by date, but we still filter
    # for import-type events and pick the earliest timestamp just in case.
    earliest_dt: Optional[datetime] = None

    for rec in records:
        event_type = rec.get("eventType") or rec.get("event_type")
        if not event_type or "import" not in str(event_type).lower():
            continue

        date_str = rec.get("date") or rec.get("eventDate")
        dt = parse_date(date_str)
        if not dt:
            continue

        if earliest_dt is None or dt < earliest_dt:
            earliest_dt = dt

    if not earliest_dt:
        logger.warning(
            "No import-type history found in /history/series for episodeId=%s (seriesId=%s)",
            episode_id,
            series_id,
        )
        return None

    logger.info(
        "Earliest Sonarr import history for episodeId=%s (seriesId=%s): %s",
        episode_id,
        series_id,
        earliest_dt.isoformat(),
    )
    return earliest_dt


def get_sonarr_earliest_import_for_episodes(
    episode_ids: Set[int],
    series_id: Optional[int],
) -> Dict[int, datetime]:
    """
    For a small set of episodes (e.g. from a webhook), query Sonarr's
    /history/series for each one individually and return a map of
    episodeId -> earliest datetime.
    """
    results: Dict[int, datetime] = {}

    for eid in sorted(episode_ids):
        dt = get_sonarr_episode_earliest_import_from_series_history(eid, series_id)
        if dt is not None:
            results[eid] = dt
        else:
            logger.warning(
                "Could not determine earliest import for episodeId=%s via /history/series",
                eid,
            )

    return results


def build_sonarr_episode_history_full() -> Dict[int, datetime]:
    """Return mapping episodeId -> earliest import date for the entire Sonarr library."""
    base, headers = _get_sonarr_base()
    if not base:
        return {}

    logger.info("Building Sonarr episode history (earliest import per episode) for full library...")
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

        records = data.get("records") if isinstance(data, dict) else data
        if not records:
            break

        for rec in records:
            event_type = rec.get("eventType") or rec.get("event_type")
            if not event_type:
                continue
            if "import" not in str(event_type).lower():
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

    logger.info("Built Sonarr earliest-import map for %d episodes (full library).", len(earliest))
    return earliest


def update_sonarr_file_timestamps_full() -> None:
    """Full-library Sonarr run: walk all series & episodefiles and reset timestamps."""
    base, headers = _get_sonarr_base()
    if not base:
        return

    earliest = build_sonarr_episode_history_full()
    if not earliest:
        logger.warning("No Sonarr history found; nothing to update.")
        return

    logger.info("Fetching Sonarr series list (full library)...")
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

            file_dt: Optional[datetime] = None
            for eid in episode_ids:
                dt = earliest.get(eid)
                if dt and (file_dt is None or dt < file_dt):
                    file_dt = dt

            if not file_dt:
                logger.info("  No import history for file: %s", path)
                continue

            logger.info(
                "  Updating Sonarr file timestamp (full) for %s using earliest import %s",
                path,
                file_dt.isoformat(),
            )
            if set_file_timestamp(path, file_dt):
                updated_files += 1

    logger.info(
        "Sonarr full-library processing complete. Examined %d files, updated %d.",
        total_files,
        updated_files,
    )


def update_sonarr_from_webhook(payload: dict) -> None:
    """
    Incremental Sonarr run: only update the episode file(s) involved in this webhook.

    Uses /api/v3/history/series per-episode to avoid scanning massive global history.
    """
    event_type = str(payload.get("eventType", "")).lower()
    logger.info("Sonarr webhook eventType='%s'", event_type)

    if "download" not in event_type and "grab" not in event_type and "rename" not in event_type:
        logger.info("Sonarr webhook eventType not a download/import event, nothing to do.")
        return

    series = payload.get("series") or {}
    series_id = series.get("id")

    episodes = payload.get("episodes") or []
    ep_ids: Set[int] = set()
    for ep in episodes:
        eid = ep.get("id") or ep.get("episodeId")
        if eid is not None:
            ep_ids.add(eid)

    episode_file = payload.get("episodeFile") or {}
    ef_episode_ids = episode_file.get("episodeIds") or []
    for eid in ef_episode_ids:
        ep_ids.add(eid)

    path = episode_file.get("path")
    if not path:
        logger.warning("Sonarr webhook payload has no episodeFile.path; nothing to update.")
        return

    if not ep_ids:
        logger.warning("Sonarr webhook payload has no episode IDs; cannot compute earliest import.")
        return

    logger.info(
        "Sonarr webhook refers to %d episode(s) with path: %s; episode IDs: %s",
        len(ep_ids),
        path,
        sorted(ep_ids),
    )

    logger.info(
        "Building Sonarr episode history via /history/series for %d target episode(s): %s",
        len(ep_ids),
        sorted(ep_ids),
    )

    earliest_map = get_sonarr_earliest_import_for_episodes(ep_ids, series_id)

    logger.info(
        "Built Sonarr earliest-import map (via /history/series) for %d/%d target episode(s).",
        len(earliest_map),
        len(ep_ids),
    )

    file_dt: Optional[datetime] = None
    for eid in ep_ids:
        dt = earliest_map.get(eid)
        if dt and (file_dt is None or dt < file_dt):
            file_dt = dt

    if not file_dt:
        logger.warning(
            "Could not determine earliest import date for any of the target episodes; file=%s",
            path,
        )
        return

    logger.info(
        "Updating Sonarr file timestamp (webhook) for %s using earliest import %s",
        path,
        file_dt.isoformat(),
    )
    set_file_timestamp(path, file_dt)


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


def build_radarr_movie_history_for(target_movie_ids: Set[int]) -> Dict[int, datetime]:
    """
    Return mapping movieId -> earliest import date for given target_movie_ids only.
    """
    base, headers = _get_radarr_base()
    if not base:
        return {}

    if not target_movie_ids:
        logger.info("No target Radarr movie IDs provided, nothing to look up.")
        return {}

    logger.info(
        "Building Radarr movie history for %d target movie(s): %s",
        len(target_movie_ids),
        sorted(target_movie_ids),
    )

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
            if "import" not in str(event_type).lower():
                continue

            movie_id = rec.get("movieId")
            if not movie_id and rec.get("movie"):
                movie_id = rec["movie"].get("id")
            if not movie_id:
                continue

            if movie_id not in target_movie_ids:
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
            "Radarr history page %s processed for targeted movies, %s records, totalRecords=%s",
            page,
            len(records),
            total_records,
        )

        if len(earliest) >= len(target_movie_ids):
            logger.info("Found earliest history for all target Radarr movies, stopping early.")
            break

        if total_records is None or page * page_size >= total_records:
            break

        page += 1

    logger.info(
        "Built Radarr earliest-import map for %d/%d target movie(s).",
        len(earliest),
        len(target_movie_ids),
    )
    return earliest


def build_radarr_movie_history_full() -> Dict[int, datetime]:
    """Return mapping movieId -> earliest import date for full Radarr library."""
    base, headers = _get_radarr_base()
    if not base:
        return {}

    logger.info("Building Radarr movie history (earliest import per movie) for full library...")
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
            if "import" not in str(event_type).lower():
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

    logger.info("Built Radarr earliest-import map for %d movies (full library).", len(earliest))
    return earliest


def update_radarr_file_timestamps_full() -> None:
    """Full-library Radarr run: walk all movies and reset timestamps."""
    base, headers = _get_radarr_base()
    if not base:
        return

    earliest = build_radarr_movie_history_full()
    if not earliest:
        logger.warning("No Radarr history found; nothing to update.")
        return

    logger.info("Fetching Radarr movie list (full library)...")
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
            logger.info(
                "No import history for movie '%s' (id=%s), file=%s",
                title,
                movie_id,
                path,
            )
            continue

        total_files += 1
        logger.info(
            "Updating Radarr file timestamp (full) for '%s' (%s) -> %s",
            title,
            path,
            dt.isoformat(),
        )
        if set_file_timestamp(path, dt):
            updated_files += 1

    logger.info(
        "Radarr full-library processing complete. Examined %d files, updated %d.",
        total_files,
        updated_files,
    )


def update_radarr_from_webhook(payload: dict) -> None:
    """
    Incremental Radarr run: only update the movie file involved in this webhook.
    """
    event_type = str(payload.get("eventType", "")).lower()
    logger.info("Radarr webhook eventType='%s'", event_type)

    if "download" not in event_type and "grab" not in event_type and "rename" not in event_type:
        logger.info("Radarr webhook eventType not a download/import event, nothing to do.")
        return

    movie = payload.get("movie") or {}
    movie_id = movie.get("id")
    title = movie.get("title") or movie.get("titleSlug") or "Unknown title"

    movie_file = payload.get("movieFile") or {}
    path = movie_file.get("path")

    if movie_id is None:
        logger.warning("Radarr webhook payload has no movie.id; cannot compute earliest import.")
        return

    if not path:
        logger.warning("Radarr webhook payload has no movieFile.path; nothing to update.")
        return

    logger.info(
        "Radarr webhook refers to movie '%s' (id=%s) with path: %s",
        title,
        movie_id,
        path,
    )

    earliest_map = build_radarr_movie_history_for({movie_id})
    dt = earliest_map.get(movie_id)
    if not dt:
        logger.warning(
            "No Radarr history found for movie '%s' (id=%s); keeping timestamp as-is for file=%s",
            title,
            movie_id,
            path,
        )
        return

    logger.info(
        "Updating Radarr file timestamp (webhook) for '%s' (%s) -> %s",
        title,
        path,
        dt.isoformat(),
    )
    set_file_timestamp(path, dt)


# ---------- HTTP SERVER ----------

class TimestampHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except BrokenPipeError:
            logger.warning(
                "Client closed connection before response could be sent (BrokenPipeError)."
            )
        except ConnectionResetError:
            logger.warning(
                "Client reset connection before response could be sent (ConnectionResetError)."
            )
        except Exception:
            logger.exception("Unexpected error while sending HTTP response")

    def _run_in_background(self, fn, *args, **kwargs):
        """Run a function in a background thread (fire-and-forget)."""
        t = threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return t

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        raw_body = self.rfile.read(content_length) if content_length > 0 else b""
        try:
            body = json.loads(raw_body.decode("utf-8") or "{}")
        except Exception:
            body = {}

        logger.info(
            "Received %s request on %s with payload keys: %s",
            self.command,
            self.path,
            list(body.keys()),
        )

        # --- SONARR ---
        if self.path.startswith("/sonarr"):
            if "eventType" in body:
                # Webhook path: do work in the background, respond immediately
                logger.info(
                    "Triggered Sonarr webhook (incremental) timestamp update (background job)."
                )
                try:
                    self._run_in_background(update_sonarr_from_webhook, body)
                    self._send_json(
                        200,
                        {
                            "status": "ok",
                            "message": "Sonarr webhook update scheduled (running in background)",
                        },
                    )
                except Exception as e:
                    logger.exception("Error while scheduling Sonarr webhook request")
                    self._send_json(
                        500,
                        {"status": "error", "message": f"Failed to schedule job: {e}"},
                    )
            else:
                # Manual/full call: we block until done
                logger.info("Triggered Sonarr full-library timestamp update (manual).")
                try:
                    update_sonarr_file_timestamps_full()
                    self._send_json(
                        200,
                        {"status": "ok", "message": "Sonarr full-library update complete"},
                    )
                except Exception as e:
                    logger.exception("Error while processing Sonarr full update request")
                    self._send_json(500, {"status": "error", "message": str(e)})

        # --- RADARR ---
        elif self.path.startswith("/radarr"):
            if "eventType" in body:
                logger.info(
                    "Triggered Radarr webhook (incremental) timestamp update (background job)."
                )
                try:
                    self._run_in_background(update_radarr_from_webhook, body)
                    self._send_json(
                        200,
                        {
                            "status": "ok",
                            "message": "Radarr webhook update scheduled (running in background)",
                        },
                    )
                except Exception as e:
                    logger.exception("Error while scheduling Radarr webhook request")
                    self._send_json(
                        500,
                        {"status": "error", "message": f"Failed to schedule job: {e}"},
                    )
            else:
                logger.info("Triggered Radarr full-library timestamp update (manual).")
                try:
                    update_radarr_file_timestamps_full()
                    self._send_json(
                        200,
                        {"status": "ok", "message": "Radarr full-library update complete"},
                    )
                except Exception as e:
                    logger.exception("Error while processing Radarr full update request")
                    self._send_json(500, {"status": "error", "message": str(e)})

        # --- FULL BOTH ---
        elif self.path.startswith("/full"):
            logger.info("Triggered Sonarr + Radarr full-library timestamp update (manual).")
            try:
                update_sonarr_file_timestamps_full()
                update_radarr_file_timestamps_full()
                self._send_json(
                    200,
                    {
                        "status": "ok",
                        "message": "Sonarr + Radarr full-library update complete",
                    },
                )
            except Exception as e:
                logger.exception("Error while processing /full update request")
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
        logger.info(
            "RUN_FULL_ON_START=true, performing one-time full Sonarr + Radarr update on startup..."
        )
        try:
            update_sonarr_file_timestamps_full()
            update_radarr_file_timestamps_full()
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
