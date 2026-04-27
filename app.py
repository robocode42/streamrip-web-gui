import asyncio
import json
import logging
import os
import queue
import re
import shutil
import threading
import time

import requests
from flask import (
    Flask,
    Response,
    jsonify,
    render_template,
    request,
    stream_with_context,
)
from streamrip.client import (
    DeezerClient,
    QobuzClient,
    SoundcloudClient,
    TidalClient,
)
from streamrip.config import Config
from streamrip.metadata.search_results import SearchResults
from streamrip.rip.main import Main

# new logging config
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

STREAMRIP_CONFIG = os.environ.get("STREAMRIP_CONFIG", "/config/config.toml")
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/music")
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "2"))
STREAMRIP_USERS = os.environ.get("STREAMRIP_USERS")
DEFAULT_QUALITY = int(os.environ.get("DEFAULT_QUALITY", "3"))
DEFAULT_SEARCH_SOURCE = os.environ.get("DEFAULT_SEARCH_SOURCE", "qobuz")

download_queue = queue.Queue()
active_downloads = {}
download_history = []
sse_clients = []
album_art_cache = {}
cache_lock = threading.Lock()
download_lock = threading.Lock()
if STREAMRIP_USERS:
    USERS = [user.strip() for user in STREAMRIP_USERS.split(",") if user.strip()]
else:
    USERS = []


class DownloadWorker(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)

    def run(self):
        while True:
            task = download_queue.get()
            if task is None:
                break

            task_id = task["id"]
            url = task["url"]
            quality = task.get("quality", DEFAULT_QUALITY)
            metadata = task.get("metadata", {})
            download_dir = task.get("directory", DOWNLOAD_DIR)
            user = task.get("user")

            active_downloads[task_id] = {
                "status": "downloading",
                "url": url,
                "metadata": metadata,
                "started": time.time(),
                "user": user,
            }

            broadcast_sse(
                {
                    "type": "download_started",
                    "id": task_id,
                    "metadata": metadata,
                    "status": "downloading",
                    "user": user,
                }
            )

            try:
                with download_lock:
                    self._download_with_library(
                        task_id, url, quality, download_dir, metadata, user
                    )

                broadcast_sse(
                    {
                        "type": "download_completed",
                        "id": task_id,
                        "status": "completed",
                        "metadata": metadata,
                        "user": user,
                    }
                )
            except Exception as e:
                logger.exception(f"Download failed for task {task_id}: {e}")
                broadcast_sse(
                    {
                        "type": "download_error",
                        "id": task_id,
                        "status": "failed",
                        "metadata": metadata,
                        "error": str(e),
                        "user": user,
                    }
                )
            finally:
                if task_id in active_downloads:
                    logger.info(f"TASKID {task_id}")
                    del active_downloads[task_id]

            download_queue.task_done()

    def _download_with_library(
        self, task_id, url, quality, download_dir, metadata, user
    ):
        """Download using streamrip library instead of subprocess."""

        async def _async_download():
            config = Config(STREAMRIP_CONFIG)
            # Set download folder and quality for this session
            config.session.downloads.folder = download_dir
            config.session.qobuz.quality = quality
            config.session.tidal.quality = quality
            config.session.deezer.quality = quality
            config.session.soundcloud.quality = quality
            # Disable CLI progress bars (we use SSE for progress)
            config.session.cli.progress_bars = False
            config.session.cli.text_output = False

            async with Main(config) as main:
                await main.add(url)
                await main.resolve()

                for item in main.media:
                    await item.rip()

        asyncio.run(_async_download())


def broadcast_sse(data):
    message = f"data: {json.dumps(data)}\n\n"
    dead_clients = []

    for client in sse_clients:
        try:
            client.put(message)
        except:
            dead_clients.append(client)

    for client in dead_clients:
        sse_clients.remove(client)


@app.route("/api/events")
def sse_events():
    def generate():
        q = queue.Queue()
        sse_clients.append(q)

        try:
            yield f"data: {json.dumps({'type': 'connected'})}\n\n"

            while True:
                try:
                    msg = q.get(timeout=30)
                    yield msg
                except queue.Empty:
                    continue  # previous heartbeat check
        finally:
            sse_clients.remove(q)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # disable nginx buffering
        },
    )


workers = []
for _ in range(MAX_CONCURRENT_DOWNLOADS):
    worker = DownloadWorker()
    worker.start()
    workers.append(worker)


def create_user_routes(app, users):
    for user in users:
        route_path = f"/{user}/"

        def user_route(user=user):
            return render_template(
                "index.html",
                user=user,
                default_quality=DEFAULT_QUALITY,
                default_search_source=DEFAULT_SEARCH_SOURCE,
            )

        # Use a unique endpoint name for each route
        endpoint_name = f"user_{user}"
        app.route(route_path, endpoint=endpoint_name)(user_route)


if USERS:

    @app.route("/")
    def user_list_page():
        return render_template("users.html", users=USERS)

    create_user_routes(app, USERS)
else:

    @app.route("/")
    def index():
        return render_template(
            "index.html",
            user=None,
            default_quality=DEFAULT_QUALITY,
            default_search_source=DEFAULT_SEARCH_SOURCE,
        )


@app.route("/api/download", methods=["POST"])
def start_download():
    data = request.json
    url = data.get("url")
    quality = data.get("quality", 3)
    user = data.get("user")

    if not url:
        return jsonify({"error": "URL is required"}), 400

    # Validate URL (basic check)
    # youtube-dl for later
    valid_services = [
        "spotify.com",
        "deezer.com",
        "tidal.com",
        "qobuz.com",
        "soundcloud.com",
        "youtube.com",
    ]
    if not any(service in url.lower() for service in valid_services):
        return jsonify({"error": "Unsupported service URL"}), 400

    if USERS:
        if not user or user not in USERS:
            return jsonify({"error": "Unauthorized or missing user"}), 403
        directory = os.path.join(DOWNLOAD_DIR, user)
    else:
        directory = DOWNLOAD_DIR

    if not os.path.exists(directory):
        try:
            os.makedirs(directory, exist_ok=True)
        except Exception as e:
            return jsonify({"error": f"Failed to create directory: {e}"}), 500

    # Metadata: use provided fields or extract from URL
    title = data.get("title")
    artist = data.get("artist")
    album_art = data.get("album_art")
    service = data.get("service")

    if title and artist and service:
        metadata = {
            "title": title,
            "artist": artist,
            "album_art": album_art,
            "service": service,
        }
    else:
        metadata = extract_metadata_from_url(url)

    task_id = f"dl_{int(time.time() * 1000)}"
    task = {
        "id": task_id,
        "url": url,
        "quality": quality,
        "metadata": metadata,
        "directory": directory,
        "user": user,
    }

    download_queue.put(task)

    return jsonify({"task_id": task_id, "status": "queued", "metadata": metadata})


@app.route("/api/status")
def get_all_status():
    user = request.args.get("user")

    filtered_active = active_downloads
    filtered_history = download_history[-20:]

    if USERS and user:
        filtered_active = {
            k: v for k, v in active_downloads.items() if v.get("user") == user
        }
        filtered_history = [h for h in download_history[-20:] if h.get("user") == user]

    return jsonify(
        {
            "active": filtered_active,
            "history": filtered_history,
            "queue_size": download_queue.qsize(),
        }
    )


@app.route("/api/config", methods=["GET", "POST"])
def config():
    if request.method == "GET":
        if os.path.exists(STREAMRIP_CONFIG):
            with open(STREAMRIP_CONFIG, "r") as f:
                return jsonify({"config": f.read()})
        return jsonify({"config": ""})

    elif request.method == "POST":
        data = request.json
        config_content = data.get("config", "")

        try:
            if os.path.exists(STREAMRIP_CONFIG):
                shutil.copy2(STREAMRIP_CONFIG, f"{STREAMRIP_CONFIG}.bak")

            os.makedirs(os.path.dirname(STREAMRIP_CONFIG), exist_ok=True)
            with open(STREAMRIP_CONFIG, "w") as f:
                f.write(config_content)

            return jsonify({"status": "success"})
        except Exception as e:
            return jsonify({"error": str(e)}), 500


def _search_with_library(source, search_type, query, config_path):
    """Search using streamrip library. Returns list of result dicts."""

    async def _async_search():
        # Map source strings to client classes
        client_map = {
            "qobuz": QobuzClient,
            "tidal": TidalClient,
            "deezer": DeezerClient,
            "soundcloud": SoundcloudClient,
        }
        client_class = client_map.get(source)
        if client_class is None:
            raise ValueError(f"Unsupported source: {source}")

        config = Config(config_path)
        client = client_class(config)

        try:
            await client.login()
            pages = await client.search(search_type, query, limit=100)
        finally:
            try:
                await client.session.close()
            except Exception:
                pass

        if not pages:
            return []

        search_results = SearchResults.from_pages(source, search_type, pages)
        return search_results.as_list(source)

    return asyncio.run(_async_search())


@app.route("/api/search", methods=["POST"])
def search_music():
    data = request.json
    query = data.get("query")
    search_type = data.get("type", "album")
    source = data.get("source", "qobuz")

    logger.info("=" * 60)
    logger.info("SEARCH REQUEST RECEIVED")
    logger.info(f"Query: '{query}'")
    logger.info(f"Type: {search_type}")
    logger.info(f"Source: {source}")
    logger.info("=" * 60)

    if not query:
        logger.warning("No query provided")
        return jsonify({"error": "Query required"}), 400

    if source == "soundcloud" and search_type in ["album", "artist"]:
        logger.debug(f"SoundCloud doesn't support {search_type} search")
        return jsonify(
            {
                "results": [],
                "query": query,
                "source": source,
                "total_count": 0,
                "message": f"SoundCloud does not support {search_type} searches. Try searching for tracks or playlists instead.",
            }
        )

    try:
        raw_results = _search_with_library(source, search_type, query, STREAMRIP_CONFIG)

        results = []
        for idx, item in enumerate(raw_results):
            item_id = item.get("id", "")
            media_type = item.get("media_type", search_type)
            url = construct_url(item.get("source", source), media_type, item_id)

            desc = item.get("desc", "")
            artist = ""
            title = desc

            if " by " in desc:
                parts = desc.rsplit(" by ", 1)
                title = parts[0]
                artist = parts[1]

            result_item = {
                "id": item_id,
                "service": item.get("source", source),
                "type": media_type,
                "artist": artist if artist else desc,
                "title": title if artist else "",
                "desc": desc,
                "url": url,
                "album_art": "",
            }
            results.append(result_item)

            if idx < 3:
                logger.debug(f"Result {idx + 1}: {result_item}")

        logger.info(f"Returning {len(results)} results")

        return jsonify(
            {
                "results": results,
                "query": query,
                "source": source,
                "total_count": len(results),
            }
        )

    except Exception as e:
        logger.exception(f"Error during library search: {e}")
        error_msg = "Search failed"
        exc_type = type(e).__name__

        if "credential" in exc_type.lower() or "authentication" in str(e).lower():
            error_msg = "Authentication failed - check your credentials in config"
        elif "secret" in str(e).lower():
            error_msg = "Invalid app secrets. Update your config."
        elif "timeout" in str(e).lower():
            error_msg = "Search timed out"

        return jsonify(
            {
                "error": error_msg,
                "debug_info": {
                    "exception_type": exc_type,
                    "message": str(e),
                },
            }
        ), 500


@app.route("/api/album-art", methods=["GET"])
def get_album_art():
    source = request.args.get("source")
    media_type = request.args.get("type")
    item_id = request.args.get("id")

    if not all([source, media_type, item_id]):
        return jsonify({"error": "Missing parameters"}), 400

    # Todo: handle SoundCloud special case and get correct albums if possible
    if source == "soundcloud":
        if "|" in item_id:
            item_id = item_id.split("|")[0]
        elif "soundcloud:tracks:" in item_id:
            match = re.search(r"soundcloud:tracks:(\d+)", item_id)
            if match:
                item_id = match.group(1)

    cache_key = f"{source}_{media_type}_{item_id}"
    if cache_key in album_art_cache:
        cached = album_art_cache[cache_key]
        if isinstance(cached, dict):
            return jsonify(cached)
        return jsonify({"album_art": cached})

    try:
        if source == "qobuz":
            result = fetch_qobuz_album_art_info(item_id, media_type, None)
            album_art_cache[cache_key] = result
            return jsonify(
                {
                    "album_art": result.get("album_art", ""),
                    "tracks_count": result.get("tracks_count"),
                    "release_type": result.get("release_type"),
                    "year": result.get("year"),
                }
            )

        elif source == "tidal":
            if media_type == "artist":
                album_art = f"https://resources.tidal.com/images/{item_id}/750x750.jpg"
            else:
                album_art = f"https://resources.tidal.com/images/{item_id}/320x320.jpg"

            return jsonify({"album_art": album_art})

        elif source == "deezer":
            result = fetch_deezer_album_art_info(item_id, media_type)
            album_art_cache[cache_key] = result
            return jsonify(result)

        elif source == "soundcloud":
            # SoundCloud doesn't provide easy access to artwork
            # Just return empty and let the frontend handle placeholders
            return jsonify({"album_art": ""})

        # Default return for unknown sources
        return jsonify({"album_art": ""})

    except Exception as e:
        logger.error(
            f"Error fetching album art for {source}/{media_type}/{item_id}: {e}"
        )
        return jsonify({"album_art": ""})


@app.route("/api/browse")
def browse_downloads():
    user = request.args.get("user")
    if USERS:
        if not user or user not in USERS:
            return jsonify({"error": "Unauthorized or missing user"}), 403
        search_dir = os.path.join(DOWNLOAD_DIR, user)
    else:
        search_dir = DOWNLOAD_DIR

    try:
        files = []
        for root, dirs, filenames in os.walk(search_dir):
            for filename in filenames:
                if filename.endswith((".mp3", ".flac", ".m4a", ".opus")):
                    filepath = os.path.join(root, filename)
                    rel_path = os.path.relpath(filepath, search_dir)
                    files.append(
                        {
                            "name": rel_path,
                            "size": os.path.getsize(filepath),
                            "modified": os.path.getmtime(filepath),
                        }
                    )

        files.sort(key=lambda x: x["modified"], reverse=True)
        return jsonify(files[:100])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_qobuz_credentials():
    try:
        if os.path.exists(STREAMRIP_CONFIG):
            with open(STREAMRIP_CONFIG, "r") as f:
                config_content = f.read()

            app_id = re.search(r'app_id\s*=\s*["\']?([^"\'\n]+)["\']?', config_content)
            token = re.search(r'password_or_token\s*=\s*"([^"]+)"', config_content)

            return {
                "app_id": app_id.group(1).strip() if app_id else "950096963",
                "token": token.group(1).strip() if token else None,
            }
    except Exception as e:
        logger.error(f"Error reading Qobuz credentials: {e}")
    return {"app_id": "950096963", "token": None}


def fetch_qobuz_album_art_info(item_id, media_type, app_id):
    creds = get_qobuz_credentials()
    if not creds["token"]:
        return {}

    try:
        response = requests.get(
            f"https://www.qobuz.com/api.json/0.2/{media_type}/get",
            params={
                "app_id": creds["app_id"],
                f"{media_type}_id": item_id,
            },
            headers={
                "X-App-Id": creds["app_id"],
                "X-User-Auth-Token": creds["token"],
            },
            timeout=3,
        )
        if response.status_code == 200:
            data = response.json()
            image = data.get("image", {})

            year = None
            release_date = data.get("release_date_original", "")
            if release_date:
                year = release_date[:4]

            return {
                "album_art": image.get("large")
                or image.get("small")
                or image.get("thumbnail")
                or "",
                "tracks_count": data.get("tracks_count"),
                "release_type": data.get("release_type"),
                "year": year,
            }
    except Exception as e:
        logger.error(f"Error fetching Qobuz album art: {e}")
    return {}


def fetch_deezer_album_art_info(item_id, media_type):
    """Fetch album art and metadata from Deezer API."""
    result = {
        "album_art": "",
        "tracks_count": None,
        "release_type": None,
        "year": None,
    }
    try:
        endpoint = f"https://api.deezer.com/{media_type}/{item_id}"
        response = requests.get(endpoint, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if media_type == "artist":
                result["album_art"] = data.get(
                    "picture_medium", data.get("picture", "")
                )
            elif media_type == "album":
                result["album_art"] = data.get(
                    "cover_medium", data.get("cover_small", "")
                )
                result["year"] = data.get("release_date", "").split("-")[0] or None
                result["release_type"] = data.get("record_type", "")
                result["tracks_count"] = str(data.get("nb_tracks", ""))
            elif media_type == "track":
                album = data.get("album", {})
                result["album_art"] = album.get(
                    "cover_medium", album.get("cover_small", "")
                )
                result["year"] = data.get("release_date", "").split("-")[0] or None
                result["tracks_count"] = ""
    except Exception as e:
        logger.debug(f"Failed to fetch Deezer metadata for {item_id}: {e}")
    return result


def get_qobuz_app_id():
    try:
        if os.path.exists(STREAMRIP_CONFIG):
            with open(STREAMRIP_CONFIG, "r") as f:
                config_content = f.read()
                # logger.debug(f"Config file content: {config_content[:200]}...")  # First 200 chars

            app_id_match = re.search(
                r'app_id\s*=\s*["\']?([^"\'\n]+)["\']?', config_content
            )

            if app_id_match:
                app_id = app_id_match.group(1).strip()
                logger.debug(f"Found app_id in config: {app_id}")
                return app_id
            else:
                logger.debug("No app_id found in config, using fallback")

        # Return a known working app_id as fallback
        fallback_app_id = "950096963"
        logger.debug(f"Using fallback app_id: {fallback_app_id}")
        return fallback_app_id

    except Exception as e:
        logger.error(f"Error extracting app_id: {e}")
        return "950096963"


def construct_url(source, media_type, item_id):
    if not item_id:
        return ""

    url_patterns = {
        "qobuz": {
            "album": f"https://open.qobuz.com/album/{item_id}",
            "track": f"https://open.qobuz.com/track/{item_id}",
            "artist": f"https://open.qobuz.com/artist/{item_id}",
            "playlist": f"https://open.qobuz.com/playlist/{item_id}",
        },
        "tidal": {
            "album": f"https://tidal.com/browse/album/{item_id}",
            "track": f"https://tidal.com/browse/track/{item_id}",
            "artist": f"https://tidal.com/browse/artist/{item_id}",
            "playlist": f"https://tidal.com/browse/playlist/{item_id}",
        },
        "deezer": {
            "album": f"https://www.deezer.com/album/{item_id}",
            "track": f"https://www.deezer.com/track/{item_id}",
            "artist": f"https://www.deezer.com/artist/{item_id}",
            "playlist": f"https://www.deezer.com/playlist/{item_id}",
        },
        "soundcloud": {
            "track": f"https://soundcloud.com/{item_id}",
            "album": f"https://soundcloud.com/{item_id}",
            "playlist": f"https://soundcloud.com/{item_id}",
        },
    }

    if source in url_patterns and media_type in url_patterns[source]:
        return url_patterns[source][media_type]

    return f"https://open.{source}.com/{media_type}/{item_id}"


def extract_metadata_from_url(url):
    metadata = {
        "service": None,
        "type": None,
        "id": None,
        "title": None,
        "artist": None,
        "album_art": None,
    }

    try:
        if "spotify.com" in url:
            metadata["service"] = "spotify"
            match = re.search(r"/(album|track|playlist|artist)/([a-zA-Z0-9]+)", url)
            if match:
                metadata["type"] = match.group(1)
                metadata["id"] = match.group(2)
                # Note: Spotify requires OAuth for metadata, so we can't easily fetch it

        elif "qobuz.com" in url:
            metadata["service"] = "qobuz"
            match = re.search(r"/(album|track|playlist|artist)/([0-9]+)", url)
            if match:
                metadata["type"] = match.group(1)
                metadata["id"] = match.group(2)
                metadata.update(fetch_qobuz_metadata(metadata["id"], metadata["type"]))

        elif "tidal.com" in url:
            metadata["service"] = "tidal"
            match = re.search(r"/(album|track|playlist|artist)/([0-9]+)", url)
            if match:
                metadata["type"] = match.group(1)
                metadata["id"] = match.group(2)
                metadata["album_art"] = (
                    f"https://resources.tidal.com/images/{metadata['id']}/320x320.jpg"
                )

        elif "deezer.com" in url:
            metadata["service"] = "deezer"
            match = re.search(r"/(album|track|playlist|artist)/([0-9]+)", url)
            if match:
                metadata["type"] = match.group(1)
                metadata["id"] = match.group(2)
                metadata.update(fetch_deezer_metadata(metadata["id"], metadata["type"]))

    except Exception as e:
        logger.error(f"Error extracting metadata from URL: {e}")

    return metadata


def fetch_qobuz_metadata(item_id, item_type):
    metadata = {}
    try:
        app_id = get_qobuz_app_id()
        api_base = "https://www.qobuz.com/api.json/0.2"

        if item_type == "album":
            response = requests.get(
                f"{api_base}/album/get",
                params={"album_id": item_id, "app_id": app_id},
                timeout=5,
            )
            if response.status_code == 200:
                data = response.json()
                metadata["title"] = data.get("title", "")
                metadata["artist"] = data.get("artist", {}).get("name", "")
                if "image" in data:
                    for size in ["small", "medium", "large", "thumbnail"]:
                        if size in data["image"]:
                            metadata["album_art"] = data["image"][size]
                            break

        elif item_type == "track":
            response = requests.get(
                f"{api_base}/track/get",
                params={"track_id": item_id, "app_id": app_id},
                timeout=5,
            )
            if response.status_code == 200:
                data = response.json()
                metadata["title"] = data.get("title", "")
                metadata["artist"] = data.get("performer", {}).get("name", "")
                album = data.get("album", {})
                if "image" in album:
                    for size in ["small", "medium", "large", "thumbnail"]:
                        if size in album["image"]:
                            metadata["album_art"] = album["image"][size]
                            break

    except Exception as e:
        logger.error(f"Error fetching Qobuz metadata: {e}")

    return metadata


def fetch_deezer_metadata(item_id, item_type):
    metadata = {}
    try:
        api_base = "https://api.deezer.com"

        if item_type == "album":
            response = requests.get(f"{api_base}/album/{item_id}", timeout=5)
            if response.status_code == 200:
                data = response.json()
                metadata["title"] = data.get("title", "")
                metadata["artist"] = data.get("artist", {}).get("name", "")
                metadata["album_art"] = data.get("cover_medium", "")

        elif item_type == "track":
            response = requests.get(f"{api_base}/track/{item_id}", timeout=5)
            if response.status_code == 200:
                data = response.json()
                metadata["title"] = data.get("title", "")
                metadata["artist"] = data.get("artist", {}).get("name", "")
                album = data.get("album", {})
                metadata["album_art"] = album.get("cover_medium", "")

    except Exception as e:
        logger.error(f"Error fetching Deezer metadata: {e}")
    return metadata


if __name__ == "__main__":
    logger.info("Starting Streamrip Web application...")
    logger.info(f"Config path: {STREAMRIP_CONFIG}")
    logger.info(f"Download directory: {DOWNLOAD_DIR}")
    logger.info(f"Max concurrent downloads: {MAX_CONCURRENT_DOWNLOADS}")
    app.run(host="0.0.0.0", port=5000, debug=False)
