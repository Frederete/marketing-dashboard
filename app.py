"""
Flask application — serves the dashboard and exposes API endpoints.
Serverless-compatible: no background threads, lazy on-demand fetching.
Cache lives in module-level variables (warm between requests on same instance).
"""

import os
import logging
import time
from datetime import datetime

from flask import Flask, jsonify, render_template, request
from dotenv import load_dotenv

import data_processor as dp
import insights_engine as ie

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(
    __name__,
    static_folder=os.path.join(os.path.dirname(__file__), "static"),
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")

SHEETS_REFRESH_INTERVAL   = int(os.getenv("SHEETS_REFRESH_INTERVAL",   60))
INSIGHTS_REFRESH_INTERVAL = int(os.getenv("INSIGHTS_REFRESH_INTERVAL", 300))

# ─── Module-level cache (survives across warm requests on same instance) ──────

_data_cache:     dict | None = None
_insights_cache: dict | None = None
_raw_frames:     dict | None = None
_last_sheets_fetch   = 0.0
_last_insights_fetch = 0.0


def _needs_sheets_refresh(force: bool = False) -> bool:
    return force or (time.time() - _last_sheets_fetch) >= SHEETS_REFRESH_INTERVAL


def _refresh_sheets(force: bool = False) -> None:
    global _data_cache, _raw_frames, _last_sheets_fetch
    if not _needs_sheets_refresh(force):
        return
    try:
        frames  = dp.fetch_all_sheets()
        payload = dp.build_dashboard_data(frames)
        _raw_frames        = frames
        _data_cache        = payload
        _last_sheets_fetch = time.time()
        logger.info("Sheets cache refreshed.")
    except Exception as e:
        logger.error("Failed to refresh Sheets: %s", e)
        if _data_cache is not None:
            _data_cache["meta"]["stale"] = True
        else:
            _data_cache = {**dp._empty_payload(), "meta": {
                "last_sync":  datetime.utcnow().isoformat() + "Z",
                "stale":      True,
                "error":      str(e),
                "date_range": {"start": None, "end": None},
            }}


def _refresh_insights(force: bool = False) -> None:
    global _insights_cache, _last_insights_fetch
    if not force and (time.time() - _last_insights_fetch) < INSIGHTS_REFRESH_INTERVAL:
        return
    if not _data_cache:
        return
    try:
        _insights_cache = {
            "alerts":          ie.generate_rule_alerts(_data_cache),
            "ai_insights":     ie.generate_ai_insights(_data_cache),
            "recommendations": ie.generate_recommendations(_data_cache),
            "generated_at":    datetime.utcnow().isoformat() + "Z",
        }
        _last_insights_fetch = time.time()
        logger.info("Insights cache refreshed.")
    except Exception as e:
        logger.error("Failed to refresh insights: %s", e)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get_data_with_filters() -> dict | None:
    _refresh_sheets()          # no-op if cache is fresh
    if _data_cache is None:
        return None

    date_start = request.args.get("date_start")
    date_end   = request.args.get("date_end")
    campaign   = request.args.get("campaign")

    if (date_start or date_end or campaign) and _raw_frames:
        try:
            filtered = dp.build_dashboard_data(_raw_frames, date_start, date_end)
            if campaign:
                filtered["performance_table"] = [
                    c for c in filtered["performance_table"]
                    if campaign.lower() in c["name"].lower()
                ]
            return filtered
        except Exception as e:
            logger.warning("Filter processing failed: %s", e)

    return _data_cache


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    data = _get_data_with_filters()
    if data is None:
        return jsonify({"error": "Não foi possível carregar os dados. Verifique a conexão com o Google Sheets."}), 503
    return jsonify(data)


@app.route("/api/insights")
def api_insights():
    _refresh_sheets()
    _refresh_insights()

    if _insights_cache:
        return jsonify(_insights_cache)

    if _data_cache:
        return jsonify({
            "alerts":          ie.generate_rule_alerts(_data_cache),
            "ai_insights":     {"text": None, "model": None, "error": "Gerando insights..."},
            "recommendations": ie.generate_recommendations(_data_cache),
            "generated_at":    datetime.utcnow().isoformat() + "Z",
        })

    return jsonify({"error": "Insights não disponíveis ainda."}), 503


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    _refresh_sheets(force=True)
    return jsonify(_data_cache or {"error": "Falha ao atualizar dados."})


@app.route("/api/health")
def api_health():
    return jsonify({
        "sheets_connected":  _data_cache is not None and not _data_cache.get("meta", {}).get("stale", False),
        "last_sheets_sync":  _data_cache["meta"]["last_sync"] if _data_cache else None,
        "cache_age_seconds": round(time.time() - _last_sheets_fetch),
        "insights_ready":    _insights_cache is not None,
    })


@app.route("/api/campaigns")
def api_campaigns():
    _refresh_sheets()
    if not _data_cache:
        return jsonify([])
    names = [c["name"] for c in _data_cache.get("performance_table", [])]
    return jsonify(sorted(set(names)))


if __name__ == "__main__":
    port  = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
