import os
import json
import time
import torch
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer, util
from .cosmos_client import logs_container, insights_container


# ==============================================================
#  Setup Environment & Model
# ==============================================================
load_dotenv()
hf_token = os.getenv("HUGGINGFACE_HUB_TOKEN", None)
model_name = "sentence-transformers/all-MiniLM-L6-v2"

print(f" Loading model: {model_name} ...")
try:
    model = SentenceTransformer(model_name, token=hf_token)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    print(f" Model loaded successfully on {device}")
except Exception as e:
    print(f" Failed to load SentenceTransformer: {e}")
    model = None


# ==============================================================
#  Paths & Constants
# ==============================================================
HISTORY_PATH = "search_results.json"
CACHE = {"logs": [], "insights": [], "last_refresh": None}


# ==============================================================
#  History Functions (Safe Save/Load)
# ==============================================================
def load_history():
    """Load structured search history safely (create dict if invalid)."""
    if not os.path.exists(HISTORY_PATH):
        return {"service_level": [], "system_health": [], "semantic": []}

    try:
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            data = {"service_level": [], "system_health": [], "semantic": data}
        elif not isinstance(data, dict):
            data = {"service_level": [], "system_health": [], "semantic": []}
        return data
    except Exception as e:
        print(f" Failed to read history: {e}")
        return {"service_level": [], "system_health": [], "semantic": []}


def save_history(entry: dict):
    """Save structured search results (grouped by query type)."""
    try:
        history = load_history()
        query_type = entry.get("type", "semantic")
        if query_type not in history:
            history[query_type] = []

        history[query_type].append(entry)
        history[query_type] = history[query_type][-10:]

        with open(HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2)
    except Exception as e:
        print(f"Failed to save history: {e}")


# ==============================================================
#  CosmosDB Data Fetch & Caching
# ==============================================================
def fetch_all_data(hours: int = None):
    """Fetch all logs & insights (optionally filtered by time)."""
    try:
        if hours:
            since_ts = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())
            time_filter = f"WHERE c._ts >= {since_ts}"
        else:
            time_filter = ""

        logs_query = f"SELECT * FROM c {time_filter} ORDER BY c._ts DESC"
        insights_query = f"SELECT * FROM c {time_filter} ORDER BY c._ts DESC"

        logs = list(logs_container.query_items(query=logs_query, enable_cross_partition_query=True, max_item_count=2000))
        insights = list(insights_container.query_items(query=insights_query, enable_cross_partition_query=True, max_item_count=500))

        print(f" Loaded {len(logs)} logs and {len(insights)} insights from CosmosDB.")
        return logs, insights
    except Exception as e:
        print(f"Error fetching from CosmosDB: {e}")
        return [], []


def refresh_cache(force: bool = False):
    """Reload cache from CosmosDB."""
    global CACHE
    now = datetime.now(timezone.utc)

    if CACHE["last_refresh"] and not force:
        delta = (now - CACHE["last_refresh"]).total_seconds()
        if delta < 10:
            print(f" Cache refreshed recently ({delta:.1f}s ago). Skipping reload.")
            return CACHE

    logs, insights = fetch_all_data()
    CACHE = {"logs": logs, "insights": insights, "last_refresh": now}
    print(f" Cache refreshed — {len(logs)} logs, {len(insights)} insights @ {now.isoformat()}")
    return CACHE


# ==============================================================
#  Embedding Helper
# ==============================================================
def encode_texts(texts):
    if not model:
        raise RuntimeError("SentenceTransformer model not loaded.")
    texts = [str(t) if t else "" for t in texts]
    return model.encode(texts, convert_to_tensor=True, normalize_embeddings=True).to(model.device)


# ==============================================================
#  Semantic Search
# ==============================================================
def semantic_search(query, logs, top_k=5):
    if not logs or not query:
        return []

    corpus = [
        f"{l.get('Message', '')} | App: {l.get('AppName', '')} | Level: {l.get('Level', '')}"
        for l in logs
    ]

    query_emb = encode_texts([query])
    corpus_emb = encode_texts(corpus)

    sims = util.cos_sim(query_emb, corpus_emb)[0]
    k = min(top_k, len(corpus))
    top_results = torch.topk(sims, k=k)

    results = []
    for score, idx in zip(top_results.values.cpu().tolist(), top_results.indices.cpu().tolist()):
        log = logs[idx]
        results.append({
            "AppName": log.get("AppName", "Unknown"),
            "Level": log.get("Level", "N/A"),
            "Message": log.get("Message", "")[:150],
            "Timestamp": log.get("Timestamp", "N/A"),
            "Severity": log.get("Severity", "N/A"),
            "score": round(float(score), 3)
        })
    return results


# ==============================================================
# 🔍 Intelligent Search Core
# ==============================================================
def intelligent_search(query: str, top_k: int = 5, from_time: str = None, to_time: str = None):
    """Smart hybrid search for insights and errors."""
    data = refresh_cache(force=True)
    logs, insights = data["logs"], data["insights"]
    q = query.lower().strip()

    # ----------------------------------------------------------
    # A 🔶 Service-Level Insights
    # ----------------------------------------------------------
    if "error" in q or "service" in q or "critical" in q:
        error_logs = [l for l in logs if l.get("Level", "").lower() in ["error", "critical"]]
        counts = Counter(l.get("AppName", "Unknown") for l in error_logs)
        top_services = counts.most_common(5)
        latest = max(insights, key=lambda x: x.get("_ts", 0), default={})

        timelines = defaultdict(lambda: defaultdict(int))
        for l in error_logs:
            app = l.get("AppName", "Unknown")
            ts = datetime.fromisoformat(l.get("Timestamp").replace("Z", "+00:00"))
            hour_key = ts.replace(minute=0, second=0, microsecond=0).isoformat()
            timelines[app][hour_key] += 1

        service_timelines = {
            app: [{"timestamp": k, "error_count": v} for k, v in sorted(hour_map.items())]
            for app, hour_map in timelines.items()
        }

        result = {
            "type": "service_level",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "query": query,
            "total_error_services": len(counts),
            "top_error_services": [{"service": a, "error_count": c} for a, c in top_services],
            "latest_status": latest.get("status", "N/A"),
            "latest_error_rate": latest.get("error_rate_percent", 0),
            "per_service_timeline": service_timelines
        }

        for service in counts:
            if service.lower() in q:
                svc_logs = [l for l in error_logs if l.get("AppName", "").lower() == service.lower()]
                result["service_details"] = {
                    "service": service,
                    "error_count": len(svc_logs),
                    "last_error_timestamp": max([l.get("Timestamp", "N/A") for l in svc_logs], default="N/A")
                }

        save_history(result)
        return result

    # ----------------------------------------------------------
    # B 🔷 System-Level Insights
    # ----------------------------------------------------------
    elif "system" in q or "stable" in q or "health" in q or "trend" in q:
        if not insights:
            return {"message": "No insights available."}

        latest = max(insights, key=lambda x: x.get("_ts", 0))
        statuses = [i.get("status", "UNKNOWN") for i in insights]
        critical_times = [
            datetime.fromisoformat(i["timestamp"].replace("Z", ""))
            for i in insights if i.get("status") == "CRITICAL"
        ]

        mtbf = None
        if len(critical_times) > 1:
            deltas = [(critical_times[i] - critical_times[i+1]).total_seconds()/3600 for i in range(len(critical_times)-1)]
            mtbf = round(sum(deltas)/len(deltas), 2)

        timeline = {}
        for i in insights:
            ts = datetime.fromisoformat(i["timestamp"].replace("Z", "+00:00"))
            hour = ts.replace(minute=0, second=0, microsecond=0).isoformat()
            timeline[hour] = timeline.get(hour, 0) + i.get("error_count", 0)

        result = {
            "type": "system_health",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "query": query,
            "latest_status": latest.get("status", "UNKNOWN"),
            "error_rate_percent": latest.get("error_rate_percent", 0),
            "top_error_service": latest.get("top_error_service", "N/A"),
            "mean_time_between_failures_hrs": mtbf or "N/A",
            "trend": "Degrading" if statuses[:3].count("CRITICAL") > 1 else "Stable",
            "records_analyzed": len(insights),
            "timeline": [{"timestamp": k, "error_count": v} for k, v in sorted(timeline.items())]
        }

        save_history(result)
        return result

    # ----------------------------------------------------------
    # C 🟪 Semantic FALLBACK  (IMPROVED 🔥)
    # ----------------------------------------------------------
    else:
        combined = logs + insights

        # 🟩 🟩 🟩 IMPROVEMENT: Detect failure queries and prioritize ERROR logs
        FAILURE_KEYWORDS = ["fail", "failed", "failure", "error", "critical", "issue", "crash"]

        if any(word in q for word in FAILURE_KEYWORDS):
            print("⚠ Failure intent detected → prioritizing ERROR logs")

            failure_logs = [
                l for l in logs
                if l.get("Level", "").lower() in ["error", "critical"]
            ]

            if failure_logs:
                combined = failure_logs

        # Run semantic vector search
        matches = semantic_search(query, combined, top_k=top_k)

        result = {
            "type": "semantic",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "query": query,
            "count": len(matches),
            "semantic_matches": matches
        }

        save_history(result)
        return result


# ==============================================================
#  Exportable Utilities
# ==============================================================
def search_history():
    return load_history()
