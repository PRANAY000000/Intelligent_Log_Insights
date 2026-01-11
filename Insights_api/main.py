from fastapi import FastAPI, Query, HTTPException
from typing import Optional
from datetime import datetime, timezone, timedelta
from .cosmos_client import logs_container, insights_container
from .intelligent_search import intelligent_search, search_history, refresh_cache
from fastapi import Query

import os
import logging

app = FastAPI(
    title="Intelligent Log Insights API",
    description="Provides analytics and semantic search over system logs.",
    version=os.getenv("APP_VERSION", "1.0.0")
)

# 1️ Fetch filtered logs
@app.get("/logs")
def get_logs(level: Optional[str] = Query(None), service: Optional[str] = Query(None)):
    filters = []
    if level:
        filters.append(f"LOWER(c.Level) = LOWER('{level}')")
    if service:
        filters.append(f"LOWER(c.AppName) = LOWER('{service}')")

    where_clause = " AND ".join(filters) if filters else "1=1"
    query = f"SELECT * FROM c WHERE {where_clause} ORDER BY c._ts DESC"

    results = list(logs_container.query_items(query=query, enable_cross_partition_query=True))
    return {
        "filters": {"level": level, "service": service},
        "count": len(results),
        "data": results
    }




# 2️ Return analytics on recurring errors
@app.get("/analytics/errors")
def get_error_analytics():
    """
    Fallback: compute error counts manually without Cosmos GROUP BY.
    """
    try:
        # 1️ Get all error-level logs
        query = "SELECT c.AppName, c.Level FROM c WHERE LOWER(c.Level) = 'error'"
        items = list(logs_container.query_items(query=query, enable_cross_partition_query=True))

        if not items:
            return {"message": "No error logs found.", "top_error_services": []}

        #  Group by AppName manually
        from collections import Counter
        app_counter = Counter(i.get("AppName", "Unknown") for i in items)
        total_errors = sum(app_counter.values()) or 1

        #  Format the response
        results = [
            {
                "service": app,
                "error_count": count,
                "error_percentage": round((count / total_errors) * 100, 2)
            }
            for app, count in app_counter.most_common(10)
        ]

        return {
            "container": "LogsStored",
            "timestamp": datetime.utcnow().isoformat(),
            "total_error_logs": total_errors,
            "top_error_services": results,
            "status": "CRITICAL" if total_errors > 100 else "STABLE"
        }

    except Exception as e:
        logging.exception("Error during analytics query")
        raise HTTPException(status_code=500, detail=f"Analytics query failed: {e}")



@app.get("/analytics/errors/timeline")
def get_error_timeline(
    start_time: Optional[str] = Query(None, description="Start ISO timestamp"),
    interval_minutes: int = Query(5, description="Time bucket in minutes (default=5)")
):
    """
    Intelligent error timeline grouping:
    - Converts timestamps to IST
    - Recent buckets shown first
    """

    query = "SELECT c.Timestamp, c._ts FROM c WHERE LOWER(c.Level) = 'error'"

    if start_time:
        query += f" AND c.Timestamp >= '{start_time}'"

    results = list(logs_container.query_items(query=query, enable_cross_partition_query=True))
    if not results:
        return {"message": "No errors found in this time range."}

    from collections import Counter
    from datetime import timezone, timedelta
    import dateutil.parser

    IST = timezone(timedelta(hours=5, minutes=30))
    buckets = Counter()

    for r in results:
        ts_value = r.get("Timestamp")
        if ts_value:
            ts = dateutil.parser.parse(ts_value).astimezone(IST)
        else:
            ts = datetime.fromtimestamp(r["_ts"], tz=IST)   # fallback

        bucket_key = ts.replace(
            second=0, microsecond=0,
            minute=(ts.minute // interval_minutes) * interval_minutes
        )
        buckets[str(bucket_key)] += 1

    timeline = [
        {"timestamp": k, "error_count": v}
        for k, v in sorted(buckets.items(), reverse=True)   # LATEST FIRST
    ]

    return {
        "interval_minutes": interval_minutes,
        "total_intervals": len(timeline),
        "timeline": timeline
    }






@app.post("/analytics/intelligent_search")
def perform_intelligent_search(
    query: str,
    top_k: int = 5,
    from_time: str = None,
    to_time: str = None
):
    """
    Hybrid intelligent search:
    Uses SentenceTransformer-based semantic analysis
    and rule-based reasoning for insights.
    """
    try:
        result = intelligent_search(query=query, top_k=top_k, from_time=from_time, to_time=to_time)
        return {
            "status": "success",
            "query": query,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "results": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Intelligent search failed: {e}")






#  System health & telemetry
@app.get("/health")
def get_system_health(
    since: Optional[str] = Query(None, description="ISO timestamp or '1h', '1d' to fetch health data since"),
    last_n: int = Query(10, ge=1, le=100, description="Number of recent summaries to return")
):
    """
    Advanced System Health API:
    - Fetches recent health summaries from TriggerInsights.
    - Supports ?since=<timestamp or relative time> (e.g., ?since=1h).
    - Computes trend and Mean Time Between Failures (MTBF).
    """

    try:
        #  Parse 'since' parameter (either ISO timestamp or relative)
        time_filter = None
        if since:
            if since.endswith("h"):
                hours = int(since[:-1])
                time_filter = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
            elif since.endswith("d"):
                days = int(since[:-1])
                time_filter = (datetime.utcnow() - timedelta(days=days)).isoformat()
            else:
                try:
                    time_filter = datetime.fromisoformat(since).isoformat()
                except Exception:
                    raise HTTPException(status_code=400, detail="Invalid 'since' format. Use ISO8601 or like 2h / 1d.")

        #  Build query for TriggerInsights
        if time_filter:
            query = f"SELECT * FROM c WHERE c.timestamp >= '{time_filter}' ORDER BY c.timestamp DESC"
        else:
            query = f"SELECT TOP {last_n} * FROM c ORDER BY c.timestamp DESC"

        items = list(insights_container.query_items(query=query, enable_cross_partition_query=True))

        if not items:
            return {"status": "UNKNOWN", "message": "No health data found in TriggerInsights"}

        #  Compute latest health and historical trend
        latest = items[0]
        statuses = [doc.get("status", "UNKNOWN") for doc in items]
        critical_times = [
            datetime.fromisoformat(doc["timestamp"].replace("Z", ""))
            for doc in items if doc.get("status") == "CRITICAL"
        ]

        #  Compute Mean Time Between Failures (MTBF)
        mtbf_hours = None
        if len(critical_times) > 1:
            deltas = [
                (critical_times[i] - critical_times[i + 1]).total_seconds() / 3600
                for i in range(len(critical_times) - 1)
            ]
            mtbf_hours = round(sum(deltas) / len(deltas), 2)

        #  Identify trend (based on most recent 3 statuses)
        trend = "Improving" if statuses[:3].count("CRITICAL") == 0 else "Degrading"

        #  Construct output
        health_summary = {
            "latest_status": latest.get("status", "UNKNOWN"),
            "timestamp": latest.get("timestamp"),
            "error_rate_percent": latest.get("error_rate_percent", 0),
            "total_logs": latest.get("total_logs", 0),
            "top_error_service": latest.get("top_error_service", "None"),
            "status_trend": trend,
            "mean_time_between_failures_hrs": mtbf_hours or "N/A",
            "records_analyzed": len(items),
            "historical_statuses": statuses,
            "container": "TriggerInsights",
        }

        #  Add visual indicator message
        if latest["status"] == "STABLE":
            health_summary["message"] = "🟢 System is stable and healthy."
        elif latest["status"] == "WARNING":
            health_summary["message"] = "🟠 Elevated error rate detected. Monitor closely."
        else:
            health_summary["message"] = "🔴 CRITICAL: Immediate action required."

        return health_summary

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health computation failed: {e}")
