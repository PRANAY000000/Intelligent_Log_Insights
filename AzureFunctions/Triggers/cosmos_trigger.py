# Triggers/cosmos_trigger.py
import os
import uuid
import logging
import json
from datetime import datetime
from typing import Iterable, Any, Dict

from azure.cosmos import CosmosClient

# Initialize Cosmos DB client & Insight container (writes insights)
COSMOS_CONN = os.getenv("COSMOSDB_CONN_STRING")
COSMOS_DB = os.getenv("COSMOSDB_DB_NAME", "ProcessedLogs")
INSIGHT_CONTAINER = os.getenv("COSMOSDB_INSIGHT_CONTAINER", "TriggerInsights")

if not COSMOS_CONN:
    logging.warning("COSMOSDB_CONN_STRING not set. Insight upserts will fail if attempted.")

# create client lazily so module import doesn't fail if env missing
_cosmos_client = CosmosClient.from_connection_string(COSMOS_CONN, connection_verify=False) if COSMOS_CONN else None
_db = _cosmos_client.get_database_client(COSMOS_DB) if _cosmos_client else None
_insight_container = _db.get_container_client(INSIGHT_CONTAINER) if _db else None


def _doc_to_dict(d: Any) -> Dict[str, Any]:
    """
    Try to convert an arbitrary change-feed doc to a plain dict.
    Handles:
      - dict (return as-is)
      - objects supporting dict(d)
      - objects supporting .to_json() -> JSON string
      - fallback: attempt to read common attributes
    """
    if isinstance(d, dict):
        return d

    # try dict() (some azure types behave like mappings)
    try:
        return dict(d)
    except Exception:
        pass

    # try to_json() (azure.functions.Document often exposes to_json())
    try:
        if hasattr(d, "to_json"):
            raw = d.to_json()
            return json.loads(raw)
    except Exception:
        pass

    # try __dict__ (object with attributes)
    try:
        if hasattr(d, "__dict__") and isinstance(d.__dict__, dict) and d.__dict__:
            return {k: v for k, v in d.__dict__.items() if not k.startswith("_")}
    except Exception:
        pass

    # last-resort: pull some commonly-needed keys via getattr
    keys = ["Level", "level", "Severity", "severity", "AppName", "app_name", "Application",
            "Message", "message", "RequestId", "request_id", "id", "TimeGenerated", "timestamp"]
    out = {}
    for k in keys:
        if hasattr(d, k):
            out[k] = getattr(d, k)
    if out:
        return out

    # give up — return a representation so we don't break
    return {"_raw_repr": str(d)}


def _flex_get(d: Any, keys, default=None):
    """
    Robust field extraction from a document of unknown type.
    keys: iterable of possible field names (case-insensitive)
    """
    try:
        doc = _doc_to_dict(d)
    except Exception:
        return default

    if not isinstance(doc, dict):
        return default

    lowered = {k.lower(): v for k, v in doc.items()}
    for k in keys:
        if k is None:
            continue
        v = lowered.get(k.lower())
        if v is not None:
            return v
    return default


def handle_cosmos_changes(documents: Iterable[Any]):
    """
    Called by function_app.py when Cosmos DB change feed raises documents.
    Documents may be azure.functions.Document objects or plain dicts; this
    implementation normalizes them and counts levels safely.
    """
    try:
        # small sanity debug: log the types / keys of first few docs
        sample_docs = []
        for i, doc in enumerate(documents):
            if i >= 5:
                break
            try:
                converted = _doc_to_dict(doc)
                sample_docs.append({
                    "type": type(doc).__name__,
                    "keys": list(converted.keys())[:20]  # don't spam logs
                })
            except Exception:
                sample_docs.append({"type": type(doc).__name__, "keys": ["<could not convert>"]})

        logging.info(f"Cosmos trigger received {len(list(documents))} docs (sample types/keys): {json.dumps(sample_docs, default=str)}")

        # Re-iterate properly: documents might be a generator-like; convert to list
        docs = list(documents)
        if not docs:
            logging.info("No documents to process.")
            return

        error_count = 0
        warning_count = 0
        info_count = 0
        service_errors = {}

        processed = 0
        for raw_doc in docs:
            processed += 1
            # normalize to dict for flexible key access
            doc = _doc_to_dict(raw_doc)

            # Defensive: skip insight docs or system docs if they don't look like logs
            # Consider a log doc to have at least one of these keys: Level, Message, AppName, RequestId
            if not any(k in {k_.lower() for k_ in doc.keys()} for k in ("level", "message", "appname", "requestid")):
                logging.debug(f"Skipping non-log doc (id if present): {doc.get('id') if isinstance(doc, dict) else '<unknown>'}")
                continue

            level = _flex_get(doc, ["Level", "level", "Severity", "severity"], default="Information")
            app_name = _flex_get(doc, ["AppName", "app_name", "Application"], default="Unknown")

            # defensive normalize: sometimes level stored under OriginalPayload.Level
            if level is None or (isinstance(level, str) and level.strip() == ""):
                orig = doc.get("OriginalPayload") or {}
                level = _flex_get(orig, ["Level", "level", "Severity", "severity"], default="Information")

            lvl = str(level).lower() if level is not None else "information"
            if "error" in lvl:
                error_count += 1
                service_errors[app_name] = service_errors.get(app_name, 0) + 1
            elif "warn" in lvl:
                warning_count += 1
            else:
                info_count += 1

        total = error_count + warning_count + info_count
        error_rate = round((error_count / total) * 100, 2) if total > 0 else 0

        if error_rate > 30:
            status = "CRITICAL"
        elif error_rate > 10:
            status = "WARNING"
        else:
            status = "STABLE"

        top_service = max(service_errors, key=service_errors.get) if service_errors else "None"

        insight_doc = {
            "id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "total_logs": total,
            "processed_batches": processed,
            "error_count": error_count,
            "warning_count": warning_count,
            "info_count": info_count,
            "error_rate_percent": error_rate,
            "status": status,
            "top_error_service": top_service,
            "service_error_breakdown": service_errors
        }

        # debug log to show what we'll upsert
        logging.info(f"Computed insight: {json.dumps({k: insight_doc[k] for k in ['id','total_logs','error_count','warning_count','info_count','status']})}")

        if _insight_container:
            _insight_container.upsert_item(insight_doc)
            logging.info(f" Insight added to {INSIGHT_CONTAINER}: {status} (Errors={error_count}, Warnings={warning_count})")
        else:
            logging.warning("Insight container client not available; skipping upsert.")
            logging.debug(f"Would have written insight: {insight_doc}")

    except Exception as e:
        logging.exception(f"Error while processing cosmos change docs: {e}")
        # Do NOT re-raise: we don't want change-feed to poison; but you may choose to re-raise during debugging.
