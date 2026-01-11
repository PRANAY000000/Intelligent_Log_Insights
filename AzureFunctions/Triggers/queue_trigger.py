# Triggers/queue_trigger.py
import os
import json
import logging
import time
import threading
from typing import Iterable, Any, Dict
from azure.cosmos import CosmosClient, exceptions as cosmos_exceptions
from azure.cosmos.exceptions import CosmosHttpResponseError  # correct import
# NOTE: if CosmosHttpResponseError import fails in your environment,
# use: from azure.cosmos.exceptions import CosmosHttpResponseError

# Thread-safe global Cosmos client (reuse connection across all calls)
_cosmos_lock = threading.Lock()
_cosmos_client = None
_db = None
_container = None

# ---------- configuration ----------
COSMOS_CONN = os.getenv("COSMOSDB_CONN_STRING")
COSMOS_DB = os.getenv("COSMOSDB_DB_NAME", "ProcessedLogs")
COSMOS_CONTAINER = os.getenv("COSMOSDB_CONTAINER_NAME", "LogsStored")
# -----------------------------------

def _get_container():
    global _cosmos_client, _db, _container
    if _container:
        return _container
    with _cosmos_lock:
        if _container:
            return _container
        if not COSMOS_CONN:
            raise RuntimeError("COSMOSDB_CONN_STRING environment variable not set.")
        # avoid certificate verification warnings in dev; recommended: remove connection_verify=False in prod
        _cosmos_client = CosmosClient.from_connection_string(COSMOS_CONN, connection_verify=False)
        _db = _cosmos_client.get_database_client(COSMOS_DB)
        _container = _db.get_container_client(COSMOS_CONTAINER)
    return _container


def _safe_json_load(s: str):
    try:
        return json.loads(s)
    except Exception:
        try:
            return json.loads(s.replace("'", '"'))
        except Exception:
            return {"raw": s}


def _get_value_flex(obj: Dict[str, Any], candidates: Iterable[str], default=None):
    if not isinstance(obj, dict):
        return default
    lowered = {k.lower(): v for k, v in obj.items()}
    for c in candidates:
        if c is None:
            continue
        v = lowered.get(c.lower())
        if v is not None:
            return v
    return default


def _safe_upsert(container, doc, retries=5):
    """Retry on 429 or transient network issues."""
    for i in range(retries):
        try:
            container.upsert_item(doc)
            return
        except cosmos_exceptions.CosmosHttpResponseError as e:
            # retry on 429 (throttling). For other status codes, re-raise.
            status = getattr(e, "status_code", None)
            logging.warning(f"Cosmos upsert error (attempt {i+1}/{retries}): {e} (status={status})")
            if status == 429:
                delay = 0.5 * (i + 1)
                time.sleep(delay)
                continue
            raise
        except Exception as e:
            logging.exception("Unexpected error during Cosmos upsert")
            raise


def _extract_system_props(sb_msg) -> Dict[str, Any]:
    """
    Try to pull as much metadata as possible from the azure.functions.ServiceBusMessage object.
    This is defensive because local SDK / bindings may differ from Azure host.
    """
    meta = {}
    try:
        # Try direct attributes that exist on azure.functions.ServiceBusMessage
        if hasattr(sb_msg, "message_id"):
            meta["message_id"] = getattr(sb_msg, "message_id")
        if hasattr(sb_msg, "delivery_count"):
            meta["delivery_count"] = getattr(sb_msg, "delivery_count")
        # system_properties is sometimes present and contains sequence_number/lock_token/enqueued_time etc.
        if hasattr(sb_msg, "system_properties") and sb_msg.system_properties:
            meta.update({k: v for k, v in sb_msg.system_properties.items()})
        # Some runtime exposes .get_system_properties() or .get_message() — best effort:
        if hasattr(sb_msg, "get_system_properties"):
            try:
                sp = sb_msg.get_system_properties()
                if isinstance(sp, dict):
                    meta.update(sp)
            except Exception:
                pass
    except Exception:
        logging.exception("error extracting system properties from ServiceBusMessage")
    return meta


def handle_message(azservicebus):
    """
    Called by function_app.process_log_message.
    azservicebus is azure.functions.ServiceBusMessage.
    """
    try:
        logging.info("Queue handler start.")
        sys_meta = _extract_system_props(azservicebus)
        logging.info(f"ServiceBus metadata: {json.dumps(sys_meta, default=str)}")

        raw = None
        try:
            if hasattr(azservicebus, "get_body"):
                raw = azservicebus.get_body().decode("utf-8")
            else:
                raw = str(azservicebus)
        except Exception:
            raw = str(azservicebus)

        payload = _safe_json_load(raw)

        # Unwrap common envelope shapes
        if isinstance(payload, dict) and len(payload) == 1 and next(iter(payload)).lower() in ("message", "body", "data"):
            inner = next(iter(payload.values()))
            if isinstance(inner, dict):
                payload = inner

        # Extract fields (flexible)
        request_id = _get_value_flex(payload, ["RequestId", "request_id", "id", "Id"])
        app_name = _get_value_flex(payload, ["AppName", "app_name", "application"], "Unknown")
        level = _get_value_flex(payload, ["Level", "level", "severity"], "Information")
        message = _get_value_flex(payload, ["Message", "message", "msg", "log"], str(payload)[:500])
        timestamp = _get_value_flex(payload, ["TimeGenerated", "timestamp", "time", "Time"])
        user_id = _get_value_flex(payload, ["UserId", "user_id", "User"])
        status_code = _get_value_flex(payload, ["StatusCode", "status_code", "status"])
        file_name = _get_value_flex(payload, ["FileName", "file", "filename"])

        lvl = str(level).lower()
        severity = "High" if "error" in lvl else "Medium" if "warn" in lvl else "Low"

        enriched_log = {
            "id": request_id or f"generated-{os.urandom(8).hex()}",
            "AppName": app_name,
            "Level": level,
            "Message": message,
            "Severity": severity,
            "UserId": user_id,
            "StatusCode": status_code,
            "FileName": file_name,
            "Timestamp": timestamp,
            # Add ServiceBus metadata so you can trace queue -> cosmos document
            "ServiceBusMetadata": sys_meta,
            # store original body (but avoid huge blobs if you have them)
            "OriginalPayload": payload
        }

        container = _get_container()
        _safe_upsert(container, enriched_log)
        logging.info(f"Stored/enriched log: {enriched_log['id']} (App={app_name} Level={level})")

    except Exception as e:
        logging.exception(f"Error in queue handle_message: {e}")
        # If the function raises, with autoComplete=false the runtime will abandon -> delivery_count++.
        # With autoComplete=true, the runtime completes if function returns without raising.
        # We re-raise to let the host know there was a failure (if appropriate).
        raise
