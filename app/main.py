from fastapi import FastAPI, Request, BackgroundTasks
from dotenv import load_dotenv
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from app.la_post import send_log
import os
import json
import time
import logging

# ---- Setup ----
load_dotenv()

SERVICEBUS_CONNECTION = os.getenv("SERVICE_BUS_CONNECTION")
SERVICEBUS_QUEUE = os.getenv("SERVICE_BUS_QUEUE")

app = FastAPI()
sb_client = ServiceBusClient.from_connection_string(SERVICEBUS_CONNECTION)

@app.get("/")
def home():
    return {"status": "running"}


@app.post("/log")
async def ingest_logs(req: Request, bg: BackgroundTasks):
    """Receive logs, send them to Log Analytics and Service Bus."""
    logs_batch = await req.json()
    if not isinstance(logs_batch, list):
        logs_batch = [logs_batch]

    # --- Send batch to Service Bus ---
    try:
        with sb_client.get_queue_sender(queue_name=SERVICEBUS_QUEUE) as sender:
            messages = [ServiceBusMessage(json.dumps(log)) for log in logs_batch]

            for attempt in range(3):
                try:
                    sender.send_messages(messages)
                    logging.info(f" Sent {len(logs_batch)} logs to Service Bus (Attempt {attempt+1})")
                    break
                except Exception as e:
                    logging.warning(f" Retry {attempt+1}/3 failed to send to Service Bus: {e}")
                    time.sleep(1.5)
    except Exception as e:
        logging.error(f" Service Bus send failed: {e}")

    # --- Send to Log Analytics asynchronously ---
    for log in logs_batch:
        bg.add_task(send_log, log)

    return {"status": f"{len(logs_batch)} logs queued"}
