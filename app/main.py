from fastapi import FastAPI, Request, BackgroundTasks
from app.la_post import send_log
from dotenv import load_dotenv
import os
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import json
import logging

load_dotenv()
app = FastAPI()

# Load environment variables
SERVICEBUS_CONNECTION = os.getenv("SERVICEBUS_CONNECTION")
SERVICEBUS_QUEUE = os.getenv("SERVICEBUS_QUEUE")

# Initialize Service Bus client
sb_client = ServiceBusClient.from_connection_string(SERVICEBUS_CONNECTION)

@app.get("/")
def home():
    return {"status": "FastAPI is running"}

@app.post("/log")
async def ingest_log(req: Request, bg: BackgroundTasks):
    """Receives a log entry, sends to Log Analytics and Service Bus."""
    log_data = await req.json()

    # Send to Log Analytics asynchronously
    bg.add_task(send_log, log_data)

    # Also send to Azure Service Bus
    try:
        with sb_client:
            sender = sb_client.get_queue_sender(queue_name=SERVICEBUS_QUEUE)
            with sender:
                message = ServiceBusMessage(json.dumps(log_data))
                sender.send_messages(message)
                logging.info("✅ Sent message to Service Bus queue.")
    except Exception as e:
        logging.error(f"❌ Failed to send to Service Bus: {e}")

    return {"status": "queued"}
