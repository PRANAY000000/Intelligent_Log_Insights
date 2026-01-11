import os
import sys
import logging
import azure.functions as func

ROOT = os.path.dirname(__file__)
sys.path.append(os.path.join(ROOT, "Triggers"))

import queue_trigger
import cosmos_trigger

app = func.FunctionApp()

# ---- Queue Trigger ----
@app.function_name(name="process_log_message")
@app.service_bus_queue_trigger(
    arg_name="azservicebus",
    queue_name="logqueue",
    connection="LogPassing_SERVICEBUS"  # must match Azure App Setting name
)
def process_log_message(azservicebus: func.ServiceBusMessage):
    logging.info("Queue trigger received a message.")
    try:
        queue_trigger.handle_message(azservicebus)
        logging.info(" Queue trigger successfully processed message.")
    except Exception as e:
        logging.exception(f" Queue handler failed: {e}")
        raise e  # ensures retries before DLQ


# ---- Cosmos DB Trigger ----
@app.function_name(name="process_cosmos_changes")
@app.cosmos_db_trigger(
    arg_name="documents",
    connection="COSMOSDB_CONN_STRING",  # must match Azure App Setting name
    database_name="ProcessedLogs",
    container_name="LogsStored",
    create_lease_container_if_not_exists=True,
    lease_container_name="leases"
)
def process_cosmos_changes(documents: func.DocumentList):
    if not documents:
        logging.info("Cosmos trigger fired but no documents to process.")
        return

    logging.info(f"Cosmos trigger: {len(documents)} documents received.")
    try:
        cosmos_trigger.handle_cosmos_changes(documents)
    except Exception as e:
        logging.exception(f"Unhandled error in cosmos handler: {e}")
    logging.info("Cosmos trigger processing finished.")
