import os
from azure.cosmos import CosmosClient
from dotenv import load_dotenv
from pathlib import Path


env_path = Path(r"D:\Revature_assignments\Intelligent_log_insights\.env")
load_dotenv(dotenv_path=env_path, override=True)

if not env_path.exists():
    raise FileNotFoundError(f" .env not found at {env_path}")

# Force reload
load_dotenv(dotenv_path=env_path, override=True)

COSMOS_CONN = os.getenv("COSMOS_CONN_STRING")
DB_NAME = os.getenv("COSMOSDB_DB_NAME")
LOGS_CONTAINER = os.getenv("COSMOSDB_CONTAINER_NAME")
INSIGHTS_CONTAINER = os.getenv("COSMOSDB_INSIGHT_CONTAINER")



if not COSMOS_CONN:
    raise ValueError(" Missing COSMOSDB_CONN_STRING. Check your .env file format or encoding.")

#  Initialize Cosmos DB
client = CosmosClient.from_connection_string(COSMOS_CONN)
db = client.get_database_client(DB_NAME)
logs_container = db.get_container_client(LOGS_CONTAINER)
insights_container = db.get_container_client(INSIGHTS_CONTAINER)


