import requests, json, hmac, hashlib, base64, os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

WORKSPACE_ID = os.getenv("LA_WORKSPACE_ID")
SHARED_KEY = os.getenv("LA_SHARED_KEY")
LOG_TYPE = "CustomAppLogs"

def build_signature(date, content_length):
    x_headers = 'x-ms-date:' + date
    string_to_hash = f"POST\n{content_length}\napplication/json\n{x_headers}\n/api/logs"
    bytes_to_hash = bytes(string_to_hash, encoding='utf-8')
    decoded_key = base64.b64decode(SHARED_KEY)
    encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, hashlib.sha256).digest()).decode()
    return f"SharedKey {WORKSPACE_ID}:{encoded_hash}"

def send_log(log_data):
    body = json.dumps(log_data)
    rfc1123date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    sig = build_signature(rfc1123date, len(body))
    uri = f"https://{WORKSPACE_ID}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': sig,
        'Log-Type': LOG_TYPE,
        'x-ms-date': rfc1123date
    }
    r = requests.post(uri, data=body, headers=headers)
    print(f"Sent → {r.status_code}")
