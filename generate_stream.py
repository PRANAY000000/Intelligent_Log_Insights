import requests
import random
import uuid
import time
from datetime import datetime, timedelta

FASTAPI_URL = "http://127.0.0.1:8000/log"  # FastAPI endpoint

USERS = ['alice', 'bob', 'charlie', 'dave', 'eve']
FILES = ['report.pdf', 'data.csv', 'image.png', 'backup.zip', 'notes.txt']
ERRORS = [
    {"code": 500, "message": "Internal Server Error"},
    {"code": 503, "message": "Service Unavailable"},
    {"code": 408, "message": "Request Timeout"},
    {"code": 400, "message": "Bad Request"}
]

def random_error():
    return random.choice(ERRORS)

def generate_log_entry():
    """Generate one fake Azure-style log entry."""
    user = random.choice(USERS)
    file = random.choice(FILES)
    size = random.randint(50_000, 5_000_000)
    req_id = f"req_{uuid.uuid4().hex[:8]}"

    # 80% success, 20% failure
    success = random.random() < 0.8
    if success:
        level = "Information"
        status_code = 200
        message = f"User {user} uploaded {file} ({size} bytes) successfully."
    else:
        err = random_error()
        level = "Error"
        status_code = err["code"]
        message = f"User {user} failed to upload {file}: {err['message']}."

    return {
        "TimeGenerated": datetime.utcnow().isoformat() + "Z",
        "Level": level,
        "Message": message,
        "UserId": user,
        "RequestId": req_id,
        "AppName": "FileUploadService",
        "FileName": file,
        "FileSizeBytes": size,
        "StatusCode": status_code,
        "StatusDetail": "OK" if success else err["message"],
    }

def stream_logs(duration_seconds=60):
    """Continuously send logs to FastAPI for a given duration."""
    print("🚀 Starting log stream...")
    end_time = datetime.utcnow() + timedelta(seconds=duration_seconds)
    total_sent = 0

    while datetime.utcnow() < end_time:
        batch = [generate_log_entry() for _ in range(random.randint(3, 6))]
        try:
            response = requests.post(FASTAPI_URL, json=batch, timeout=5)
            if response.status_code == 200:
                total_sent += len(batch)
                print(f"📤 Sent {len(batch)} logs → {response.status_code}")
            else:
                print(f"⚠️  Failed to send logs ({response.status_code}): {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error: {e}")

        time.sleep(1)

    print(f"✅ Finished streaming. Total logs sent: {total_sent}")

if __name__ == "__main__":
    stream_logs(100)
