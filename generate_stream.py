import requests
import random
import uuid
import time
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

FASTAPI_URL = "http://127.0.0.1:8000/log"  # FastAPI endpoint

# --- Session with retries ---
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.5)
session.mount("http://", HTTPAdapter(max_retries=retries))


def generate_log_entry():
    """Generate one realistic healthcare insurance log entry with Azure-style structure."""
    user = random.choice([
        "patient_ravi", "patient_meena", "doctor_arun",
        "ins_officer_neha", "admin_kiran", "api_gateway"
    ])
    service = random.choice([
        "ClaimProcessingService", "FileUploadService",
        "ECGAnalysisService", "PatientPortalAPI",
        "FraudDetectionService", "NotificationService"
    ])
    file = random.choice([
        "ECG_Report.pdf", "Insurance_Form.docx", "Lab_Results.csv",
        "Claim_Documents.zip", "KYC_Verification.json"
    ])
    req_id = f"req_{uuid.uuid4().hex[:8]}"
    claim_id = f"C{random.randint(10000,99999)}"
    patient_id = f"P{random.randint(1000,9999)}"

    # Select log category (like Azure Monitor severity levels)
    log_category = random.choices(
        ["Information", "Warning", "Error", "Critical", "Security", "Performance"],
        weights=[60, 10, 15, 5, 5, 5],
        k=1
    )[0]

    operation = random.choice([
        "claim_submission", "file_upload", "login",
        "document_validation", "report_analysis",
        "payment_processing", "data_sync", "inference"
    ])

    # Defaults
    status_code = 200
    status_detail = "OK"
    message = ""
    context_detail = {}

    # Map operations to realistic success codes
    success_code_by_op = {
        "file_upload": (201, "Created"),
        "claim_submission": (202, "Accepted"),
        "login": (200, "OK"),
        "document_validation": (200, "Validated"),
        "report_analysis": (200, "Analyzed"),
        "payment_processing": (200, "Payment Processed"),
        "data_sync": (204, "No Content"),
        "inference": (200, "Inference Completed")
    }

    # -------------------------
    # Realistic Log Generation
    # -------------------------
    if log_category == "Information":
        status_code, status_detail = success_code_by_op.get(operation, (200, "OK"))
        message = (
            f"{service}: Successfully completed {operation} for patient {patient_id}, "
            f"claim {claim_id}. File {file} processed without issues."
        )

    elif log_category == "Warning":
        status_code = 206
        status_detail = "Partial"
        latency = random.randint(1200, 3000)
        message = (
            f"{service}: Degraded performance - {operation} experienced high latency "
            f"({latency}ms) for claim {claim_id}. Operation returned partial results."
        )
        context_detail = {"latency_ms": latency, "threshold_ms": 1000}

    elif log_category == "Error":
        err_case = random.choice([
            ("Invalid insurance document format", 422),
            ("Claim API unavailable", 503),
            ("ECG report timeout", 408),
            ("Unauthorized data access", 401),
            ("Duplicate claim submission", 409)
        ])
        status_detail = err_case[0]
        status_code = err_case[1]
        message = (
            f"{service}: Error during {operation} for claim {claim_id}. "
            f"Reason: {status_detail}. RequestId: {req_id}."
        )

    elif log_category == "Critical":
        status_code = 500
        status_detail = "Internal Server Error"
        message = (
            f"{service}: CRITICAL — persistent database write failure while processing "
            f"claim {claim_id}. Risk of data loss. RequestId: {req_id}."
        )
        context_detail = {"escalation": True, "impact": "data_loss"}

    elif log_category == "Security":
        reason, code = random.choice([
            ("Invalid credentials", 401),
            ("Account locked after repeated failures", 403),
            ("Too many requests from device", 429)
        ])
        status_code = code
        status_detail = reason
        device = f"DEV-{random.randint(1000,9999)}"
        attempts = random.randint(3, 12)
        message = (
            f"{service}: Security alert — {reason} (attempts={attempts}) for user {user} "
            f"from device {device}."
        )
        context_detail = {"failed_attempts": attempts, "device_id": device}

    elif log_category == "Performance":
        cpu = random.randint(40, 95)
        latency = random.randint(200, 2500)
        status_code = 200
        status_detail = "Metrics"
        message = (
            f"{service}: Performance metrics captured for {operation} — latency "
            f"{latency}ms, cpu {cpu}%."
        )
        context_detail = {"cpu_percent": cpu, "latency_ms": latency}

    # -------------------------
    # Final structured log (Azure-style)
    # -------------------------
    return {
        "TimeGenerated": datetime.now(timezone.utc).isoformat(),
        "Level": log_category,
        "Message": message,
        "UserId": user,
        "RequestId": req_id,
        "AppName": service,
        "FileName": file,
        "FileSizeBytes": random.randint(80_000, 5_000_000),
        "StatusCode": status_code,
        "StatusDetail": status_detail,
        "ClaimId": claim_id,
        "PatientId": patient_id,
        "Operation": operation,
        "Context": context_detail
    }


def stream_logs(duration_seconds=60):
    """Continuously send log batches to FastAPI for the given duration."""
    print(" Starting log stream...")
    end_time = datetime.now(timezone.utc) + timedelta(seconds=duration_seconds)
    total_sent = 0
    batch_count = 0

    while datetime.now(timezone.utc) < end_time:
        batch = [generate_log_entry() for _ in range(random.randint(3, 6))]
        try:
            response = session.post(FASTAPI_URL, json=batch, timeout=15)
            if response.status_code == 200:
                total_sent += len(batch)
                batch_count += 1
                print(f" Batch {batch_count}: Sent {len(batch)} logs → {response.status_code}")
            else:
                print(f" Failed ({response.status_code}): {response.text}")
        except requests.exceptions.RequestException as e:
            print(f" Network error: {e}")

        time.sleep(1.5)  # Small delay to avoid FastAPI overload

    print(f" Finished streaming. Total logs sent: {total_sent}")


if __name__ == "__main__":
    stream_logs(10)  # Run for 100 seconds
    