from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator 
from datetime import datetime, timezone
import requests
import json
import boto3
from airflow.sdk import get_current_context

# configuration
API_BASE_URL      = "http://localhost:8000"
S3_BUCKET         = ""
AWS_REGION        = "us-east-2"
MEDALLION         = "bronze"

# bikin helper 
def get_s3_client():
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    conn = S3Hook(aws_conn_id="aws_default")
    credentials = conn.get_credentials()
    return boto3.client(
        "s3",
        aws_access_key_id     = credentials.access_key,
        aws_secret_access_key = credentials.secret_key,
        region_name           = AWS_REGION,
    )

def s3_key(process_type: str, execution_date: datetime) :
 
    timestamp = execution_date.strftime('%Y%m%d_%H%M%S')
    process_type_list = ['01', '02']

    label = ""
    if process_type in process_type_list :
        label = "import" if process_type == "01" else "export"

        return (
            f"{MEDALLION}/"
            f"manifest_api/"
            f"{execution_date.strftime('%Y')}/"
            f"{execution_date.strftime('%m')}/"
            f"{execution_date.strftime('%d')}/"
            f"{label}/manifest_{timestamp}.json"
        )

    return (
            f"{MEDALLION}/"
            f"vessel_schedule_api/"
            f"{execution_date.strftime('%Y')}/"
            f"{execution_date.strftime('%m')}/"
            f"{execution_date.strftime('%d')}/"
            f"vessel_schedule_{timestamp}.json"
        )

# bikin fungsi utk hit api 
def fetch_manifest_import_and_upload():
    context = get_current_context()
    execution_date = context["logical_date"]

    # Hit API
    response = requests.get(
        f"{API_BASE_URL}/manifest/import",
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    # Upload ke S3
    s3      = get_s3_client()
    key     = s3_key("01", execution_date)
    payload = json.dumps(data, indent=2).encode("utf-8")

    s3.put_object(
        Bucket      = S3_BUCKET,
        Key         = key,
        Body        = payload,
        ContentType = "application/json",
    )

    # Simpan juga ke local untuk simulator
    with open("/root/portconnect/batch/data/manifest_import.json", "w") as f:
        json.dump(data, f, indent=2)

def fetch_manifest_export_and_upload():
    context = get_current_context()
    execution_date = context["logical_date"]

    # Hit API
    response = requests.get(
        f"{API_BASE_URL}/manifest/export",
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    # Upload ke S3
    s3      = get_s3_client()
    key     = s3_key("02", execution_date)
    payload = json.dumps(data, indent=2).encode("utf-8")

    s3.put_object(
        Bucket      = S3_BUCKET,
        Key         = key,
        Body        = payload,
        ContentType = "application/json",
    )

    # simpan juga ke local untuk simulator
    with open("/root/portconnect/batch/data/manifest_export.json", "w") as f:
        json.dump(data, f, indent=2)

def fetch_schedule_vessel_and_upload():
    context = get_current_context()
    execution_date = context["logical_date"]

    # Hit API
    response = requests.get(
        f"{API_BASE_URL}/vessel/schedule",
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    # Upload ke S3
    s3      = get_s3_client()
    payload = json.dumps(data, indent=2).encode("utf-8")
    key = s3_key(process_type="vessel",execution_date=execution_date)

    s3.put_object(
        Bucket      = S3_BUCKET,
        Key         = key,
        Body        = payload,
        ContentType = "application/json",
    )
   
    # Save to local
    with open("/root/portconnect/batch/data/vessel_schedule.json", "w") as f:
        json.dump(data, f, indent=2)


with DAG(
    dag_id          = "fetch_manifest_bc_and_vessel_schedule",
    description     = "Fetch manifest BC dan Vessel schedule dari API dan simpan ke S3",
    start_date      = datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule        = "@daily",
    catchup         = False,
    tags            = ["manifest", "bc", "s3"],
) as dag:

    task_import = PythonOperator(
        task_id         = "fetch_manifest_import",
        python_callable = fetch_manifest_import_and_upload,
    )

    task_export = PythonOperator(
        task_id         = "fetch_manifest_export",
        python_callable = fetch_manifest_export_and_upload,
    )

    task_vessel_schedule = PythonOperator(
        task_id         = "fetch_vessel_schedule",
        python_callable = fetch_schedule_vessel_and_upload,
    )

    # Import selesai dulu, baru export
    task_import >> task_export >> task_vessel_schedule