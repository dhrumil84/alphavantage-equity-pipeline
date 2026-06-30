import os
import json
import boto3
import certifi
from botocore.config import Config
from botocore.exceptions import ClientError
from typing import List, Dict, Any
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

# Bound the time we'll spend on any one R2 call. Defaults (60s/60s, no max
# retries) let a stalled TCP connection wedge the ingest loop for ~minutes
# per call. Standard-mode retries cover throttling and transient 5xx with
# botocore's own exponential backoff.
_R2_CONFIG = Config(
    connect_timeout=5,
    read_timeout=30,
    retries={"max_attempts": 5, "mode": "standard"},
)

_S3_CLIENT = None

def _get_client():
    """Initialises and caches the boto3 S3 client for Cloudflare R2."""
    global _S3_CLIENT
    if _S3_CLIENT is None:
        account_id = os.environ.get("R2_ACCOUNT_ID")
        if not account_id:
            raise ValueError("R2_ACCOUNT_ID environment variable is missing.")

        access_key_id = os.environ.get("R2_ACCESS_KEY_ID")
        secret_access_key = os.environ.get("R2_SECRET_ACCESS_KEY")

        if not access_key_id or not secret_access_key:
            raise ValueError("R2 access keys are missing from environment variables.")

        endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

        _S3_CLIENT = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name="auto",
            use_ssl=True,
            verify=certifi.where(),
            config=_R2_CONFIG,
        )
    return _S3_CLIENT

def _get_bucket() -> str:
    """Retrieves the R2 bucket name from environment variables."""
    bucket = os.environ.get("R2_BUCKET_NAME")
    if not bucket:
        raise ValueError("R2_BUCKET_NAME environment variable is missing.")
    return bucket

def upload_json(data: dict, key: str) -> None:
    """
    Serialise a dictionary to a JSON string and upload it to R2.
    """
    client = _get_client()
    bucket = _get_bucket()
    
    json_str = json.dumps(data)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json_str,
        ContentType="application/json"
    )

def upload_bytes(data: bytes, key: str) -> None:
    """
    Upload raw bytes to R2.
    """
    client = _get_client()
    bucket = _get_bucket()
    
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data
    )

def download_bytes(key: str) -> bytes:
    """
    Download raw bytes from R2.
    """
    client = _get_client()
    bucket = _get_bucket()
    
    response = client.get_object(Bucket=bucket, Key=key)
    return response['Body'].read()

def download_json(key: str) -> dict:
    """
    Download a JSON object from R2 and deserialise it into a dictionary.
    """
    client = _get_client()
    bucket = _get_bucket()
    
    response = client.get_object(Bucket=bucket, Key=key)
    json_bytes = response['Body'].read()
    return json.loads(json_bytes)

def key_exists(key: str) -> bool:
    """
    Check if an object exists in R2 without downloading its contents.
    Uses a HEAD request instead of retrieving the full payload.
    """
    client = _get_client()
    bucket = _get_bucket()
    
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        # Rethrow if it's some other error (e.g., authentication, permissions)
        raise

def list_keys(prefix: str) -> List[str]:
    """
    List all object keys in R2 under a specific prefix.
    """
    client = _get_client()
    bucket = _get_bucket()
    
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    keys = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                keys.append(obj['Key'])
                
    return keys

if __name__ == '__main__':
    
    test_key = "test/r2_client_test.json"
    test_data = {"message": "Hello from r2_client!", "status": "success"}
    
    try:
        print(f"Uploading test JSON to '{test_key}'...")
        upload_json(test_data, test_key)
        print("Upload complete.")
        
        print("Checking if test key exists...")
        exists = key_exists(test_key)
        print(f"Key exists: {exists}")
        
        print(f"Downloading test JSON from '{test_key}'...")
        downloaded_data = download_json(test_key)
        print(f"Downloaded: {downloaded_data}")
        
        print("Listing keys under 'test/'...")
        keys = list_keys("test/")
        print(f"Keys found: {keys}")
    except ValueError as e:
        print(f"Skipping execution test. Setup incomplete: {e}")
    except Exception as e:
        print(f"An error occurred during testing: {e}")
