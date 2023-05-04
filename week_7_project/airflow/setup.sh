#!/bin/bash

echo -e "AIRFLOW_UID=$(id -u)" >> .env
echo -e "PROJECT_ID=${PROJECT_ID}" >> .env
echo -e "GCS_BUCKET_EXPRESS=${GCS_BUCKET_EXPRESS}" >> .env
export JSON_KEY='{"conn_type": "google_cloud_platform","extra": {"key_path": "/.google/credentials/google_credentials.json","scope": "https://www.googleapis.com/auth/cloud-platform","project": "'"$PROJECT_ID"'"}}'

echo -e "AIRFLOW_CONN_GCP_ZOOMCAMP_CONNECTION=${JSON_KEY}" >> .env