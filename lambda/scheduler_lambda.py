import pymysql
import boto3
import csv
import os
from datetime import datetime
import io

# ---------- DATABASE CONFIG ----------
DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = "carbon_scheduler"

s3 = boto3.client('s3')

# ---------- LOAD CARBON DATA ----------
def load_carbon_data(bucket):

    # Demo carbon intensity data
    return {
        "India": [
            {"month": 1, "day": 5, "hour": 9, "carbon": 450.25},
            {"month": 1, "day": 5, "hour": 10, "carbon": 430.10},
            {"month": 1, "day": 5, "hour": 11, "carbon": 420.00},
        ],
        "Germany": [
            {"month": 1, "day": 5, "hour": 9, "carbon": 180.50},
            {"month": 1, "day": 5, "hour": 10, "carbon": 150.20},
            {"month": 1, "day": 5, "hour": 11, "carbon": 137.95},
        ]
    }

# ---------- OPTIMIZATION ----------
def optimize_job(job, carbon_data):

    submission = datetime.strptime(job['submission_time'], "%Y-%m-%d %H:%M:%S")
    deadline = datetime.strptime(job['deadline_time'], "%Y-%m-%d %H:%M:%S")

    baseline_country = "India"
    baseline_carbon = None

    optimized_country = None
    optimized_carbon = float("inf")
    optimized_hour = None

    for country, records in carbon_data.items():

        for entry in records:

            if entry["month"] == submission.month and entry["day"] == submission.day:

                if entry["hour"] <= deadline.hour:

                    if country == baseline_country and baseline_carbon is None:
                        baseline_carbon = entry["carbon"]

                    if entry["carbon"] < optimized_carbon:
                        optimized_carbon = entry["carbon"]
                        optimized_country = country
                        optimized_hour = entry["hour"]

    scheduled_time = submission.replace(hour=optimized_hour)

    return {
        "baseline_country": baseline_country,
        "baseline_carbon": baseline_carbon,
        "optimized_country": optimized_country,
        "optimized_carbon": optimized_carbon,
        "carbon_saved": baseline_carbon - optimized_carbon,
        "scheduled_time": scheduled_time
    }

# ---------- LAMBDA HANDLER ----------
def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Only process job uploads
    if not key.startswith("jobs/"):
        return {"status": "ignored"}

    carbon_data = load_carbon_data(bucket)

    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')

    reader = csv.DictReader(io.StringIO(content))

    results = []

    for job in reader:

        result = optimize_job(job, carbon_data)

        results.append({
            "job_id": job['job_id'],
            "baseline_country": result['baseline_country'],
            "optimized_country": result['optimized_country'],
            "carbon_saved": result['carbon_saved']
        })

    return {
        "status": "completed",
        "jobs_processed": len(results),
        "results": results
    }