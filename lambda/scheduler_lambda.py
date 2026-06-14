import pymysql
import csv
import io
import boto3
from datetime import datetime

# ---------- DB CONFIG ----------
DB_HOST = "carbon-scheduler-db.ci588meqwfkv.us-east-1.rds.amazonaws.com"
DB_USER = "admin"
DB_PASSWORD = "Ssr180605"
DB_NAME = "carbon_scheduler"

s3 = boto3.client('s3')


# ---------- DB CONNECTION ----------
def get_connection():
    print("🔵 Connecting to DB...")
    
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=3306,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor
    )
    
    print("🟢 DB Connected")
    return conn


# ---------- LOAD CARBON DATA ----------
def load_carbon_data(bucket, key):
    print(f"🌍 Loading: {key}")
    
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    
    reader = csv.DictReader(io.StringIO(content))

    headers = reader.fieldnames
    print("📊 Headers:", headers)

    time_col = [h for h in headers if "Datetime" in h][0]
    carbon_col = [h for h in headers if "direct" in h][0]

    data = []

    for row in reader:
        try:
            time_str = row[time_col].split("+")[0].strip()
            t = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

            carbon = float(row[carbon_col])

            data.append({
                "month": t.month,
                "day": t.day,
                "hour": t.hour,
                "carbon": carbon
            })

        except:
            continue

    print(f"✅ Loaded {len(data)} rows")
    return data


# ---------- LAMBDA ----------
def lambda_handler(event, context):

    print("🚀 Lambda started")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # ---------- S3 INPUT ----------
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        print(f"📦 {bucket} | {key}")

        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(content))

        # ---------- LOAD CARBON DATA ----------
        carbon_files = {
            "spain": "spain_2024.csv",
            "netherlands": "netherlands_2024.csv",
            "germany": "germany_2024.csv"
        }

        carbon_data = {}
        for region, file in carbon_files.items():
            carbon_data[region] = load_carbon_data(bucket, file)

        count = 0

        # ---------- PROCESS JOBS ----------
        for job in reader:

            print(f"➡️ Job: {job['job_id']}")

            submission = datetime.strptime(job['submit_time_utc'], "%Y-%m-%d %H:%M:%S")
            deadline = datetime.strptime(job['deadline_utc'], "%Y-%m-%d %H:%M:%S")

            duration = int(float(job['duration_hours']))
            energy = float(job['energy_kwh'])

            # ---------- BASELINE ----------
            # ---------- BASELINE (STANDARD REGION) ----------
            baseline_region = "germany"
            baseline_time = submission
            baseline_carbon = None

            for entry in carbon_data[baseline_region]:
                if (entry["month"] == submission.month and
                    entry["day"] == submission.day and
                    entry["hour"] == submission.hour):

                    baseline_carbon = entry["carbon"]
                    break

            if baseline_carbon is None:
                baseline_carbon = 0

            # ---------- OPTIMIZATION ----------
            best_time = None
            best_region = None
            min_carbon = float("inf")

            for region, data in carbon_data.items():

                data = sorted(data, key=lambda x: (x["month"], x["day"], x["hour"]))

                for i in range(len(data) - duration):

                    window = data[i:i+duration]

                    if all(
                        entry["month"] == submission.month and
                        entry["day"] == submission.day and
                        submission.hour <= entry["hour"] <= deadline.hour
                        for entry in window
                    ):

                        avg_carbon = sum(e["carbon"] for e in window) / len(window)

                        if avg_carbon < min_carbon:
                            min_carbon = avg_carbon
                            best_time = datetime(
                                submission.year,
                                submission.month,
                                submission.day,
                                window[0]["hour"]
                            )
                            best_region = region

            # ---------- FALLBACK ----------
            if best_time is None:
                print("⚠️ Fallback used")
                best_time = submission
                best_region = baseline_region
                min_carbon = baseline_carbon

            optimized_carbon = min_carbon
            carbon_saved = baseline_carbon - optimized_carbon

            # ---------- INSERT ----------
            query = """
            INSERT INTO scheduled_jobs 
            (job_id, submit_time_utc, deadline_utc, duration_hours, energy_kwh,
             assigned_region, scheduled_time, carbon,
             baseline_region, baseline_time, baseline_carbon,
             optimized_carbon, carbon_saved)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """

            cursor.execute(query, (
                job['job_id'],
                submission,
                deadline,
                duration,
                energy,
                best_region,
                best_time,
                optimized_carbon,
                baseline_region,
                baseline_time,
                baseline_carbon,
                optimized_carbon,
                carbon_saved
            ))

            print(f"✅ {job['job_id']} | saved={carbon_saved}")

            count += 1

        print(f"🎯 Total jobs: {count}")

        return {
            "status": "SUCCESS",
            "jobs": count
        }

    except Exception as e:
        print("❌ ERROR:", str(e))
        return {
            "status": "FAILED",
            "error": str(e)
        }
