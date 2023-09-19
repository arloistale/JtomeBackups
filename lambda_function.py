import psycopg2
import csv
import boto3
from datetime import datetime
import os
import io

def handler(event, context):
    # Database connection parameters
    SUPABASE_URL = os.environ['SUPABASE_URL']
    PASSWORD = os.environ['SUPABASE_PASSWORD']
    PORT = "5432"
    DB = "postgres"
    USER = "postgres"
    
    # Connect to the database
    conn = psycopg2.connect(host=SUPABASE_URL, port=PORT, dbname=DB, user=USER, password=PASSWORD)
    cursor = conn.cursor()

    # Fetch data from the table
    cursor.execute(f"SELECT * FROM aphorisms;")
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    # Current date for naming the CSV
    current_date = datetime.now().strftime('%Y%m%d')
    csv_filename = f"output_{current_date}.csv"

    # Use StringIO to write the CSV data
    csv_output = io.StringIO()
    csvwriter = csv.writer(csv_output, quoting=csv.QUOTE_NONNUMERIC)
    csvwriter.writerow(column_names)  # Write header
    csvwriter.writerows(rows)

    # Convert StringIO to BytesIO for uploading to S3
    csv_output.seek(0)
    output = io.BytesIO(csv_output.getvalue().encode('utf-8'))

    # Stream the in-memory file to S3
    s3 = boto3.client('s3')
    s3.upload_fileobj(output, 'tome-backups', csv_filename)

    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': f"Data streamed to S3 as {csv_filename}"
    }
