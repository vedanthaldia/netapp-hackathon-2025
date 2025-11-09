import sqlite3
import os
import datetime
from google.cloud import storage
import shutil # <-- THIS IS THE FIX

# --- ❗ CONFIGURATION ---
DB_NAME = 'metadata.db'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcs-credentials.json"
COLD_BUCKET_NAME = "netapp-cold-storage-vedant-2025" # ❗ Make sure this is your GCS bucket
# --- ❗ END CONFIGURATION ---

# Initialize GCS Client
try:
    gcs_client = storage.Client()
    cold_bucket = gcs_client.bucket(COLD_BUCKET_NAME)
    # Test connection
    cold_bucket.reload()
    print(f"Successfully connected to GCS Bucket: {COLD_BUCKET_NAME}")
except Exception as e:
    print(f"FATAL ERROR: Could not connect to GCS bucket '{COLD_BUCKET_NAME}'.")
    print("Please check your bucket name and 'gcs-credentials.json' file.")
    print(f"Error: {e}")
    exit()

# Define the local storage paths
STORAGE_PATHS = {
    'on_prem': 'mock_on_prem',
    'hot': 'mock_aws_s3_hot',
}

# --- 1. Create Local Directories ---
for path in STORAGE_PATHS.values():
    os.makedirs(path, exist_ok=True)
print("Created mock storage directories.")

# --- 2. Create DB and Table ---
conn = sqlite3.connect(DB_NAME)
c = conn.cursor()
c.execute("DROP TABLE IF EXISTS files") # Start fresh
c.execute('''
CREATE TABLE files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name TEXT NOT_NULL UNIQUE,
    current_location TEXT NOT_NULL,
    access_count INTEGER DEFAULT 0,
    last_accessed_ts TIMESTAMP,
    data_sensitivity TEXT DEFAULT 'LOW',
    required_latency_ms INTEGER DEFAULT 500,
    file_size_mb INTEGER DEFAULT 10
)
''')
print("Created 'files' table in metadata.db.")

# --- 3. Define Dummy Data ---
now = datetime.datetime.now()
dummy_files_data = [
    # A "HOT" file (local)
    ('project_alpha_logs.csv', 'hot', 150, now - datetime.timedelta(minutes=5), 'LOW', 10, 50),
    
    # A "WARM" file (local)
    ('hr_payroll_q4.dat', 'on_prem', 5, now - datetime.timedelta(hours=2), 'HIGH', 100, 20),
    
    # A "COLD" file (in GCS)
    ('2021_archive.zip', 'cold', 1, now - datetime.timedelta(days=90), 'LOW', 300, 2000),
    
    # A "MISPLACED" file (in GCS, but should be hot)
    ('user_activity_stream.json', 'cold', 95, now - datetime.timedelta(minutes=30), 'LOW', 20, 15)
]

# --- 4. Insert data and create/upload files ---
c.executemany('''
INSERT INTO files (file_name, current_location, access_count, last_accessed_ts, data_sensitivity, required_latency_ms, file_size_mb)
VALUES (?, ?, ?, ?, ?, ?, ?)
''', dummy_files_data)

print("Populating initial file state...")
for file_data in dummy_files_data:
    file_name = file_data[0]
    location_key = file_data[1]
    
    # Create a dummy file to upload/move
    dummy_content = f"This is a mock file: {file_name}"
    # Create the file in the script's directory before moving/uploading
    temp_file_path = os.path.join(os.getcwd(), file_name)
    with open(temp_file_path, 'w') as f:
        f.write(dummy_content)

    if location_key == 'cold':
        # Upload to GCS
        print(f"Uploading '{file_name}' to GCS (cold)...")
        blob = cold_bucket.blob(file_name)
        blob.upload_from_filename(temp_file_path)
        os.remove(temp_file_path) # Remove temp file
        
    else:
        # Move to local folder
        print(f"Moving '{file_name}' to local ({location_key})...")
        dest_path = os.path.join(STORAGE_PATHS[location_key], file_name)
        # Ensure the destination directory exists (it should, but good to be safe)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        shutil.move(temp_file_path, dest_path) # Move temp file

conn.commit()
conn.close()

print(f"Database '{DB_NAME}' created and synced with cloud state.")