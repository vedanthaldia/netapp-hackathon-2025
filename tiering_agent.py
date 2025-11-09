import sqlite3
import json
import time
import os
import shutil
import logging
import sys
import subprocess
from google.cloud import storage # <-- NEW IMPORT

# --- Configuration ---
DB_NAME = 'metadata.db'

# --- NEW: Cloud Storage Config ---
# Tell Python where to find your key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcs-credentials.json"
# --- ❗ MAKE SURE THIS BUCKET NAME IS 100% CORRECT ---
COLD_BUCKET_NAME = "netapp-cold-storage-vedant-2025" 

# --- NEW: Initialize GCS Client ---
gcs_client = storage.Client()
cold_bucket = gcs_client.bucket(COLD_BUCKET_NAME)

# --- NEW: Updated Storage Paths ---
# 'cold' is no longer a local folder
STORAGE_PATHS = {
    'on_prem': 'mock_on_prem',
    'hot': 'mock_aws_s3_hot', # We'll still treat 'hot' as local for now
    # 'cold': 'mock_gcp_cold' # This is now managed by GCS
}

def setup_logging():
    """Configures logging to write to 'agent.log' and the console."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO) 
    
    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler('agent.log', mode='w', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    logger.addHandler(file_handler)


    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(console_handler)

    logging.info("Logging initialized.")

def get_database_state():
    """Fetches all file data and formats it as a JSON string for the LLM."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row 
    c = conn.cursor()
    
    c.execute("SELECT * FROM files")
    files_list = [dict(row) for row in c.fetchall()] 
    
    conn.close()
    
    return json.dumps(files_list, indent=2, default=str) 

def extract_json_from_response(text):
    """
    Finds and parses the first valid JSON array (starting with '[')
    from the LLM's raw string output.
    """
    try:
        start_index = text.find('[')
        end_index = text.rfind(']')
        
        if start_index == -1 or end_index == -1 or end_index < start_index:
            logging.error("AI: ERROR! No valid JSON array found in response.")
            return [] 

        json_str = text[start_index : end_index + 1]
        return json.loads(json_str)
        
    except json.JSONDecodeError as e:
        logging.error(f"AI: ERROR! Failed to decode JSON: {e}")
        logging.error(f"AI: Raw response was: {text}")
        return []
    except Exception as e:
        logging.error(f"AI: ERROR! Unexpected error in extract_json: {e}")
        return []

# --- 🚀 NEW UPGRADED FUNCTION 🚀 ---
def execute_migration_plan(plan):
    """
    Executes the migration tasks given by the LLM.
    Uses GCS for 'cold' storage and local folders for 'hot'/'on_prem'.
    """
    if not plan:
        logging.info("AI: No migrations needed.")
        return

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    for task in plan:
        file_name = task['file_name']
        current_loc_key = task['current_location']
        new_loc_key = task['new_location']
        
        logging.info(f"AI: Executing move for '{file_name}' from {current_loc_key} to {new_loc_key}...")
        
        # Define local paths
        on_prem_path = os.path.join(STORAGE_PATHS['on_prem'], file_name)
        hot_path = os.path.join(STORAGE_PATHS['hot'], file_name)

        try:
            # === NEW GCS LOGIC ===
            
            # Case 1: Moving TO GCS (cold)
            if new_loc_key == 'cold':
                logging.info(f"AI: Uploading '{file_name}' to GCS (Cold Storage)...")
                blob = cold_bucket.blob(file_name)
                
                source_file_path = ""
                if current_loc_key == 'on_prem':
                    source_file_path = on_prem_path
                elif current_loc_key == 'hot':
                    source_file_path = hot_path
                
                blob.upload_from_filename(source_file_path)
                os.remove(source_file_path) # Delete local copy after upload

            # Case 2: Moving FROM GCS (cold)
            elif current_loc_key == 'cold':
                logging.info(f"AI: Downloading '{file_name}' from GCS (Cold Storage)...")
                blob = cold_bucket.blob(file_name)
                
                destination_file_path = ""
                if new_loc_key == 'on_prem':
                    destination_file_path = on_prem_path
                elif new_loc_key == 'hot':
                    destination_file_path = hot_path
                
                blob.download_to_filename(destination_file_path)
                blob.delete() # Delete from cloud after download

            # === ORIGINAL LOCAL LOGIC ===
            
            # Case 3: Local-to-Local move (e.g., on_prem <-> hot)
            else:
                logging.info(f"AI: Executing local move for '{file_name}'...")
                src_path = os.path.join(STORAGE_PATHS[current_loc_key], file_name)
                dst_path = os.path.join(STORAGE_PATHS[new_loc_key], file_name)
                shutil.move(src_path, dst_path)

            # --- This part is the same ---
            c.execute("UPDATE files SET current_location = ? WHERE file_name = ?", 
                      (new_loc_key, file_name))
            conn.commit()
            logging.info(f"AI: Move complete for '{file_name}'.")
            
        except Exception as e:
            logging.error(f"AI: ERROR moving {file_name}: {e}")
            
    conn.close()

def get_llm_decision(data_state_json):
    """
    Sends the current data state to the Gemini CLI and gets a migration plan.
    """
    logging.info("AI: Analyzing data state via Gemini CLI...")

    prompt = f"""
You are a NetApp Intelligent Data Tiering agent. Your job is to optimize storage cost and performance based on defined policies.

Storage Tiers & Policies:
* `hot` (Hot): For data with high access frequency (accessed > 10 times in the last 10 minutes) AND low latency requirements (< 10ms).
* `on_prem` (Warm): For data with moderate access (accessed in the last 24 hours) OR high sensitivity ('HIGH').
* `cold` (Cold): For data accessed > 24 hours ago. Cost is the priority. This is GCS, so moves are slow.

Current Data State:
{data_state_json}

Your Task:
Analyze the `current_data_state` against the policies. Predict future access trends based on recent activity.
Return a JSON array of migration tasks for files that are in the *wrong* location. Only include files that need to be moved.
Do not include any text, preamble, or explanations. Only output the raw JSON array.

Format: `[{{ "file_name": "file123.txt", "current_location": "cold", "new_location": "hot", "reason": "High recent access frequency" }}]`

If no migrations are needed, return `[]`.
"""

    try:
        process = subprocess.run(
            ['gemini', '-'], 
            input=prompt,
            capture_output=True,
            text=True,
            check=True,
            encoding='utf-8'
        )
        
        raw_output = process.stdout
        logging.info("AI: Received raw response from Gemini.")
        
        migration_plan = extract_json_from_response(raw_output)
        
        if migration_plan:
            logging.info(f"AI: Parsed migration plan: {json.dumps(migration_plan, indent=2)}")
        
        return migration_plan

    except subprocess.CalledProcessError as e:
        logging.error(f"AI: ERROR! Gemini CLI command failed: {e}")
        logging.error(f"AI: Stderr: {e.stderr}")
        return []
    except Exception as e:
        logging.error(f"AI: ERROR! An unexpected error occurred: {e}")
        return []

def main_loop():
    """The main agent loop that runs forever."""
    setup_logging() 
    
    while True:
        logging.info("\n--- (AI Agent Loop Start) ---")
        
        current_state_json = get_database_state()
        migration_plan = get_llm_decision(current_state_json)
        execute_migration_plan(migration_plan)
        
        logging.info("--- (AI Agent Loop End) ---")

        # --- THIS IS THE NEW FIX ---
        # Force all log handlers to flush (write to disk)
        # This makes the log file visible to the dashboard container
        for handler in logging.getLogger().handlers:
            handler.flush()
        # --- END OF FIX ---
            
        time.sleep(15) 

if __name__ == "__main__":
    main_loop()