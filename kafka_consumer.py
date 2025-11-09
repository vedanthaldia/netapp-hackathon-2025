import json
import sys
import time
import logging
import sqlite3
from kafka import KafkaConsumer

# --- CONFIGURATION ---
KAFKA_BROKER = 'kafka:9093' # <-- 1. THIS IS THE FIRST FIX
TOPIC_NAME = 'access_logs'
DB_NAME = 'metadata.db'

def setup_logging():
    """Configures logging to write to 'kafka.log' and the console."""
    logger = logging.getLogger('kafka_consumer')
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    # File handler
    file_handler = logging.FileHandler('kafka.log', mode='w', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(console_handler)
    
    return logger

def update_db(file_name, logger):
    """Connects to the DB and increments the access count for a file."""
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        # Update the access count and timestamp
        c.execute("""
            UPDATE files 
            SET access_count = access_count + 1, 
                last_accessed_ts = CURRENT_TIMESTAMP
            WHERE file_name = ?
        """, (file_name,))
        
        # Check if any row was actually updated
        if c.rowcount == 0:
            logger.warning(f"Warning: No record found in the database for file_name '{file_name}'.")
        else:
            logger.info(f"Successfully updated access count for '{file_name}'.")
            
        conn.commit()
    except Exception as e:
        logger.error(f"DB Error: Could not update database: {e}")
    finally:
        if conn:
            conn.close()

def main():
    logger = setup_logging()
    
    consumer = None
    # Try to connect to Kafka, retrying a few times
    for _ in range(5):
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest', # Start from the beginning
                group_id='file-access-log-consumer', # Consumer group ID
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info(f"Successfully connected to Kafka topic '{TOPIC_NAME}' at {KAFKA_BROKER}")
            break
        except Exception as e:
            logger.info(f"Waiting for Kafka broker at {KAFKA_BROKER}... retrying in 5s.")
            time.sleep(5)

    if not consumer:
        # --- 2. THIS IS THE SECOND FIX ---
        # Removed the 'file=sys.stderr' argument
        logger.error(f"Error: Could not connect to Kafka broker at {KAFKA_BROKER}. Please ensure Kafka is running.")
        return

    # Connect to DB once to log success
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("SELECT 1 FROM files LIMIT 1")
        conn.close()
        logger.info(f"Successfully connected to database '{DB_NAME}'")
    except Exception as e:
        logger.error(f"FATAL: Could not connect to database '{DB_NAME}': {e}")
        return

    logger.info("Waiting for messages...")
    try:
        for message in consumer:
            try:
                file_name = message.value['file_name']
                logger.info(f"Processing message: file_name = {file_name}")
                update_db(file_name, logger)
                
                # --- Log flushing fix ---
                for handler in logger.handlers:
                    handler.flush()
                    
            except Exception as e:
                logger.error(f"Error processing message: {e} - Value: {message.value}")

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        if consumer:
            consumer.close()

if __name__ == "__main__":
    main()