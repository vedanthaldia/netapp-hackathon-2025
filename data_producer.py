import time
import json
import random
import sys
import logging
from kafka import KafkaProducer

# --- CONFIGURATION ---
KAFKA_BROKER = 'kafka:9093' # <-- 1. THIS IS THE FIX
TOPIC_NAME = 'access_logs'

# This is the list of valid files from your database
FILE_LIST = [
    'project_alpha_logs.csv',
    'hr_payroll_q4.dat',
    '2021_archive.zip',
    'user_activity_stream.json'
]

def setup_logging():
    """Configures logging for the producer."""
    logger = logging.getLogger('kafka_producer')
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(console_handler)
    return logger

def main():
    logger = setup_logging()
    
    producer = None
    # Try to connect to Kafka, retrying a few times
    for _ in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"Successfully connected to Kafka broker at {KAFKA_BROKER}")
            break
        except Exception as e:
            logger.info(f"Waiting for Kafka broker at {KAFKA_BROKER}... retrying in 5s.")
            time.sleep(5)

    if not producer:
        logger.error(f"Error: Could not connect to Kafka broker at {KAFKA_BROKER}. Please ensure Kafka is running.")
        return

    logger.info(f"Starting to send messages to topic '{TOPIC_NAME}'. Press Ctrl+C to stop.")
    
    try:
        while True:
            # Pick a random file
            file_name = random.choice(FILE_LIST)
            message = {'file_name': file_name}
            
            # Send the message
            producer.send(TOPIC_NAME, value=message)
            logger.info(f"Sending message: {message}")
            
            # Wait 1-3 seconds
            time.sleep(random.uniform(1, 3))
            
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()