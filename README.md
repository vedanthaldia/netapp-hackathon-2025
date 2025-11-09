# Intelli-Tier Fabric: AI-Powered Data Management

This is a submission for the AI Club x NetApp Hackathon (November 2025).

## Problem Statement

We built an intelligent data management solution that dynamically analyzes and moves data across a simulated hybrid-cloud environment (On-Prem, Local Hot, and Google Cloud Storage) based on real-time access patterns.

## Core Features

* **Real-Time Streaming:** Uses Kafka to simulate live user access logs.
* **AI-Powered Brain:** A Python agent uses the Gemini LLM to analyze data policies (access frequency, latency) and make intelligent decisions.
* **Real Cloud Integration:** Automatically moves "cold" data to a **real Google Cloud Storage bucket** and downloads it when it becomes "hot" again.
* **Live Dashboard:** A Streamlit dashboard visualizes the entire system, showing live logs from Kafka and the AI agent, plus metrics and file locations.
* **Containerized Backend:** The entire data pipeline (Kafka, Consumer, Producer, UI) is containerized with Docker Compose for scalability.

## How to Run

This project uses a 2-terminal launch system.

**Prerequisites:**
* Docker & Docker Compose
* Python 3.11
* Google Cloud Account with a GCS bucket
* `gcs-credentials.json` file (see below)

**1. Create Credentials:**
* Create a Google Cloud Storage bucket.
* Create a Service Account with "Storage Admin" permissions.
* Download its JSON key and rename it to `gcs-credentials.json` in this folder.

**2. Update `tiering_agent.py`:**
* Update the `COLD_BUCKET_NAME` variable to match your GCS bucket name.

**3. Run the Backend (Terminal 1):**
* This command starts Kafka, the UI, the producer, and the consumer.
* `docker compose up --build`
* Open the dashboard at **http://localhost:8501**

**4. Run the AI "Brain" (Terminal 2):**
* This script must run on the host machine to access the Gemini CLI.
* `pip install -r requirements.txt`
* `source venv_stable/bin/activate`
* `python tiering_agent.py`

You will now see all components working and interacting on the dashboard.