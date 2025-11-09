import time
import streamlit as st
import pandas as pd
import sqlite3
import os
from collections import deque

DB_NAME = 'metadata.db'

# NEW FIXED CODE
def load_data():
    """Connects to the DB and returns all file data as a Pandas DataFrame."""
    conn = sqlite3.connect(DB_NAME)
    try:
        df = pd.read_sql_query("SELECT * FROM files", conn, 
                               parse_dates=['last_accessed_ts']) # <-- FIXED
    except Exception as e:
# ...
        st.error(f"Failed to load database: {e}")
        return pd.DataFrame() # Return empty dataframe on error
    finally:
        conn.close()
    return df

def display_agent_log():
    """Reads and displays the last 15 lines of the agent.log file."""
    log_content = ""
    try:
        with open('agent.log', 'r', encoding='utf-8') as f:
            last_lines = deque(f, 15) 
            log_content = "".join(last_lines)
        
        if not log_content:
            log_content = "Log file is empty. Waiting for agent to start..."
            
    except FileNotFoundError:
        log_content = "Waiting for 'agent.log' file to be created by the agent..."
    except Exception as e:
        log_content = f"An error occurred while reading the log file: {e}"

    st.code(log_content, language='log')

def display_kafka_log():
    """Reads and displays the last 15 lines of the kafka.log file."""
    log_content = ""
    try:
        with open('kafka.log', 'r', encoding='utf-8') as f:
            last_lines = deque(f, 15) 
            log_content = "".join(last_lines)

        if not log_content:
            log_content = "Log file is empty. Waiting for consumer to start..."

    except FileNotFoundError:
        log_content = "Waiting for 'kafka.log' file to be created by the consumer..."
    except Exception as e:
        log_content = f"An error occurred while reading the log file: {e}"

    st.code(log_content, language='log')

# --- Page Configuration (Do this first) ---
st.set_page_config(
    page_title="Intelli-Tier Fabric Dashboard",
    page_icon="☁️",
    layout="wide"
)

# --- Main Page UI ---
st.title("Intelli-Tier Fabric Dashboard 🚀")
st.text("Live monitoring of the hybrid-cloud data fabric.")

# --- Load Data ---
data = load_data()

if not data.empty:
    # --- Main Metrics ---
    st.header("Current Data Distribution")
    col1, col2, col3 = st.columns(3)

    # Pie Chart
    pie_data = data['current_location'].value_counts()
    col1.subheader("Data by Location")
    col1.bar_chart(pie_data)

    # File Count
    total_files = len(data)
    col2.subheader("Total Files")
    col2.metric(label="Files Monitored", value=total_files)

    # Total Storage (Mock)
    total_size = data['file_size_mb'].sum()
    col3.subheader("Total Storage")
    col3.metric(label="Total Data (MB)", value=f"{total_size} MB")

    st.markdown("---") # Horizontal line

    # --- Full Data Table ---
    st.header("All Monitored Files")
    st.dataframe(data, use_container_width=True)

else:
    st.warning("Database is empty or could not be read. Run `database_setup.py`.")


st.markdown("---")

# --- Placeholders for Live Logs ---
st.header("Live System Logs")
col_kafka, col_agent = st.columns(2)

with col_kafka:
    st.subheader("Real-Time Access Log (from Kafka)")
    display_kafka_log() # Placeholder for Kafka logs

with col_agent:
    st.subheader("AI Tiering Agent Log")
    display_agent_log() # Call your new function

# --- Auto-refresh ---
# Use the new st.rerun() method
if st.button("Refresh Now"):
    st.rerun()

st.caption("Page automatically refreshes every 5 seconds.")
time.sleep(5)
st.rerun()