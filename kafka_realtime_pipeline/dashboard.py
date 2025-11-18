import time
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import streamlit as st

PG_CONN_INFO = {
    "host": "localhost",
    "port": 5432,
    "dbname": "kafka_db",
    "user": "kafka_user",
    "password": "kafka_password",
}

def get_connection():
    return psycopg2.connect(**PG_CONN_INFO)

@st.cache_data(ttl=5)  # cache for 5 seconds
def load_data(minutes_back=30):
    conn = get_connection()
    query = """
        SELECT *
        FROM rides
        WHERE start_ts >= NOW() - INTERVAL '%s minutes'
        ORDER BY start_ts DESC;
    """
    df = pd.read_sql(query, conn, params=[minutes_back])
    conn.close()
    return df

def main():
    st.set_page_config(page_title="Real-Time Ride Streaming", layout="wide")
    st.title("🚕 Real-Time Ride Streaming Dashboard")

    # Auto-refresh every 5 seconds
    st_autorefresh = st.experimental_rerun  # fallback if needed
    st_autorefresh_placeholder = st.empty()
    st_autorefresh_placeholder.write("Auto-refresh every 5 seconds…")
    st_autorefresh_placeholder.empty()

    refresh_interval = 5
    placeholder = st.empty()

    while True:
        df = load_data(30)

        with placeholder.container():
            st.subheader("Last 30 minutes of rides")
            if df.empty:
                st.info("Waiting for data from Kafka → PostgreSQL…")
            else:
                # KPIs
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total rides", f"{len(df):,}")
                col2.metric("Avg fare (USD)", f"{df['fare_usd'].mean():.2f}")
                col3.metric("Avg distance (km)", f"{df['distance_km'].mean():.2f}")
                col4.metric(
                    "Avg surge",
                    f"{df['surge_multiplier'].mean():.2f}"
                )

                st.markdown("---")
                st.subheader("Rides by city")
                city_counts = df.groupby("city")["ride_id"].count().reset_index(name="rides")
                st.bar_chart(city_counts.set_index("city"))

                st.subheader("Fare distribution (USD)")
                st.line_chart(df["fare_usd"])

                st.subheader("Recent rides table")
                st.dataframe(df.head(50))

        time.sleep(refresh_interval)

if __name__ == "__main__":
    main()
