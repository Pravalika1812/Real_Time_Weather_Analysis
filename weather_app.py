import streamlit as st
import pandas as pd
from snowflake.connector import connect

# Load data from Snowflake
@st.cache_data(ttl=600)
def load_data():
    conn = connect(
        user="SNOWPRAVI",
        password="Pravalika@1812",
        account="qqwbyuc-qcb49551",
        warehouse="WEATHER_WH",
        database="WEATHER_DB",
        schema="WEATHER_SCHEMA"
    )
    query = """
    SELECT city, temperature, humidity, condition, processed_at
    FROM WEATHER_DATA
   WHERE TO_DATE(PROCESSED_AT) = CURRENT_DATE()
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.set_page_config(page_title="Real-Time Weather Dashboard", layout="wide")
st.title("🌦️ Real-Time Weather Dashboard")
st.markdown("See latest weather updates from across US cities. Updates every 30 seconds.")

if st.button("🔄 Refresh Now"):
    st.experimental_rerun()

df = load_data()

if df.empty:
    st.warning("No data found.")
else:
    grouped = df.groupby("CITY").agg(
        max_temp=("TEMPERATURE", "max"),
        min_temp=("TEMPERATURE", "min"),
        avg_temp=("TEMPERATURE", "mean"),
        max_hum=("HUMIDITY", "max"),
        min_hum=("HUMIDITY", "min"),
        avg_hum=("HUMIDITY", "mean"),
        last_condition=("CONDITION", "last")
    ).reset_index()

    for _, row in grouped.iterrows():
        st.markdown(f"""
        ### 📍 {row['CITY']}
        <div style='display: flex; justify-content: space-between; font-size: 18px;'>
            <div>🌡️ Max Temp: <strong>{row['max_temp']:.2f}°C</strong></div>
            <div>🌡️ Min Temp: <strong>{row['min_temp']:.2f}°C</strong></div>
            <div>🌡️ Avg Temp: <strong>{row['avg_temp']:.2f}°C</strong></div>
        </div>
        <div style='display: flex; justify-content: space-between; font-size: 18px;'>
            <div>💧 Max Humidity: <strong>{row['max_hum']}%</strong></div>
            <div>💧 Min Humidity: <strong>{row['min_hum']}%</strong></div>
            <div>💧 Avg Humidity: <strong>{row['avg_hum']:.2f}%</strong></div>
        </div>
        <div style='margin-top: 5px; font-size: 18px;'>⛅ Condition: <strong>{row['last_condition']}</strong></div>
        <hr style='border: 1px solid #eee;' />
        """, unsafe_allow_html=True)
