import streamlit as st
import pandas as pd
import pydeck as pdk
import requests
import subprocess
import time
from sqlalchemy import create_engine,inspect
from sqlalchemy.exc import SQLAlchemyError
import logging
import plotly.express as px
# -----------------------------
# Streamlit page setup
# -----------------------------
st.set_page_config(page_title="✈️ Flight Tracker Map", layout="wide")
st.title("✈️ Flight Tracker Map")
st.subheader("API docs available at: http://127.0.0.1:8000/docs")
st.text("For refreshed data, please click the button below.")

# -----------------------------
# Start API server
# -----------------------------
if "api_started" not in st.session_state:
    st.session_state.api_started = False

if not st.session_state.api_started:
    if st.button("Start API Server"):
        try:
            st.write("Starting API...")
            subprocess.Popen(["/home/ubuntu/.venv/bin/uvicorn", "myproject:app", "--reload"])
            # Give API a few seconds to start
            time.sleep(3)
            st.session_state.api_started = True
            st.success("API started on http://localhost:8000")
        except Exception as e:
            st.error(f"Failed to start API: {e}")
else:
    st.info("API is already running 🚀")

# -----------------------------
# Fetch flight data with error handling
# -----------------------------
API_URL = "http://127.0.0.1:8000/flights"

try:
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()
    flight_data = pd.DataFrame(response.json())

    if flight_data.empty:
        st.warning("No flight data available at the moment.")
        st.stop()

except requests.exceptions.RequestException as e:
    st.error(f"Error fetching flight data: {e}")
    st.stop()

# -----------------------------
# Normalize flight status
# -----------------------------
flight_data["flight_status_norm"] = flight_data["flight_status"].str.strip().str.lower()

# Map flight_status to icon URLs
icon_map = {
    "landed": "https://img.icons8.com/color/48/000000/airplane-front-view.png",
    "en-route": "https://img.icons8.com/fluency/48/airplane-take-off.png",
    "departed": "https://img.icons8.com/color/48/000000/airplane-landing.png"
}

# Assign icon data for PyDeck
def create_icon_data(status):
    url = icon_map.get(status, icon_map["en-route"])  # default to en-route icon
    return {"url": url, "width": 48, "height": 48, "anchorY": 48}

flight_data["icon_data"] = flight_data["flight_status_norm"].apply(create_icon_data)

# -----------------------------
# Create PyDeck icon layer
# -----------------------------
icon_layer = pdk.Layer(
    type="IconLayer",
    data=flight_data,
    get_icon="icon_data",
    get_position=["flight_longitude", "flight_latitude"],
    get_size=4,
    size_scale=15,
    pickable=True
)

# -----------------------------
# Map view setup
# -----------------------------
view_state = pdk.ViewState(
    latitude=20,
    longitude=0,
    zoom=1.5,
    #pitch=45,
    bearing=30
)

# -----------------------------
# Tooltip template
# -----------------------------
tooltip = {
    "html": "<b>Flight:</b> {flight_icao}<br>"
            "<b>Status:</b> {flight_status}<br>"
            "<b>Departure:</b> {departure_airport_name}<br>"
            "<b>Arrival:</b> {arrival_airport_name}",
    "style": {"backgroundColor": "white", "color": "black"}
}

# -----------------------------
# Render PyDeck chart
# -----------------------------
try:
    r = pdk.Deck(
        layers=[icon_layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="mapbox://styles/mapbox/satellite-v9"
    )
    st.pydeck_chart(r)
except Exception as e:
    st.error(f"Error displaying map: {e}")



# -----------------------------
# 2) Database setup
# -----------------------------
DB_URL = "postgresql+psycopg2://dataenguser:DataEngineer@localhost:5432/airlabs"
try:
    engine = create_engine(DB_URL)
    inspector = inspect(engine)
    logging.info("Database connection successful.")
except SQLAlchemyError as e:
    logging.error(f"Database connection failed: {e}")
    raise SystemExit(e)


# --- Run the SQL query ---
query = """
SELECT
    arrival_airport_name,
    COUNT(*) AS total_flights
FROM "realTimeFlightsDataGold"
GROUP BY arrival_airport_name
ORDER BY total_flights DESC
LIMIT 10;
"""

df = pd.read_sql(query, engine)

# --- Show data table ---
st.write("Top 10 Arrival Airports by Flight Count", df)

# --- Plot bar chart ---
st.bar_chart(data=df.set_index("arrival_airport_name")["total_flights"])


# --- Run the SQL query ---
query = """SELECT COUNT(*) AS total_flights,departure_airport_name FROM "realTimeFlightsDataGold" GROUP BY departure_airport_name ORDER BY total_flights DESC LIMIT 10;
"""

df = pd.read_sql(query, engine)

# --- Show data table ---
st.write("Top 10 Departure Airports by Flight Count", df)

# --- Plot bar chart ---
st.bar_chart(data=df.set_index("departure_airport_name")["total_flights"])



st.title("Number of Flights per Airline")

# --- SQL query ---
query = """
SELECT 
    COUNT(*) AS total_flights,
    airline_name
FROM "realTimeFlightsDataGold"
GROUP BY airline_name
ORDER BY total_flights DESC
LIMIT 10;
"""

df = pd.read_sql(query, engine)

# --- Show table ---
st.write(df)

# --- Line chart using Plotly ---
fig = px.line(
    df,
    x='airline_name',
    y='total_flights',
    color='airline_name',
    markers=True,
    title='Flights per Airline'
)

st.plotly_chart(fig)

st.title("Flight Status Distribution")

# --- Query flight status counts ---
query = """
SELECT flight_status, COUNT(*) AS total_flights
FROM "realTimeFlightsDataGold"
GROUP BY flight_status
ORDER BY total_flights DESC;
"""

df_status = pd.read_sql(query, engine)

# --- Show table ---
st.write("Flight Status Counts", df_status)

# --- Plot pie chart using Plotly ---
fig = px.pie(
    df_status,
    names='flight_status',
    values='total_flights',
    color='flight_status',
    color_discrete_map={
        'en-route': 'skyblue',
        'departed': 'orange',
        'landed': 'green',
    },
    title='Flight Status Distribution'
)

st.plotly_chart(fig)
