import streamlit as st
import pandas as pd
import pydeck as pdk
import requests
import subprocess
import time
import docker
from sqlalchemy import create_engine
# -----------------------------
# Streamlit page setup
# -----------------------------
st.set_page_config(page_title="✈️ Flight Tracker Map", layout="wide")
st.title("✈️ Flight Tracker Map")
st.subheader("API docs available at: http://127.0.0.1:8000/docs")
st.text("For refreshed data, please click the button below.")

# -----------------------------
# API settings
# -----------------------------
API_URL = "http://api:8000/flights"  # Use Docker service name, not localhost
API_CONTAINER = "my_api"             # Container name in docker-compose
DOCKER_SOCKET = "/var/run/docker.sock"  # Mounted docker socket

# -----------------------------
# Function: Restart API container
# -----------------------------
def restart_api():
    client = docker.DockerClient(base_url="unix://var/run/docker.sock")
    container = client.containers.get(API_CONTAINER)
    container.restart()
    st.success("API restarted successfully 🚀")
    time.sleep(3)  # wait for container to be ready

# -----------------------------
# Restart API Button
# -----------------------------
st.subheader("API Controls")
if st.button("🔄 Restart API"):
    try:
        restart_api()
    except Exception as e:
        st.error(f"Failed to restart API: {e}")
# -----------------------------
# Fetch flight data with error handling
# -----------------------------
API_URL = "http://api:8000/flights"

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
