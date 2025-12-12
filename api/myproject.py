import requests
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError,OperationalError
from fastapi import FastAPI, Path, HTTPException,BackgroundTasks
from typing import Literal
import logging
import os
from dotenv import load_dotenv
import time

# -----------------------------
# 1) Logging setup
# -----------------------------
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
engine=None
def datafetch():

    # -----------------------------
    # 2) Database setup
    # -----------------------------
    # ---------------------------------------------------
    # Read DB URL from environment variable
    # Docker Compose passes this automatically
    # ---------------------------------------------------
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")

    if not DATABASE_URL:
        raise Exception("DATABASE_URL environment variable not set!")

# ---------------------------------------------------
# Create SQLAlchemy engine (initially unvalidated)
# ---------------------------------------------------
    

    def connect_with_retry(max_retries=10, delay=3):
        global engine
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Attempting DB connection ({attempt}/{max_retries})...")
                engine = create_engine(DATABASE_URL)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    logger.info("Connected to PostgreSQL successfully!")
                return
            except OperationalError as e:
                logger.warning(f"Database not ready: {e}")
                time.sleep(delay)

        raise Exception("Could not connect to Postgres after multiple attempts.")

# Try to connect when API starts
    connect_with_retry()
# -----------------------------
# 3) API setup
# -----------------------------
    API_KEY = '13b25fb7-6bd1-46d3-a5ca-dec02f9db368'
    API_BASE = 'http://airlabs.co/api/v9/'
    HEADERS = {"Accept": "application/json"}
    PARAMS = {'api_key': API_KEY}

    def fetch_api_data(endpoint: str) -> pd.DataFrame:

#Fetch API data and return as a DataFrame.
        try:
            response = requests.get(f"{API_BASE}{endpoint}", params=PARAMS, headers=HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json().get("response", [])
            if not data:
                logging.warning(f"No data returned for endpoint: {endpoint}")
            return pd.DataFrame(data)
        except requests.RequestException as e:
            logging.error(f"API request failed for {endpoint}: {e}")
            return pd.DataFrame()

# -----------------------------
# 4) Fetch API data
# -----------------------------
    df_realTimeFlightData = fetch_api_data('flights')
    df_airlinesInfo = fetch_api_data('airlines')
    df_airportsInfo = fetch_api_data('airports')

# -----------------------------
# 5) Data cleaning functions
# -----------------------------
    def clean_flight_data(df: pd.DataFrame) -> pd.DataFrame:
        required_cols = ['flight_number','flight_icao','flight_iata','dep_icao','dep_iata',
                     'arr_icao','arr_iata','airline_icao','airline_iata','aircraft_icao','lat','lng','status']
        df_clean = df.dropna(subset=required_cols)
        return df_clean

    def clean_airlines(df: pd.DataFrame) -> pd.DataFrame:
        df_clean = df.dropna(subset=['icao_code','name']).drop_duplicates(subset='icao_code')
        return df_clean

    def clean_airports(df: pd.DataFrame) -> pd.DataFrame:
        df_clean = df.dropna(subset=['name','icao_code','lat','lng','country_code']).drop_duplicates(subset='icao_code')
        return df_clean

# Clean silver data
    df_realTimeFlightDataSilverData = clean_flight_data(df_realTimeFlightData)
    df_airlinesInfoSilverData = clean_airlines(df_airlinesInfo)
    df_airportsInfoSilverData = clean_airports(df_airportsInfo)

# -----------------------------
# 6) Save silver data to DB
# -----------------------------
    def save_to_db(df: pd.DataFrame, table_name: str):
        try:
            if engine is None:
                print("Engine is not initialized")
                return
            df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            logging.info(f"Table '{table_name}' saved successfully.")
        except SQLAlchemyError as e:
            logging.error(f"Failed to save table '{table_name}': {e}")

    save_to_db(df_realTimeFlightDataSilverData, "df_realTimeFlightDataSilverData")
    save_to_db(df_airlinesInfoSilverData, "df_airlinesInfoSilverData")
    save_to_db(df_airportsInfoSilverData, "df_airportsInfoSilverData")

# -----------------------------
# 7) Gold data query
# -----------------------------
    gold_query = """
        SELECT 
    realtime.lat AS flight_latitude,
    realtime.lng AS flight_longitude,
    realtime.flight_icao,
    realtime.arr_icao,
    realtime.dep_icao,
    airports_arrival.name AS arrival_airport_name,
    departure_airports.name AS departure_airport_name,
    airlines.name AS airline_name,
    airlines.icao_code as airlines_icao,
    realtime.status AS flight_status
FROM "df_realTimeFlightDataSilverData" AS realtime
LEFT JOIN "df_airportsInfoSilverData" AS airports_arrival
    ON airports_arrival.icao_code = realtime.arr_icao
LEFT JOIN "df_airportsInfoSilverData" AS departure_airports
    ON departure_airports.icao_code = realtime.dep_icao
LEFT JOIN "df_airlinesInfoSilverData" AS airlines
    ON airlines.icao_code = realtime.airline_icao limit 200;
"""
    global df_gold
    try:
        with engine.connect() as conn:
            df_gold = pd.read_sql(text(gold_query), conn)
        save_to_db(df_gold, "realTimeFlightsDataGold")
    except SQLAlchemyError as e:
        logging.error(f"Failed to create gold data: {e}")
        df_gold = pd.DataFrame()

# calling the function datafetch
#datafetch()

# -----------------------------
# 8) FastAPI app
# -----------------------------
app = FastAPI(title="Flight Data API")
@app.on_event("startup")
async def startup_task():
    datafetch()
@app.post("/refresh")
async def refresh(background_tasks: BackgroundTasks):
    """
    Trigger a background data refresh.
    """
    background_tasks.add_task(datafetch)
    return {"status": "Refresh started"}
@app.get("/health")
def health():
    return {"status": "ok"}
# ===============================================================
# ROUTES
# ===============================================================
@app.get("/")
def home():
    return {"message": "Welcome to the Flight Data API!"}
@app.get("/flights")
def get_flights():
    if df_gold.empty:
        raise HTTPException(status_code=404, detail="No flight data available")
    return df_gold.to_dict(orient="records")

@app.get("/flights/{flight_icao}")
def get_flight_by_icao(flight_icao: str):
    record = df_gold[df_gold["flight_icao"] == flight_icao.upper()]
    if record.empty:
        raise HTTPException(status_code=404, detail="Flight not found")
    return record.to_dict(orient="records")

@app.get("/flights/status/{flight_status}")
def get_flight_status(flight_status: Literal["en-route", "landed", "scheduled"] = Path(description="Valid statuses: en-route, landed, scheduled")):
    record = df_gold[df_gold["flight_status"] == flight_status]
    if record.empty:
        raise HTTPException(status_code=404, detail="No flights with this status")
    return record.to_dict(orient="records")

@app.get("/flights/arrival/{arr_icao}")
def get_flight_by_arrival_icao(arr_icao: str):
    record = df_gold[df_gold["arr_icao"] == arr_icao.upper()]
    if record.empty:
        raise HTTPException(status_code=404, detail="No flights with this arrival ICAO")
    return record.to_dict(orient="records")

@app.get("/flights/departure/{dep_icao}")
def get_flight_by_departure_icao(dep_icao: str):
    record = df_gold[df_gold["dep_icao"] == dep_icao.upper()]
    if record.empty:
        raise HTTPException(status_code=404, detail="No flights with this departure ICAO")
    return record.to_dict(orient="records")

@app.get("/flights/airlines/{airline_icao}")
def get_flight_by_airline_icao(airline_icao: str):
    record = df_gold[df_gold["airlines_icao"] == airline_icao.upper()]
    if record.empty:
        raise HTTPException(status_code=404, detail="No flights for this airline ICAO")
    return record.to_dict(orient="records")
