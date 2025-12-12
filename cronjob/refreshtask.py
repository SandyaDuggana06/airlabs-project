import requests
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)

API_URL = "http://api:8000/refresh"

try:
    response = requests.post(API_URL, timeout=10)
    response.raise_for_status()
    logging.info(f"{datetime.now()}: Refresh API called successfully!")
except requests.RequestException as e:
    logging.error(f"{datetime.now()}: Failed to call refresh API: {e}")
