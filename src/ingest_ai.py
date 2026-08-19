"""
src/ingest_ai.py
Extracts Pool Price and Actual Forecast Load from AESO API Gateway.
"""

import os
import json
import logging
from datetime import datetime, timedelta, timezone
import requests
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Configuration
API_KEY = os.getenv("AESO_API_KEY")
POOL_PRICE_URL = "https://apimgw.aeso.ca/public/poolprice-api/v1.1/price/poolPrice"
AIL_URL = "https://apimgw.aeso.ca/public/actualforecast-api/v1/load/albertaInternalLoad"
RAW_DATA_DIR = "data/raw"


def fetch_data(url: str, params: dict) -> dict:
    """
    Calls the AESO API Gateway with APIM authentication headers.
    """
    if not API_KEY:
        raise ValueError("AESO_API_KEY environment variable is not set. Please run: export AESO_API_KEY='your_key'")

    # AESO APIM headers
    headers = {
        "API-Key": API_KEY,
        "Ocp-Apim-Subscription-Key": API_KEY,
        "X-API-Key": API_KEY,
        "Accept": "application/json"
    }

    logging.info(f"Calling endpoint: {url} with params {params}")
    response = requests.get(url, headers=headers, params=params, timeout=30)
    
    if response.status_code != 200:
        logging.error(f"API Request Failed [{response.status_code}]: {response.text}")
    response.raise_for_status()

    return response.json()


def ingest_data(days_back: int = 15):
    """
    Pulls data for the last `days_back` days to cover the 10-day rolling window.
    """
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    # Use timezone-aware UTC datetime
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days_back)
    
    date_params = {
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d")
    }

    # 1. Fetch Pool Price Report
    try:
        pool_price_data = fetch_data(POOL_PRICE_URL, date_params)
        pool_price_file = os.path.join(RAW_DATA_DIR, "pool_price_raw.json")
        with open(pool_price_file, "w") as f:
            json.dump(pool_price_data, f, indent=2)
        logging.info(f"Successfully saved pool price raw data -> {pool_price_file}")
    except Exception as e:
        logging.error(f"Error fetching Pool Price data: {e}")
        raise

    # 2. Fetch Actual Forecast Load (AIL) Report
    try:
        load_data = fetch_data(AIL_URL, date_params)
        load_file = os.path.join(RAW_DATA_DIR, "actual_forecast_load_raw.json")
        with open(load_file, "w") as f:
            json.dump(load_data, f, indent=2)
        logging.info(f"Successfully saved load raw data -> {load_file}")
    except Exception as e:
        logging.error(f"Error fetching Load data: {e}")
        raise


if __name__ == "__main__":
    ingest_data(days_back=15)