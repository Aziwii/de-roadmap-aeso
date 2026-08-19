import os
import json
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables from .env
load_dotenv()

# Configuration
API_KEY = os.getenv("AESO_API_KEY")
POOL_PRICE_URL = "https://apimgw.aeso.ca/public/poolprice-api/v1.1/price/poolPrice"
AIL_URL = "https://apimgw.aeso.ca/public/actualforecast-api/v1/load/albertaInternalLoad"
GEN_CAP_URL = "https://apimgw.aeso.ca/public/aiesgencapacity-api/v1/AIESGenCapacity"
RAW_DATA_DIR = "data/raw"

def _fetch_and_save(url, target_date, file_prefix):
    """ Helper function to fetch AESO endpoint data for a single date and write to raw JSON"""
    if not API_KEY:
            raise ValueError("AESO_API_KEY environment variable is not set. Check your .env file.")
    
    # Headers required by Azure API Management
    headers = {
        "API-KEY": API_KEY,
        "Accept": "application/json"
    }

    # Query parameters
    params = {
        "startDate": target_date,
        "endDate": target_date
    }

    print(f"[{file_prefix}] Fetching data from {target_date}...")
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        # Raise an exception for HTTP errors 
        response.raise_for_status() 
        
        # Parse output to ensure it is valid JSON
        data = response.json()

        # Create output directory if it doesn't exist
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        
        # Save raw JSON
        output_file = f"{RAW_DATA_DIR}/{file_prefix}_{target_date}.json"

        with open(output_file, "w") as f:
            json.dump(data, f, indent=4)
            
        print(f"Successfully downloaded [{file_prefix}]. Saved to: {output_file}")
        
    except requests.exceptions.RequestException as e:
        print(f"[{file_prefix}] An error occurred during ingestion for {target_date}: {e}")

def ingest_pool_prices(target_date: str) -> None:
    """
    Fetches the hourly pool prices for a single day
    """
    _fetch_and_save(POOL_PRICE_URL, target_date, "raw_pool_prices")
    
    

def ingest_ail(target_date: str) -> None:
    """
    Fetches the Alberta internal load metrics for a single day
    """
    _fetch_and_save(AIL_URL, target_date, "raw_ail")

    

def ingest_gen_capacity(target_date: str) -> None:
    """
    Fetches the generation and capacity metrics for a single day
    """
    _fetch_and_save(GEN_CAP_URL, target_date, "raw_gen_cap")

def run_daily_ingestion(target_date: str | None = None) -> None: # str or none allowed else default to none
    """Runs the daily ingestion for the target_date.
        Defaults to yesterdays date if target_date is not provided.
    """
    if target_date is None:
        yesterday = datetime.now() - timedelta(days=1)
        target_date = yesterday.strftime("%Y-%m-%d")

    print(f"----Starting Daily Ingestion for {target_date}----")

    ingest_pool_prices(target_date)
    ingest_ail(target_date)
    ingest_gen_capacity(target_date)

    print(f"----Finished Daily Ingestion for {target_date}----")

if __name__ == "__main__":
    # daily ingestion
    run_daily_ingestion()

    #we can backfill by doing run_daily_ingestion(target_date="2026-08-15") if needed