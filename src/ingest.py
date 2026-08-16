import os
import json
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Configuration
API_KEY = os.getenv("AESO_API_KEY")
POOL_PRICE_URL = "https://apimgw.aeso.ca/public/poolprice-api/v1.1/price/poolPrice"
AIL_URL = "https://apimgw.aeso.ca/public/actualforecast-api/v1/load/albertaInternalLoad"
GEN_CAP_URL = "https://apimgw.aeso.ca/public/aiesgencapacity-api/v1/AIESGenCapacity"
RAW_DATA_DIR = "data/raw"

def ingest_pool_prices(start_date: str, end_date: str):
    """
    Fetches wholesale hourly pool prices from the AESO API Gateway 
    for a given date range and saves the raw JSON to disk.
    """
    if not API_KEY:
        raise ValueError("AESO_API_KEY environment variable is not set. Check your .env file.")

    # Headers required by Azure API Management
    headers = {
        "API-KEY": API_KEY,
        "Accept": "application/json"
    }

    # Query parameters
    params = {
        "startDate": start_date,
        "endDate": end_date
    }

    print(f"Requesting pool prices from {start_date} to {end_date}...")
    
    try:
        response = requests.get(POOL_PRICE_URL, headers=headers, params=params, timeout=15)
        # Raise an exception for HTTP errors 
        response.raise_for_status() 
        
        # Parse output to ensure it is valid JSON
        data = response.json()

        # Create output directory if it doesn't exist
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        
        # Save raw JSON
        output_file = f"{RAW_DATA_DIR}/raw_pool_prices_{start_date}_to_{end_date}.json"
        with open(output_file, "w") as f:
            json.dump(data, f, indent=4)
            
        print(f"Successfully downloaded raw prices. Saved to: {output_file}")
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during ingestion: {e}")

def ingest_ail(start_date: str, end_date: str):
    """
    Ingest the supply and demand 
    """

    #check if key is valid
    if not API_KEY:
        raise ValueError("AESO_API_KEY environment variable is not set. Check your .env file.")

    #how we pass the key
    headers = {
        "API-KEY": API_KEY,
        "Accept": "application/json"
    }

    #Api needs startDate and endDate
    params = {
    "startDate": start_date,
    "endDate": end_date
    }

    print(f"Requesting pool prices from {start_date} to {end_date}...")

    try:
        response = requests.get(AIL_URL, headers=headers, params=params, timeout=15)
        response.raise_for_status #gives us the status of the response

        #parse the data to python dict
        data = response.json()

        # Create output directory if it doesn't exist
        os.makedirs(RAW_DATA_DIR, exist_ok=True)

        output_file = f"{RAW_DATA_DIR}/raw_ail_{start_date}_to_{end_date}.json" #create the file name
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=4)

        print(f"Successfully downloaded raw prices. Saved to: {output_file}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during ingestion: {e}")

def ingest_gen_capacity(start_date: str, end_date: str):
    """
    We want to ingest the generation and the capacity for the alberta power grid
    """

    if not API_KEY:
        raise ValueError("API-KEY invalid. Check your .env file.")

    headers = {
        "API-KEY": API_KEY,
        "Accept": "application/json"
    }

    params = {
        "startDate": start_date, 
        "endDate": end_date
    }

    print(f"Requesting generation capacity from {start_date} to {end_date}")

    try:
        response = requests.get(GEN_CAP_URL, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
    
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        output_file = f"{RAW_DATA_DIR}/raw_gen_cap_{start_date}_to_{end_date}.json"
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=4)

        print(f"Successfully downloaded raw prices. Saved to: {output_file}")

    except requests.exceptions.RequestException:
        print("An error occured during ingestion")

if __name__ == "__main__":
    # Test pull for a 10-day block in August 2026

    ingest_pool_prices(start_date="2026-08-01", end_date="2026-08-10")
    ingest_ail(start_date="2026-08-01", end_date="2026-08-10")
    ingest_gen_capacity(start_date="2026-08-01", end_date="2026-08-10")
    
