"""
Pulls hourly pool price data from the AESO API and loads it into raw_pool_price.

TODO (Week 1):
    - Load AESO_API_KEY and DATABASE_URL from .env
    - Make one successful GET request to the poolPrice endpoint for a single day
    - Print/inspect the raw JSON shape before doing anything else

TODO (Week 2):
    - Loop over a date range, respecting the API's max range per request
    - Add retry/backoff on rate-limit or transient errors
    - Parse response into rows, dedupe, and upsert into raw_pool_price
      (use ON CONFLICT (begin_datetime_mpt) DO UPDATE, not blind INSERT)
    - Tag every run with a source_run_id (e.g. uuid4()) for traceability
"""

# Endpoint reference (confirm current path/params against the AESO dev portal):
# GET https://api.aeso.ca/report/v1.1/price/poolPrice
# Headers: {"X-API-Key": AESO_API_KEY}
# Query params: startDate, endDate (YYYY-MM-DD)

if __name__ == "__main__":
    raise NotImplementedError("Week 1: implement the single-day fetch here.")
