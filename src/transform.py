"""
Reads raw_pool_price for a given date, computes daily stats, and
upserts the result into daily_pool_price_agg.

TODO (Week 3):
    - Query raw_pool_price for the target date
    - Handle/flag nulls per the rule documented in the scoping doc
    - Compute avg/min/max/stddev price, hours_reported, null_hours_dropped
    - Upsert into daily_pool_price_agg (ON CONFLICT (price_date) DO UPDATE)
    - Prove idempotency: run twice for the same date, diff the row, confirm no drift
"""

if __name__ == "__main__":
    raise NotImplementedError("Week 3: implement the daily aggregation here.")
