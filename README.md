# Alberta Power Grid (AESO) Market Analytics & Battery Arbitrage Pipeline

An automated, end to end date pipeline that ingests hourly power prices and grid demand from Alberta Electric System Operator (AESO), processes the metrics with PySpark, loads the clean data into a PostgreSQL database, and simulates the profits from a 10 MWh industrial battery using SQL views.

## 1. The Business Case & Analytical Outcome
Alberta's power market prices fluctuate wildly—sometimes hitting the maximum cap of $1,000/MWh during peak demand. This project models how much money a **10 MW battery storage system** could make by buying power when it’s cheap and selling it back to the grid when prices spike.

### Key Insights
* **10-Day Simulated Battery Profit:** $10,641.67 (factoring in 90% round-trip efficiency losses).
* **Demand vs. Price Correlation:** **0.125** - The weak correlation proves that the high demand doesn't trigger price spikes in AB. Supply drop (wind & gen) outages, cause high volitility. This makes automated price-responsive battery arbitrage essential.
---

## 2. System Architecture
```bash
[AESO API] ---> [ingest.py] ---> [data/raw/ (JSON)]
|
[Postgres DB] <--- [load.py] <--- [transform.py (PySpark)] <--- [data/clean/ (Parquet)]
|
[v_daily_battery_arbitrage (SQL View)]
```
### Data Workflow:
1.  **Ingest:** A modular Python script retrieves historical or hourly prices (Pool Prices) and Alberta Internal load (AIL) from the AESO API.
2.  **Transform:** PySpark flattens the nested raw JSON, standardizes types and timestamps, cleans null records, and outputs a Parquet file.
3.  **Load:** A transactional load script handles idempotent database writes (upserting) to a local Dockerized PostgreSQL instance.
4.  **Analyze:** A dynamic PostgreSQL View computes daily battery charging costs, discharge revenues, and net profits on the fly.

---

## 3. Database Schema Design
The data is modeled into an optimized two-layer analytical schema:

*   **`raw_hourly_grid` (Transactional/Staging Layer):** Stores hour-by-hour pool prices and actual system load.
*   **`daily_grid_agg` (Mart/Aggregate Layer):** Consolidates daily high, low, average, and standard deviation metrics, along with a reporting completeness audit (`hours_reported`).
*   **`v_daily_battery_arbitrage` (Analytical View):** Performs the math for a 10 MWh battery operating under a **90% Round-Trip Efficiency (RTE)** constraint.

---

## 4. Key Engineering Decisions & Talking Points

*   **Decoupled ELT Architecture:** By writing raw JSON payloads directly to disk before executing Spark transformations, the pipeline protects the external AESO API from unnecessary repeated hits during local script debugging.
*   **Resource-Optimized Orchestration (Airflow Standalone):** Rather than running a resource-heavy 6-container Docker-Airflow stack, this project utilizes Apache Airflow Standalone directly in the local virtual environment. This significantly reduces CPU and RAM overhead on WSL while preserving identical DAG and task monitoring APIs.
*   **Idempotency & Atomic Transactions:** The database loader is fully idempotent; re-running identical dates updates current records rather than inserting duplicate keys. Furthermore, raw inserts and daily aggregates are handled in a single SQL transaction—if either step fails, the database rolls back to prevent half-loaded states.
*   **Realistic Physical Modeling:** Instead of modeling a naive 100% efficient battery, the arbitrage simulation enforces a 10% thermal and inverter loss on the discharge side (charge 10 MWh, but only discharge 9 MWh). On stable days with small price spreads, this correctly models negative daily profits, proving physical asset reality.

---

## 5. How To Run Locally

### Prerequisites
*   WSL2 (Ubuntu)
*   Docker Desktop
*   Python 3.10+
*   Java Runtime Environment (for PySpark execution)

### 1. Start PostgreSQL in background
```bash
docker compose up -d 
```
### 2. Activate virtual environment and install dependencies
```bash
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure environment variables (Paste your AESO API key here)
```bash
cp .env.example .env
```

### 4. Initialize PostgreSQL schema and views
```bash
docker exec -i aeso_postgres psql -U aeso_user -d aeso_market_db < sql/schema.sql
```

### 5. Export Airflow home directory and launch server
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow standalone
#Note: Log in to http://localhost:8080 using the temporary credentials printed in your terminal or (simple_auth_manager_passwords.json.generated in the airflow folder), activate the aeso_grid_pipeline DAG, and trigger it manually.
```



## 6. Project Structure
```bash
├── airflow/
│   └── dags/
│       └── aeso_pipeline_dag.py # Airflow DAG orchestrating tasks
├── data/
│   ├── raw/                     # Raw JSON payloads from API (Git-ignored)
│   └── clean/                   # PySpark clean output (Git-ignored)
├── sql/
│   ├── schema.sql               # DB Table and View DDLs
│   └── analysis.sql             # SQL templates for DBeaver reporting
├── src/
│   ├── ingest.py                # Python API extract task
│   ├── transform.py             # PySpark timezone/type cleaning
│   └── load.py                  # Idempotent Postgres database writer
├── docker-compose.yml           # Runs local PostgreSQL container
├── requirements.txt             # Python dependencies
└── .gitignore                   # Preserves folders while ignoring temporary data
```