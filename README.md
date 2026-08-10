# AESO Pool Price Pipeline

Alberta electricity pool price ingestion pipeline — see `de-roadmap-scoping-doc.md`
for the full spec (source, target, transform logic, done-criteria).

Built as part of a 4-week DE prep plan. Maps to that plan as:
- **Week 1 (this setup):** env, Docker Postgres, AESO API key, repo scaffolding
- **Week 2:** implement `src/ingest.py` and `src/transform.py` solo, no AI
- **Week 3 Mon:** wrap this into a scheduled job (cron, or Airflow via Docker)
- **Week 3 Wed:** rebuild the same spec with AI assistance in a separate branch, diff

## Setup
```bash
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env              # then fill in AESO_API_KEY

docker compose up -d              # starts local Postgres
docker exec -i de-roadmap-pg psql -U postgres -d aeso < sql/schema.sql
```

## Project layout
```
src/ingest.py       # pulls from AESO API -> raw_pool_price   (Week 2)
src/transform.py    # raw_pool_price -> daily_pool_price_agg  (Week 3)
sql/schema.sql       # table definitions
docker-compose.yml    # local Postgres
```
# de-roadmap-aeso
