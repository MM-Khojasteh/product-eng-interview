# Torob Travel — Flight Search & AI Itinerary Planner

Persian-first travel MVP: live flight price comparison over scraped Iranian
OTAs (FlyToday, Alibaba) plus an AI day-planner. FastAPI + Jinja2 + HTMX web
app, Kafka ingestion pipeline, Redis hot cache, Postgres history.

See the [product screenshots](showcase/) for selected views of the implemented UI.

## Requirements

- Python 3.11+
- Docker — only for the demo and live modes
- Node (`npx`) — only to rebuild the CSS

## Install and test

```bash
pip install -r requirements/dev.txt
python -m pytest            # fully offline, no services needed
```

## Run

The web app always serves `http://127.0.0.1:8000`; what a flight search returns
depends on what else is running. The planner at `/itinerary/kish` works in
every mode.

**1. Mock UI** — nothing else running:

```bash
TOROB_FORCE_MOCK=1 python main.py     # PowerShell: $env:TOROB_FORCE_MOCK=1; python main.py
```

**2. Demo** — the real read path on seeded Redis data, no scraping:

```bash
cp .env.example .env                  # set POSTGRES_PASSWORD and PGADMIN_DEFAULT_PASSWORD
docker compose up -d redis
python scripts/seed_redis_demo.py THR KIH <date> --watch   # Jalali date, digits only, e.g. tomorrow
python main.py                        # in a second terminal
```

**3. Full live** — real suppliers. Fill in every `[REQUIRED]` value in `.env`
(supplier endpoint matrix and passwords), then:

```bash
docker compose up -d                  # Kafka, Redis, Postgres + UIs on :8080 / :5540 / :5050
python scripts/dev.py                 # web + worker + normalizer + liveness, .env loaded
```

A cold search triggers an on-demand scan and fills in after ~15 s. To also
persist price history to Postgres (needs `DATABASE_URL`), run
`python scripts/run_with_env.py pipelines.kafka_to_db`.

Headless alternative — every pipeline service in a container, including the
Postgres sink (no separate `kafka_to_db` run, unlike `scripts/dev.py`):

```bash
# .env with the two passwords required — compose refuses to start without them
docker compose -f docker-compose.yml -f docker-compose.pipeline.yml up -d --build
python main.py  # the web app has no container — run on the host, needs requirements/dev.txt
docker compose -f docker-compose.yml -f docker-compose.pipeline.yml ps  # status; logs -f <service>
docker compose -f docker-compose.yml -f docker-compose.pipeline.yml down  # stop; -v also wipes volumes
```

The first log line shows which mode you got: `bootstrap: mode=LIVE …` or
`bootstrap: mode=MOCK …`.

## Configuration

`.env.example` documents every variable. The Python processes do not read
`.env` themselves: Docker Compose does, and `scripts/dev.py` or
`python scripts/run_with_env.py <module>` load it for host-run processes.

The itinerary planner reads Postgres only when `DATABASE_URL` points at a
server with PostGIS and pgvector (the compose `postgres:17-alpine` image has
neither); otherwise it serves built-in Kish demo data. Without
`OPENAI_API_KEY` it writes a deterministic narrative instead of calling an LLM.

## Scripts

| Command | Purpose |
|---|---|
| `bash scripts/verify_pipeline.sh` | End-to-end pipeline smoke test against the running stack |
| `python scripts/run_with_env.py scripts.pipeline_status` | Consumer lag, DLQ and recent scans (`--json` for CI) |
| `bash scripts/build_css.sh` | Rebuild `static/css/torob.css` after changing Tailwind classes |
| `python scripts/enrich_poi_media.py` | Re-fetch POI photos and summaries from Wikipedia/Commons |

## Layout

```
main.py       entry shim (uvicorn main:app)
src/          web app: routers, Redis read model, itinerary/ planner
pipelines/    ingestion services: python -m pipelines.<raw_worker|normalizer|kafka_to_db|liveness>
templates/    Jinja2 templates
static/       compiled CSS, JS, logos, POI photos, vendored libraries
frontend/     Tailwind build inputs
tests/        offline test suite
scripts/      dev runner, demo seeding, ops and build tools
deploy/       pgAdmin server list mounted by docker compose
```
