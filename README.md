# Customer Churn ELT Pipeline

## Overview

This project implements a scalable, ELT pipeline for customer churn analytics using modern open-source tools. The pipeline ingests CSV data, stages and transforms it in PostgreSQL, orchestrates workflows with Dagster, manages transformations with dbt, and and expose insights via Metabase dashboards. All components are containerized for easy deployment and scalability.

---

## Architecture

- **Dagster**: Orchestration and workflow management.
- **dbt**: SQL-based data transformation and modeling.
- **PostgreSQL**: Staging and analytics data warehouse.
- **Metabase**: Open-source BI/reporting tool.
- **Docker Compose**: Container orchestration for all services.


## ![Project Architecture](architecture.png)

---

## Key Features

- Automated ELT Pipeline (Dagster)
    - Hourly ingestion of a CSV feed (configurable via cron)
    - Staging → transformation → analytics flow
    - Orchestration through Dagster jobs + schedules
    - Sensors for monitoring new file arrivals or scheduled runs
- Data Warehouse Architecture (Postgres)
    - raw schema → ingestion
    - staging schema → cleaning & PII anonymisation
    - analytics schema → marts for BI
- Transformation Layer (dbt)
    - dbt models for:
        - data cleaning
        - anonymisation
        - segmentation tables
        - churn rate summaries
    - dbt tests for data quality
- Reporting (Metabase)
    - Metabase connected directly to the analytics schema
    - Automatically detects new marts (churn by age, contract, revenue buckets, etc.)
- Fully Containerised (Docker Compose)
    - Postgres, Dagster, dbt, and Metabase
    - Environment variables managed via `.env`

---

## Getting Started

### Prerequisites

- Docker & Docker Compose installed

### Quick Start

1. **Clone the repository**
   ```sh
   git clone <repo-url>
   cd <project_dir>
   ```

2. **Configure Environment**
   - Edit `.env` to match environment and secrets.

3. **Build and Start All Services**
   ```sh
   docker-compose up --build
   ```

4. **Access Services**
   - Dagster UI: [http://localhost:3000](http://localhost:3000)
   - Metabase: [http://localhost:3001](http://localhost:3001)
   - PostgreSQL: `localhost:5432` (see `.env` for credentials)

---

## Project Structure

```
├── postgres_init/                   # SQL scripts for DB initialization
├── Dockerfile                       # Python/Dagster/dbt image
├── docker-compose.yml               # Orchestration for all services
├── requirements.txt                 # Python dependencies
├── .env                             # Environment variables
├── README.md                        # Project documentation
├── hg_repo/                         # Dagster repository: assets, jobs, sensors
    ├── config.py                    # Env loader + DB connection helper
    ├── jobs.py                      # Dagster job + schedule
    ├── sensors.py                   # File-based or run-based sensors 
    ├── __init__.py                  # Dagster Definitions
    ├── assets/                          
        ├── raw_assets.py            # Ingestion asset
        ├── dbt_assets.py            # dbt build asset
        ├── monitoring_assets.py     # Freshness / KPI check
└── churn_dbt/
    ├── dbt_project.yml
    ├── profiles/                    # dbt profiles (via env)
    ├── models/
        ├── stg/                     # staging models
        ├── marts/                   # analytics models

```
---

## Pipeline Details

- **Ingestion**: Watches for new CSV files in `data/` and loads them into staging tables.
- **Transformation**: dbt models clean, anonymize, and prepare data for analytics.
- **Orchestration**: Dagster schedules and monitors the pipeline, configurable via `.env`.
- **Reporting**: Metabase connects to the analytics schema for dashboards and ad-hoc queries.

---

## Configuration

All configuration is managed via the `.env` file. Key variables include:

- Database credentials and hostnames
- Dagster and dbt settings
- Metabase connection details
- Pipeline schedule (cron and sensor interval)

---

## Extensibility

- Add more sources into assets/raw_assets.py
- Add more marts in churn_dbt/models/marts/
- Add sensors for external triggers
- Add anomaly detection on monitoring outputs
- Swap Postgres → Snowflake/BigQuery by adjusting dbt profiles

## Scalability & Production Readiness

- All services are stateless and can be scaled horizontally.
- Environment-driven configuration for easy deployment to any environment.
- Modular design: swap or scale components as needed.
- Add monitoring, alerting, and CI/CD for full production deployment.

---

## Note
    For Metabase UI 1st time sync with DB:
        `jdbc:postgresql://postgres:5432/hg_dw`
        DB_USER=hg_user
        DB_PASS=hg_password1
