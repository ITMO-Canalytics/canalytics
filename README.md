# Canalytics: Suez Canal Traffic Analytics Pipeline

A streamlined analytics pipeline for Suez Canal traffic data, delivering production-ready code, a concise analytical report, and a presentation slide deck.

## Project Overview

This project collects, processes, and analyzes shipping traffic data for major maritime chokepoints, with a focus on the Suez Canal. It combines AIS (Automatic Identification System) ship tracking data with relevant news articles to provide insights into maritime traffic patterns and potential disruptions.

## Architecture

The project follows a modern data engineering architecture:

1. **Data Collection**: Python scripts collect AIS data and news articles
2. **Orchestration**: Apache Airflow manages the data pipeline workflow
3. **Storage**: AWS S3 for raw data, ClickHouse for analytical data processing
4. **Processing**: ETL scripts transform raw data into analysis-ready datasets
5. **Analysis**: Jupyter notebooks for exploratory analysis and visualization

## Directory Structure

```
canalytics/
├── data/                       # Raw and processed data storage
│   ├── raw/                    # Raw CSV and JSON from collectors
│   └── processed/              # Cleaned datasets
├── collectors/                 # Data collection scripts
│   ├── ais_collector.py        # AIS data fetcher
│   └── news_collector.py       # News headlines scraper
├── pipeline/                   # Orchestration & ingestion
│   ├── Dockerfile              # Container for collectors
│   └── airflow/                # Airflow DAGs and configs
│       └── dags/
│           └── dag_collect.py
├── storage/                    # Storage utilities
│   ├── s3_loader.py            # S3 storage operations
│   ├── clickhouse_loader.py    # ClickHouse database operations
│   ├── db_loader.py            # Combined loader utilities
│   └── __init__.py             # Package initialization
├── analysis/                   # Data preparation and analysis
│   ├── etl.py                  # ETL pipeline scripts
│   └── notebooks/              # Jupyter notebooks with exploratory analysis
│       └── suez_canal_analysis.ipynb
├── report/                     # Report and slides
│   ├── report.md               # Markdown source for report
│   └── slides/                 # Reveal.js slide deck
├── tests/                      # Unit and integration tests
├── .github/                    # CI/CD workflows
│   └── workflows/
│       └── ci.yml
├── docker-compose.yml          # Local development orchestration
├── requirements.txt            # Python dependencies
└── README.md                   # Project overview and setup instructions
```

## Setup and Installation

### Prerequisites

- Docker and Docker Compose
- AWS account with S3 access
- API keys for AIS data and news services

### Quick Start

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/canalytics.git
   cd canalytics
   ```

2. Create a `.env` file with your credentials:

   ```bash
   cp env.example .env
   # Edit .env with your actual values
   ```

3. Start the services:

   ```bash
   docker-compose up -d
   ```

4. Access the services:
   - Airflow: http://localhost:8081
   - Jupyter: http://localhost:8888 (token: canalytics)
   - ClickHouse: localhost:8123

## Usage

### Running the Pipeline

1. In Airflow, enable the `canalytics_collect_data` DAG
2. The DAG will run daily, collecting fresh data
3. Manually trigger the DAG for immediate execution

### Analyzing Data

1. Open Jupyter at http://localhost:8888
2. Navigate to `analysis/notebooks/suez_canal_analysis.ipynb`
3. Run the notebook to analyze the latest data

### Extending the Pipeline

- Add new data sources in the `collectors/` directory
- Create new DAGs in `pipeline/airflow/dags/`
- Add new analysis notebooks in `analysis/notebooks/`

## Environment Variables

The following environment variables are required:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_REGION=us-east-1
CANALYTICS_S3_BUCKET=canalytics-data

# Data Collection API Keys
AIS_TOKEN=your_ais_token
NEWS_API_KEY=your_news_api_key

# ClickHouse Configuration
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_DB=canalytics
CLICKHOUSE_USER=canalytics_user
CLICKHOUSE_PASSWORD=canalytics_password

# Airflow Configuration
AIRFLOW__WEBSERVER__ADMIN_USER=admin
AIRFLOW__WEBSERVER__ADMIN_PASSWORD=admin
AIRFLOW__WEBSERVER__ADMIN_EMAIL=admin@example.com

# Jupyter Configuration
JUPYTER_TOKEN=canalytics
```

## Team & Responsibilities

- **Dimitry** – Lead Data Collector

  - Python scripts for AIS and news collection
  - Automated data collection

- **Timur** – Pipeline & Orchestration Engineer

  - Docker containerization
  - Airflow task orchestration

- **Umar** – Storage & Infrastructure Engineer

  - AWS S3 and ClickHouse setup
  - Database loading utilities

- **Maria** – Data Analyst & Visualization Lead
  - ETL implementation
  - Analysis and visualization

## License

This project is licensed under the MIT License - see the LICENSE file for details.
