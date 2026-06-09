# homelab-data-engineering

Data engineering pipelines for a self-hosted homelab running cybersecurity and AI tools. Orchestrated with Apache Airflow, backed by PostgreSQL, and designed to support analytics and AI-assisted workflows.

## Overview

This repo contains Airflow DAGs, EDA notebooks, and supporting utilities that automate data collection and loading into a local PostgreSQL instance.

**Stack:**
- Apache Airflow 
- PostgreSQL
- Python (pandas / yfinance)
- Jupyter Notebooks

## Pipelines

### `finance_pipeline_to_postgres` (finance_dag.py)

Fetches 5-year historical ticker price data for a watchlist of equities and writes it to PostgreSQL on a daily schedule.

**Detailed Description**
1. Reads ticker symbols from `watchlist/watchlist_cad.csv`
3. Downloads ticker price history via `yfinance` 
4. Stacks and reshapes the multi-level DataFrame
5. Writes to `finance.watchlist_cad_ticker_price` 

## Watchlist

Edit `watchlist/watchlist_cad.csv` to customize which tickers are tracked:

## Deployment

Clone this repo in your Airflow server's `dags/` directory.

## Related

- Homelab writeups: [fsstance.medium.com](https://fsstance.medium.com)
