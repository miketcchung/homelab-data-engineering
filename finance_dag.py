import pandas as p
import yfinance as yf
import os
import csv
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def cad_ticker_price_task():
    """Fetch 5-year historical price data for Canadian equities and load into PostgreSQL.

    Reads ticker symbols from watchlist/watchlist_cad.csv, appends the '.TO' suffix
    for the Toronto Stock Exchange, downloads OHLCV data via yfinance starting 5 years
    ago, and writes the result to the finance.watchlist_cad_ticker_price table using
    the Airflow connection pg_data_science_conn. Replaces the table on each run.
    """

    # Open csv file to get watchlist and read into list
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(dag_folder, "watchlist/watchlist_cad.csv")
    
    with open(csv_path, mode="r", encoding="utf-8") as file:
        csv_reader = csv.reader(file)
        watchlist_cad = list(csv_reader)[0]
        
    watchlist_cad = [x + ".to" for x in watchlist_cad]

    # Create pandas dataframe with data starting from 5 years agi
    start_dt = date.today() - relativedelta(years=5)

    finance_df = yf.download(watchlist_cad, start=start_dt)
    finance_df = finance_df.stack(level=1)
    finance_df = finance_df.reset_index()
    finance_df = finance_df.rename(columns={'level_0': 'Date'})
    finance_df.index.name = None

    # Write dataframe to SQL table
    db_hook = PostgresHook(postgress_conn_id='pg_data_science_conn')
    engine = db_hook.get_sqlachemy_engine()
    table_name = "finance.watchlist_cad_ticker_price"

    finance_df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False,
        chunksize=1000
    )
    print(f"{table_name} written to PostgreSQL")


# Default arguments for Airflow DAG
default_args = {
    "owner": "mike",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Airflow DAG
with DAG(
    dag_id="finance_pipeline_to_postgres",
    default_args=default_args,
    schedule="@daily",
    catchup=False
) as dag:
    write_to_db = PythonOperator(
        task_id="watchlist_cad_ticker_price_to_postgres",
        python_callable=cad_ticker_price_task
    )
    write_to_db