from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import sys
import os

# Add the project root to Python path
sys.path.append("/opt/airflow/dags/repo")

from cmbs_scraper import CMBSScraper
from config import COMPANY_NAMES

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def scrape_company(company_id, company_name, **context):
    """Task to scrape data for a specific CMBS company."""
    scraper = CMBSScraper()
    try:
        scraper.scrape_company(
            company_name,
            date_from="2018-01-01",
            scrape=True,
            download=True
        )
    except Exception as e:
        print(f"Error scraping {company_id}: {str(e)}")
        raise

with DAG(
    'cmbs_scraper',
    default_args=default_args,
    description='Scrapes CMBS ABS-EE filings from SEC EDGAR',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sec', 'cmbs', 'scraping'],
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    end = EmptyOperator(
        task_id='end',
    )

    # Create a task for each company
    company_tasks = []
    for company_id, company_name in COMPANY_NAMES.items():
        task = PythonOperator(
            task_id=f'scrape_{company_id.lower()}',
            python_callable=scrape_company,
            op_kwargs={
                'company_id': company_id,
                'company_name': company_name,
            },
        )
        company_tasks.append(task)

    # Set task dependencies
    start >> company_tasks >> end 