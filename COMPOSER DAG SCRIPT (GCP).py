#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

# Define default arguments for the DAG
default_args = {
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email': ['your-email@example.com'],
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='A data pipeline to extract data from GCS, transform it using Dataproc, and load it into BigQuery',
    schedule_interval='0 20 * * *',  # Schedule to run at 8 PM daily
)

# Define the PySpark job
pyspark_job = {
    'reference': {
        'project_id': 'icezy-423617'
    },
    'placement': {
        'cluster_name': 'ecommerce'
    },
    'pyspark_job': {
        'main_python_file_uri': 'gs://your-bucket/data_transformation.py',
    },
}

# Task to submit the PySpark job to Dataproc
submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job',
    job=pyspark_job,
    region='us-central1',
    project_id='icezy-423617',
    dag=dag,
)

# Task to check if the table exists in BigQuery
check_table_exists = BigQueryCheckOperator(
    task_id='check_table_exists',
    use_legacy_sql=False,
    sql='SELECT 1 FROM `icezy-423617.Ecommerce.sales_transaction` LIMIT 1;',
    dag=dag,
)

# Task to create fact table in BigQuery
create_fact_table = BigQueryInsertJobOperator(
    task_id='create_fact_table',
    configuration={
        "query": {
            "query": """
            CREATE OR REPLACE TABLE `icezy-423617.Ecommerce.fact_table` AS
            SELECT
                user_id,
                SUM(price) AS total_sales,
                COUNT(*) AS transaction_count
            FROM
                `icezy-423617.Ecommerce.sales_transaction`
            GROUP BY
                user_id;
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

# Task to create dimension table in BigQuery
create_dimension_table = BigQueryInsertJobOperator(
    task_id='create_dimension_table',
    configuration={
        "query": {
            "query": """
            CREATE OR REPLACE TABLE `icezy-423617.Ecommerce.dimension_table` AS
            SELECT
                category_code,
                AVG(price) AS avg_price,
                COUNT(*) AS product_count
            FROM
                `icezy-423617.Ecommerce.sales_transaction`
            GROUP BY
                category_code;
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

# Define task dependencies
submit_pyspark_job >> check_table_exists >> [create_fact_table, create_dimension_table]

