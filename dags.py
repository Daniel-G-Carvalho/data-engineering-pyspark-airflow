import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from transformation import *

def etl():
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    #pass the dataframes to the transformation function
    ratings_df = transform_avg_ratings(movies_df, users_df)
    #load the ratings dataframe 
    load_df_to_db(ratings_df)

default_args = {
    'owner': 'admin',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
# Instantiate the DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='An example DAG for Apache Airflow',
    schedule_interval=timedelta(days=1),  # Run the DAG daily
)

etl_task = PythonOperator(
    task_id='etl_task',
    python_callable=etl,
    dag=dag,
)

etl()