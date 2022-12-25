from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
#####################
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'abdullah',
    'start_date': datetime(2018, 11, 1, 0, 0, 0, 0),
    'end_date': datetime(2018, 11, 30, 0, 0, 0, 0),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_project_airflow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_table = PostgresOperator(task_id="create_table",dag=dag,postgres_conn_id="redshift",sql='create_tables.sql')

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table="staging_events",
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    LOG_JSONPATH="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table="staging_songs",
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    LOG_JSONPATH="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = "public.songplays",
    redshift_conn_id='redshift',
    insert_tbl=SqlQueries.songplay_table_insert,
    truncate=True
    )

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dimension_table',
    dag=dag,
    redshift_conn_id='redshift',
    table = "public.users",
    insert_tbl=SqlQueries.user_table_insert,
    truncate=True
    )

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dimension_table',
    dag=dag,
    redshift_conn_id='redshift',
    table = "public.songs",
    insert_tbl=SqlQueries.song_table_insert,
    truncate=True
    )

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dimension_table',
    dag=dag,
    redshift_conn_id='redshift',
    table = "public.artists",
    insert_tbl=SqlQueries.artist_table_insert,
    truncate=True
    )

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dimension_table',
    dag=dag,
    redshift_conn_id='redshift',
    table = "public.\"time\"",
    insert_tbl=SqlQueries.time_table_insert,
    truncate=True
)

run_quality_checks=DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    data_quality_checks = [
        {'data_sql': 'select count(*) from public.songs', 'real_value': 24},
        {'data_sql': 'select count(*) from public.artists', 'real_value': 24 },
        {'data_sql': 'select count(*) from public.users', 'real_value': 22},
        {'data_sql': 'select count(*) from public.\"time\"', 'real_value': 330},
        {'data_sql': 'select count(*) from public.songplays ', 'real_value': 330 }
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
#Task dependency
start_operator >> create_table 
create_table >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator

