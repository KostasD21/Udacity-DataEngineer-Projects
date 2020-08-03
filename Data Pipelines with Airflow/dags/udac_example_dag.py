from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from airflow.operators import CreateTablesOperator
from helpers import SqlQueries

s3_bucket = 'udacity-dend'
song_s3_key = "song_data"
log_s3_key = "log-data"
log_json_file = "s3://udacity-dend/log_json_path.json"

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 3,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_redshift = CreateTablesOperator(task_id='Create_tables',  
                                              dag=dag, 
                                              redshift_conn_id="redshift",
                                              file_path = "/home/workspace/airflow/create_tables.sql")

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_events",
    aws_credentials_id="aws_credentials",
    s3_bucket=s3_bucket,
    s3_key=log_s3_key,
    log_json_file = log_json_file,
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_songs",
    aws_credentials_id="aws_credentials",
    s3_bucket=s3_bucket,
    s3_key=song_s3_key,
    log_json_file = "",
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table_name="users",
    redshift_conn_id="redshift",
    append_data=False,
    sql_statement=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table_name="songs",
    redshift_conn_id="redshift",
    append_data=False,
    sql_statement=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table_name="artists",
    redshift_conn_id="redshift",
    append_data=False,
    sql_statement=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table_name="times",
    redshift_conn_id="redshift",
    append_data=False,
    sql_statement=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    dq_checks=[
    {'check_sql': "SELECT COUNT(*) FROM users"},
    {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
    {'check_sql': "SELECT COUNT(*) FROM artists"},
    {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},    
    {'check_sql': "SELECT COUNT(*) FROM songs"},
    {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
    {'check_sql': "SELECT COUNT(*) FROM songplays"},
    {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result': 0},
    {'check_sql': "SELECT COUNT(*) FROM times"},
    {'check_sql': "SELECT COUNT(*) FROM times WHERE start_time is null", 'expected_result': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_redshift >> [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator
