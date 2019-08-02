from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)

#Constraints
load_time_dimension_table_task_id='Load_artist_dim_table'
load_artist_dimension_table_task_id='Load_artist_dim_table'
load_song_dimension_table_task_id='Load_song_dim_table'
load_user_dimension_table_task_id='Load_user_dim_table'
load_songplays_fact_tabletask_id='Load_songplays_fact_table'
dag_name='sparkify_etl'
stage_songs_task_id='Stage_songs'
begin_execution_task_id='Begin_execution'
stage_events_task_id='stage_events'
stop_execution_task_id='stop_execution'
run_data_quality_checks_task_id='run_data_quality_checks'

default_args = {
    'owner': 'fs',
    'start_date': datetime(2018, 5, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=3
        )

start_operator = DummyOperator(task_id=begin_execution_task_id,  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id=stage_events_task_id,
    dag=dag,
    provide_context=True,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    extra_params="format as json 's3://udacity-dend/log_json_path.json'",
    execution_date=start_date
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id=stage_songs_task_id,
    dag=dag,
    provide_context=True,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    extra_params="json 'auto' compupdate off region 'us-west-2'",
    execution_date=start_date
)

load_songplays_table = LoadFactOperator(
    task_id=load_songplays_fact_tabletask_id,
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_source=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id=load_user_dimension_table_task_id'
    start_date= datetime(2018, 5, 1),
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="users",
    sql_source=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id=load_song_dimension_table_task_id,
    redshift_conn_id="redshift",
    table="songs",
    aws_credentials_id="aws_credentials",
    start_date= datetime(2018, 5, 1),
    sql_source=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id=load_artist_dimension_table_task_id,
    redshift_conn_id="redshift",
    table="artists",
    start_date= datetime(2018, 5, 1),
    aws_credentials_id="aws_credentials",
    sql_source=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id=load_time_dimension_table_task_id,
    redshift_conn_id="redshift",
    table="time",
    aws_credentials_id="aws_credentials",
    start_date= datetime(2018, 5, 1),
    sql_source=SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id=run_data_quality_checks_task_id,
    redshift_conn_id="redshift",
    table="time",
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    tables=["staging_events", "users", 'staging_songs', "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id=stop_execution_task_id,  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator