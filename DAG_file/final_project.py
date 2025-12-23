from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

from udacity.common.final_project_sql_statements import SqlQueries


# -------------------------------------------------
# DEFAULT ARGUMENTS
# -------------------------------------------------
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}


# -------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------
dag = DAG(
    'udacity_sparkify_pipeline',
    default_args=default_args,
    description='Sparkify ETL pipeline',
    schedule_interval='@hourly'
)


# -------------------------------------------------
# START / END
# -------------------------------------------------
start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)


# -------------------------------------------------
# STAGING TASKS
# -------------------------------------------------
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    params={
        'redshift_conn_id': 'redshift',
        'aws_credentials_id': 'aws_credentials',
        'table': 'staging_events',
        's3_bucket': 'udacity-dend',
        's3_key': 'log-data',
        'json_path': 's3://udacity-dend/log_json_path.json'
    }
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    params={
        'redshift_conn_id': 'redshift',
        'aws_credentials_id': 'aws_credentials',
        'table': 'staging_songs',
        's3_bucket': 'udacity-dend',
        's3_key': 'song-data',
        'json_path': 'auto'
    }
)


# -------------------------------------------------
# FACT TABLE
# -------------------------------------------------
load_songplays_fact_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    params={
        'redshift_conn_id': 'redshift',
        'table': 'songplays',
        'sql': SqlQueries.songplay_table_insert
    }
)


# -------------------------------------------------
# DIMENSION TABLES
# -------------------------------------------------
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    params={
        'redshift_conn_id': 'redshift',
        'table': 'users',
        'sql': SqlQueries.user_table_insert,
        'append_only': False
    }
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    params={
        'redshift_conn_id': 'redshift',
        'table': 'songs',
        'sql': SqlQueries.song_table_insert,
        'append_only': False
    }
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    params={
        'redshift_conn_id': 'redshift',
        'table': 'artists',
        'sql': SqlQueries.artist_table_insert,
        'append_only': False
    }
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    params={
        'redshift_conn_id': 'redshift',
        'table': 'time',
        'sql': SqlQueries.time_table_insert,
        'append_only': False
    }
)


# -------------------------------------------------
# DATA QUALITY CHECKS
# -------------------------------------------------
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    params={
        'redshift_conn_id': 'redshift',
        'tests': [
            {
                'sql': 'SELECT COUNT(*) FROM songplays WHERE songplay_id IS NULL',
                'expected_result': 0
            }
        ]
    }
)


# -------------------------------------------------
# TASK DEPENDENCIES
# -------------------------------------------------
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_fact_table

load_songplays_fact_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
]

[
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
] >> run_quality_checks

run_quality_checks >> end_operator
