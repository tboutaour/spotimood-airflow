from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable, TaskInstance
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, \
    DataprocSubmitPySparkJobOperator, ClusterGenerator
from operators.patched_dataproc_delete_cluster_operator import PatchedDataprocDeleteClusterOperator

# Schedule time variables
EXECUTION_DATE = '{{ ds }}'
EXECUTION_DATE_NODASH = '{{ ds_nodash }}'
START_DATETIME = '{{ }}'  # Start datetime
END_DATETIME = '{{ }}'  # Start datetime + 1 hour

# Project configuration
GCS_APP_BUCKET = Variable.get('gcs_app_bucket')
GCP_PROJECT = Variable.get('gcp_project')
GCE_REGION = Variable.get('gce_region')
GCE_ZONE = Variable.get('gce_zone')
GCS_LANDING_BUCKET = Variable.get('gcs_landing_bucket')
GCS_POSTGRESQL_JAR_PATH = Variable.get('postgresql_jar_gcp_path')

# Configuration for cluster
MASTER_MACHINE_TYPE = Variable.get('master_machine_type_aval')
WORKER_MACHINE_TYPE = Variable.get('worker_machine_type_aval')
NUM_WORKERS = int(Variable.get("num_workers_aval"))
CLUSTER_NAME = f'ephemeral-recent-played-spark-cluster-{EXECUTION_DATE_NODASH}'

PYSPARK_JOB = 'gs://' + GCS_APP_BUCKET + '/driver.py'

# Default arguments for Airflow DAG
DEFAULT_ARGS = {
    'wait_for_downstream': True,
    'depends_on_past': True,
    'start_date': datetime(2021, 1, 31),
    'project_id': GCP_PROJECT,
    'email_on_failure': False,
}

dag = DAG(
    'recent_played_ingestion',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 * * * *',
    max_active_runs=1
)

POSTGRES_BACKEND_HOST = Variable.get("POSTGRES_BACKEND_HOST")
POSTGRES_BACKEND_DATABASE = Variable.get("POSTGRES_BACKEND_DATABASE")
POSTGRES_BACKEND_PORT = Variable.get("POSTGRES_BACKEND_PORT")
POSTGRES_BACKEND_USER = Variable.get("POSTGRES_BACKEND_USER")
POSTGRES_BACKEND_PASSWORD = Variable.get("POSTGRES_BACKEND_PASSWORD")
SPOTIFY_CLIENT_ID = Variable.get("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = Variable.get("SPOTIFY_CLIENT_SECRET")
BACKENT_USER_ID = Variable.get("BACKENT_USER_ID")
GCS_TEMP_CHECKPOINT = Variable.get("GCS_TEMP_CHECKPOINT")

DATA_LANDING = f"gs://{GCS_LANDING_BUCKET}/playlist_information.avro"
DATA_LANDING_RECENT_PLAYED = f"gs://{GCS_LANDING_BUCKET}/recent_played.avro"
SONG_CLASSIFIED_TABLE = "spotify_user_playlist_songs_classified"
RECENT_PLAYED_CLASSIFIED_TABLE = "spotify_user_recent_played_songs_classified"
MODEL_PATH = f"gs://{GCS_LANDING_BUCKET}/model/ml_model.sav"

CLUSTER_DATA = ClusterGenerator(num_workers=NUM_WORKERS,
                                project_id=GCP_PROJECT,
                                image_version='1.5-debian10',
                                master_machine_type=MASTER_MACHINE_TYPE,
                                worker_machine_type=WORKER_MACHINE_TYPE,
                                properties={"spark-env:GCS_LANDING_BUCKET": GCS_LANDING_BUCKET,
                                            "spark-env:BQ_PROJECT_ID": GCP_PROJECT,
                                            "spark-env:POSTGRES_BACKEND_HOST": POSTGRES_BACKEND_HOST,
                                            "spark-env:POSTGRES_BACKEND_DATABASE": POSTGRES_BACKEND_DATABASE,
                                            "spark-env:POSTGRES_BACKEND_PORT": POSTGRES_BACKEND_PORT,
                                            "spark-env:POSTGRES_BACKEND_USER": POSTGRES_BACKEND_USER,
                                            "spark-env:POSTGRES_BACKEND_PASSWORD": POSTGRES_BACKEND_PASSWORD,
                                            "spark-env:SPOTIFY_CLIENT_ID": SPOTIFY_CLIENT_ID,
                                            "spark-env:SPOTIFY_CLIENT_SECRET": SPOTIFY_CLIENT_SECRET,
                                            "spark-env:BACKENT_USER_ID": BACKENT_USER_ID,
                                            "spark-env:DATA_LANDING": DATA_LANDING,
                                            "spark-env:DATA_LANDING_RECENT_PLAYED": DATA_LANDING_RECENT_PLAYED,
                                            "spark-env:SONG_CLASSIFIED_TABLE": SONG_CLASSIFIED_TABLE,
                                            "spark-env:RECENT_PLAYED_CLASSIFIED_TABLE": RECENT_PLAYED_CLASSIFIED_TABLE,
                                            "spark-env:MODEL_PATH": MODEL_PATH,
                                            "spark-env:GCS_TEMP_CHECKPOINT": GCS_TEMP_CHECKPOINT
                                            },
                                service_account_scopes=["https://www.googleapis.com/auth/cloud-platform"],
                                metadata={"PIP_PACKAGES": "google-cloud-storage==1.29.0 six==1.15.0"},
                                init_actions_uris=[f"gs://{GCS_APP_BUCKET}/pip_install.sh"],
                                idle_delete_ttl=600,
                                ).make()

# Create Dataproc cluster
create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster_dataproc',
    cluster_name=CLUSTER_NAME,
    cluster_config=CLUSTER_DATA,
    region=GCE_REGION,
    dag=dag
)

# Submit gcs ingestion job
submit_ingestion_job = DataprocSubmitPySparkJobOperator(
    task_id='run_recent_played_ingestion_pypark',
    main=PYSPARK_JOB,
    cluster_name=CLUSTER_NAME,
    region=GCE_REGION,
    dataproc_properties={'spark.jars.packages': 'org.apache.spark:spark-avro_2.12:2.4.5',
                         'spark.jars': GCS_POSTGRESQL_JAR_PATH},
    pyfiles=[f"gs://{GCS_APP_BUCKET}/reporting_online-0.1.0_SNAPSHOT-py3.7.egg"],
    arguments=["spotify_mood.main.spotify_recent_played_data_ingestion_main.py",
               "--start-day",
               START_DATETIME,
               "--end-day",
               END_DATETIME],
    dag=dag)

# Submit prediction job
submit_classification_job = DataprocSubmitPySparkJobOperator(
    task_id='run_recent_played_classification_pypark',
    main=PYSPARK_JOB,
    cluster_name=CLUSTER_NAME,
    region=GCE_REGION,
    dataproc_properties={'spark.jars.packages': 'org.apache.spark:spark-avro_2.12:2.4.5'},
    pyfiles=[f"gs://{GCS_APP_BUCKET}/reporting_online-0.1.0_SNAPSHOT-py3.7.egg"],
    arguments=["spotify_mood.main.hourly_recent_played_playlist_mood_tracker_main.py",
               "--start-day",
               START_DATETIME,
               "--end-day",
               END_DATETIME],
    dag=dag)

# Delete Dataproc cluster
delete_cluster = PatchedDataprocDeleteClusterOperator(
    task_id='delete_cluster_dataproc',
    cluster_name=CLUSTER_NAME,
    region=GCE_REGION,
    dag=dag)

create_cluster >> submit_ingestion_job >> submit_classification_job >> delete_cluster
