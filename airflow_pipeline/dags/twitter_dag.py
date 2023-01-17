import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from os.path import join
from airflow.utils.dates import days_ago
from pathlib import Path

with DAG(dag_id = "TwitterDAG", start_date=days_ago(2), schedule_interval="@daily") as dag:
    BASE_FOLDER = join(
        str(Path("~/Documents").expanduser()),
        "airflowalura/datalake/{stage}/twitter_datascience/{partition}",
    )
    PARTITION_FOLDER_EXTRACT = "extract_date={{ data_interval_start.strftime('%Y%m-%d') }}"

    TIMESTAMP_FOMART = "%Y-%m-%dT%H:%M:%S.00Z" # ISO 8601/RFC 3339
    query = "datascience"

    twitter_operator = TwitterOperator(file_path=join(BASE_FOLDER.format(stage="Bronze", partion=PARTITION_FOLDER_EXTRACT),
                                        "datascience_{{ ds_nodash }}.json"),
                                        query=query, start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                                        end_time="{{ data_interval_endstrftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                                        task_id="twitter_datascience")

    twitter_transform = SparkSubmitOperator(task_id="transform_twitter_datascience",
                                            application="src/scripts/extracao_api_twitter.py",
                                            name="twitter_transformation",
                                            application_args=["--src", BASE_FOLDER.format(stage="Bronze", partion=PARTITION_FOLDER_EXTRACT), 
                                            "--dest", BASE_FOLDER.format(stage="Silver", partion=""), 
                                            "--process-date", "{{ ds }}"])

twitter_operator >> twitter_transform