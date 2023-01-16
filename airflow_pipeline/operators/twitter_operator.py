import sys
sys.path.append("airflow_pipeline")

from airflow.models import BaseOperator, DAG, TaskInstance
from hook.twitter_hook import TwitterHook
import json
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path

class TwitterOperator(BaseOperator):

    template_fields = ["query", "file_path", "start_time", "end_time"]

    def __init__(self,file_path, end_time, start_time, query, **kwargs):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.file_path = file_path
        super().__init__(**kwargs)

    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        end_time = self.end_time
        start_time = self.start_time
        query = self.query

        self.create_parent_folder()

        with open(self.file_path, "w") as output_file:
            for pgnas in TwitterHook(end_time, start_time, query).run():
                json.dump(pgnas, output_file, ensure_ascii=False)
                output_file.write("\n")

if __name__ == "__main__":

    #Montando url
    TIMESTAMP_FOMART = "%Y-%m-%dT%H:%M:%S.00Z" # ISO 8601/RFC 3339

    end_time = datetime.now().strftime(TIMESTAMP_FOMART)
    start_time = (datetime.now() + timedelta(days=-1)).date().strftime(TIMESTAMP_FOMART)
    query = "datascience"

    with DAG(dag_id = "TwitterTest", start_date=datetime.now()) as dag:
        to = TwitterOperator(file_path=join("datalake/twitter_datascience",
                                            f"extract_date={datetime.now().date()}",
                                            f"datascience_{datetime.now().date().strftime('%Y%m%d')}.json"),
                                            query=query, start_time=start_time, end_time=end_time, task_id="test_run")
        ti = TaskInstance(task=to)
        to.execute(ti.task_id)
        

