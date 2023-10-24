from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook, ElasticsearchPythonHook
from airflow.operators.bash import BashOperator
from pyspark.sql.types import *
from datetime import datetime,date
import time
import logging
import traceback
import shutil
import base64
import json
import os
import csv
from elasticsearch import Elasticsearch



### Read logs and send ElasticSearch ####

def read_file():
    dst = "/opt/airflow/data_logs/completed_logs"
    src = "/opt/airflow/data_logs/created_logs"

    try:
        for filename in os.listdir(src):
            f = os.path.join(src,filename)
            print(f)
            if os.path.isfile(f):
                my_data = ""
                with open(f, "r") as log_file:
                    file = log_file.readlines()

                shutil.move(f,os.path.join(dst,filename))
    except Exception as e:
        logging.error(traceback.format_exc())

    else:
        print("to Elasticsearch completed ...")

#### Read from Elasticsearh and write csv ###

spark_master = "spark://spark:7077"
postgres_driver_jar = "/opt/airflow/resources/jar/postgresql-42.5.0.jar"
file_path="/opt/bitnami/spark/resources/data/information_creation.csv"

with DAG(dag_id='test_pipeline', schedule_interval='@daily', 
start_date=datetime(2022, 1, 1), catchup = False) as dag:

    start = DummyOperator(task_id="start")

    transfer_elastic = PythonOperator(
        task_id="transfer_elastic",
        python_callable=read_file
    )
    es_to_csv = BashOperator(
        task_id='es_to_csv',
        bash_command='python3 /opt/airflow/dags/es_to_csv.py',
    )

    spark_job = SparkSubmitOperator(
        task_id="spark_process",
        application= '/opt/bitnami/spark/app/write_postgres.py',
        name="load-postgres",
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
        application_args=[file_path]

    )
    end = DummyOperator(task_id="end")

    #start >> spark_job >> end
    start >> transfer_elastic >> es_to_csv >> spark_job >> end