import pandas as pd
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 1, 1),
    'catchup' : False
}

dag = DAG(
    'Final_project',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

api_check = HttpSensor(
    task_id='api_check',
    http_conn_id='airflow_http',  
    endpoint='giveaways/', 
    method='GET', 
    dag=dag,
)

task_get_giveways = SimpleHttpOperator(
    task_id = 'task_get_giveaways',
    http_conn_id = 'airflow_http',
    endpoint = 'giveaways/',
    response_filter = lambda response: json.loads(response.text) ,
    log_response = True,
    dag = dag,
)

def convert_json_to_csv(ti):
    #ti will help xcom pull information about the task execution environment
    response_data = ti.xcom_pull(task_ids="task_get_giveaways")

    if response_data:
        df = pd.DataFrame(response_data)
        csv_file_path = '/home/kushal/airflow/Airflow_assignments/giveaways.csv'
        df.to_csv(csv_file_path, index=False)
    else:
        print("JSON data unavailable in XCom")

task_to_csv = PythonOperator(
    task_id = 'task_to_csv',
    python_callable = convert_json_to_csv,
    provide_context = True, #this allows function to use xcom
    dag = dag,
)

check_file = FileSensor(
    task_id = 'check_file',
    filepath = '/home/kushal/airflow/Airflow_assignments/giveaways.csv',
    poke_interval = 15,
    timeout = 300,
    mode = 'poke',
    dag = dag,
)

move_file2tmp = BashOperator(
    task_id = 'move_file2tmp',
    bash_command = 'mv /home/kushal/airflow/Airflow_assignments/giveaways.csv /tmp/',
    dag = dag,
)

to_postgres = PostgresOperator(
    task_id = 'to_postgres',
    postgres_conn_id = 'postgres',
    sql = """TRUNCATE TABLE giv_input;
                COPY giv_input FROM '/tmp/giveaways.csv' CSV HEADER;""",
    dag = dag,
)

spark_submit = BashOperator(
        task_id='spark_submit',
        bash_command='/home/kushal/.local/bin/spark-submit --driver-class-path /usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.5.0.jar /home/kushal/airflow/scripts/spark.py',
        dag = dag,
    )

from_postgres = PostgresOperator(
        task_id="from_postgres",
        postgres_conn_id="postgres",
        sql="SELECT * FROM giv_input_af;",
        dag = dag,
    )

def log_output(ti):
        result = ti.xcom_pull(task_ids='from_postgres')
        for row in result:
         print(row) 
    
log_result_final = PythonOperator(
    task_id = 'log_result_final',
    python_callable = log_output,
    provide_context = True,
    dag = dag,
    )

api_check >> task_get_giveways >> task_to_csv >> check_file >> move_file2tmp >> to_postgres >> spark_submit >> from_postgres >> log_result_final
