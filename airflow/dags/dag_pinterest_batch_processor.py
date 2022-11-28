from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

default_args = {'owner': 'awoyelu123',
                'retries': 5,
                'retry_delay': timedelta(minutes = 1)


}

work_dir = weather_dir = Variable.get("work_dir")

with DAG(
    default_args = default_args,
    dag_id = 'pintrest_batch_processor_v1',
    description= 'Dag to automate the batch processing ',
    start_date = datetime(2022,11,24),
    catchup= False,
    schedule_interval='*/10 * * * *'
    ) as dag:

        #start_zookeeper_task= BashOperator
        batch_consume_task = BashOperator(
        task_id='consume_batch_data',
        bash_command=f'cd {work_dir} && python3 batch_consumer.py ')
    
        batch_process_task = BashOperator(
        task_id='process_batch_data',
        bash_command=f'cd {work_dir} && python3 batch_processing.py')

        batch_consume_task >> batch_process_task