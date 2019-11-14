from datetime import timedelta

import airflow
from airflow import DAG
from airflow import exceptions
from airflow.operators.bash_operator import BashOperator
#from airflow.contrib.operators import gcs_to_bq
from airflow.exceptions import AirflowException


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['herman.cheung@loblaw.ca'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    'framework',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval="@daily",
)
# t1, t2 and t3 are examples of tasks created by instantiating operators
read_config = BashOperator(
    task_id='read_config',
    bash_command='date',
    dag=dag,
)

sensor = BashOperator(
    task_id='sensor',
    bash_command='date',
    dag=dag,
)
#### Sensor Documentation

sensor.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

dag.doc_md = __doc__

validate = BashOperator(
    task_id='validate',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag,
)

templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

ingest = BashOperator(
    task_id='ingest',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

read_config >> sensor

sensor >> validate >> ingest