from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="exemplo-job", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
    # Tasks are represented as operators
    start = BashOperator(task_id="start", bash_command="echo start")
    end = BashOperator(task_id="end", bash_command="echo end")
    
    
    # Set dependencies between tasks
    start >> end