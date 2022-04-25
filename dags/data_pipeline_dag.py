from pickle import FALSE


try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def source_to_raw_1():
    print("The source to the first raw data ....")


def source_to_raw_2():
    print("The source to the second raw data ....")


def raw_to_formatted_1():
    print("The first transform data ....")


def raw_to_formatted_2():
    print("The second transform data ....")


def produce_usage():
    print("Usage ....")


def index_to_elastic():
    print("Index to elastic ....")


with DAG(
    dag_id="data_pipeline_dag",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2021, 1, 1)
    },
    catchup=False
) as f:

    source_to_raw_1 = PythonOperator(
        task_id="source_to_raw_1",
        python_callable=source_to_raw_1,
        provide_context=True,

    )
    source_to_raw_2 = PythonOperator(
        task_id="source_to_raw_2",
        python_callable=source_to_raw_2,
        provide_context=True,

    )
    raw_to_formatted_1 = PythonOperator(
        task_id="raw_to_formatted_1",
        python_callable=raw_to_formatted_1,
        provide_context=True,

    )
    raw_to_formatted_2 = PythonOperator(
        task_id="raw_to_formatted_2",
        python_callable=raw_to_formatted_2,
        provide_context=True,

    )
    produce_usage = PythonOperator(
        task_id="produce_usage",
        python_callable=produce_usage,
        provide_context=True,

    )
    index_to_elastic = PythonOperator(
        task_id="index_to_elastic",
        python_callable=index_to_elastic,
        provide_context=True,

    )

    source_to_raw_1 >> raw_to_formatted_1 >> produce_usage >> index_to_elastic
    source_to_raw_2 >> raw_to_formatted_2 >> produce_usage >> index_to_elastic
