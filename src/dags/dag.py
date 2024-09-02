import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id = "datalake_marts_creation",
    default_args=default_args,
    schedule_interval=None,
)

mart_users = SparkSubmitOperator(
    task_id='showcase_by_users_2',
    dag=dag_spark,
    application ='/lessons/showcase_by_users_2.py' ,
    conn_id= 'yarn_spark',
    application_args = [ 
        "/user/master/data/geo/events/", 
        "/user/denis19/data/geo/cities/actual/geo.csv", 
        "/user/denis19/analytics/showcase_by_users/"
        ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '4g'
)

mart_zones = SparkSubmitOperator(
    task_id='showcase_in_section_of_zones_3',
    dag=dag_spark,
    application ='/lessons/showcase_in_section_of_zones_3.py' ,
    conn_id= 'yarn_spark',
    application_args = [
        "/user/master/data/geo/events/", 
        "/user/denis19/data/geo/cities/actual/geo.csv",
        "/user/denis19/analytics/showcase_in_section_of_zones/"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '4g'
)

mart_geo_activity = SparkSubmitOperator(
    task_id='recommendations_to_friends_4',
    dag=dag_spark,
    application ='/lessons/recommendations_to_friends_4.py' ,
    conn_id= 'yarn_spark',
    application_args = [
        "/user/master/data/geo/events/", 
        "/user/denis19/data/geo/cities/actual/geo.csv", 
        "/user/denis19/analytics/showcase_recommendations_to_friends/"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '4g'
)

mart_users >> mart_zones >> mart_geo_activity