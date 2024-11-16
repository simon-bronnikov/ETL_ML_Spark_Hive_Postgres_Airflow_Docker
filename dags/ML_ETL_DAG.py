from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'Simon',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'ML_ETL_prediction',
    default_args = default_args,
    description = 'Обработка датасета, загрузка в postgres, обучение модели, предсказание',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    start = EmptyOperator(
        task_id='start'
    )

    air_data_analysis = SparkSubmitOperator(
        task_id='data_analysis',
        application='./dags/ML_ETL_spending_prediction/csv-postgres.py', 
        verbose=True,
        conn_id='spark_default',
        conf={"spark.master": "spark://spark-master:7077"}, 
    )

    air_train_model = SparkSubmitOperator(
        task_id='train_model',
        application='./dags/ML_ETL_spending_prediction/model_train.py', 
        verbose=True,
        conn_id='spark_default',
        conf={"spark.master": "spark://spark-master:7077"}, 
    )

    air_pred_model = SparkSubmitOperator(
        task_id='pred_model',
        application='./dags/ML_ETL_spending_prediction/predict_model.py', 
        verbose=True,
        conn_id='spark_default',
        conf={"spark.master": "spark://spark-master:7077"}, 
    )

    finish = EmptyOperator( 
        task_id = 'finish'
    )

    start >> air_data_analysis >> air_train_model >> air_pred_model >> finish
