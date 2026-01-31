from airflow import DAG
from airflow.utils.dates import days_ago
from kafka_operators import KafkaHealthCheckOperator, KafkaTopicCheckOperator, KafkaConsumerGroupOperator, KafkaDataFlowOperator
from telegram_notifier import send_telegram_alert
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_telegram_alert,
}

with DAG(
    'kafka_monitoring_dag',
    default_args=default_args,
    description='Monitor Kafka Broker Health and Connectivity',
    schedule_interval=timedelta(minutes=5),
    start_date=days_ago(1),
    catchup=False,
    tags=['monitoring', 'kafka'],
) as dag:

    # Task 1: Check Kafka broker health
    check_kafka_health = KafkaHealthCheckOperator(
        task_id='check_kafka_health',
        bootstrap_servers='kafka:29092,kafka2:29092,kafka3:29092'
    )

    # Task 2: Verify topic availability
    check_topic = KafkaTopicCheckOperator(
        task_id='check_topic_availability',
        bootstrap_servers='kafka:29092,kafka2:29092,kafka3:29092',
        topic_name='product_view_local',
        expected_partitions=3  # Adjust based on your topic configuration
    )

    # Task 3: Monitor consumer group (Basic Status)
    check_consumer_group = KafkaConsumerGroupOperator(
        task_id='check_consumer_group',
        bootstrap_servers='kafka:29092,kafka2:29092,kafka3:29092',
        group_id='spark-streaming-consumer'  # Adjust to match your Spark consumer group
    )

    # Task 4: Monitor Data Flow (Lag & Throughput)
    check_data_flow = KafkaDataFlowOperator(
        task_id='check_data_flow',
        bootstrap_servers='kafka:29092,kafka2:29092,kafka3:29092',
        topic_name='product_view_local',
        group_id='spark-streaming-consumer',
        max_lag_threshold=5000,
        min_throughput_threshold=1
    )

    # Define task dependencies
    check_kafka_health >> [check_topic, check_consumer_group, check_data_flow]
