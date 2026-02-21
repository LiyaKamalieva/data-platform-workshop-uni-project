"""
Domain: Clickstream
Data Product: Top Products
Owner: Alena
SLA: every 10 minutes
Published interfaces: DuckDB(top_products), Redis(top_products)
"""






from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import duckdb
from airflow.providers.redis.hooks.redis import RedisHook
from datetime import datetime, timedelta
import json
import os

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def read_aggregates_from_kafka(**context):
    """Упрощённое чтение последних сообщений из Kafka (можно заменить на Consumer)."""
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        'aggregated_clicks',
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000
    )
    aggregates = {}
    for msg in consumer:
        pid = msg.value['product_id']
        cnt = msg.value['count']
        aggregates[pid] = aggregates.get(pid, 0) + cnt
    consumer.close()
    # Сохраняем в XCom
    context['ti'].xcom_push(key='aggregates', value=aggregates)

def join_with_orders(**context):
    """Читает агрегаты из XCom, запрашивает исторические заказы из DuckDB, объединяет."""
    aggregates = context['ti'].xcom_pull(key='aggregates', task_ids='read_kafka') or {}

    con = duckdb.connect(DUCKDB_PATH)

    # Таблица raw.orders (создаём если её ещё нет)
    con.execute("""
        CREATE TABLE IF NOT EXISTS orders (
          order_id BIGINT,
          product_id BIGINT,
          quantity BIGINT,
          price DOUBLE,
          order_date TIMESTAMP
        )
    """)

    rows = con.execute("""
        SELECT product_id,
               COALESCE(SUM(quantity), 0) as total_orders,
               COALESCE(AVG(price), 0) as avg_price
        FROM orders
        GROUP BY product_id
    """).fetchall()

    con.close()

    orders_data = {row[0]: {'total_orders': row[1], 'avg_price': row[2]} for row in rows}

    final = []
    for pid, click_count in aggregates.items():
        orders = orders_data.get(pid, {'total_orders': 0, 'avg_price': 0})
        final.append({
            'product_id': int(pid),
            'click_count': int(click_count),
            'total_orders': int(orders['total_orders']),
            'avg_price': float(orders['avg_price']),
            'score': click_count * 0.7 + orders['total_orders'] * 0.3
        })

    final.sort(key=lambda x: x['score'], reverse=True)
    top10 = final[:10]
    context['ti'].xcom_push(key='top10', value=top10)

def write_to_redis_bigquery(**context):
    top10 = context['ti'].xcom_pull(key='top10', task_ids='join_orders') or []

    # 1) Redis (feature store)
    redis_hook = RedisHook(redis_conn_id='redis_default')
    redis_conn = redis_hook.get_conn()
    redis_conn.hset('top_products', mapping={str(item['product_id']): json.dumps(item) for item in top10})

    # 2) DuckDB (витрина dwh.top_products)
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS top_products (
          product_id BIGINT,
          click_count BIGINT,
          total_orders BIGINT,
          avg_price DOUBLE,
          score DOUBLE,
          updated_at TIMESTAMP
        )
    """)

    # Вставляем строки
    for item in top10:
        con.execute("""
            INSERT INTO top_products (product_id, click_count, total_orders, avg_price, score, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [item['product_id'], item['click_count'], item['total_orders'], item['avg_price'], item['score']])

    con.close()

with DAG(
    'recommendation_pipeline',
    default_args=default_args,
    schedule_interval='*/10 * * * *',  # каждые 10 минут
    catchup=False,
    tags=['example'],
) as dag:

    t1 = PythonOperator(
        task_id='read_kafka',
        python_callable=read_aggregates_from_kafka,
    )

    t2 = PythonOperator(
        task_id='join_orders',
        python_callable=join_with_orders,
    )

    t3 = PythonOperator(
        task_id='write_targets',
        python_callable=write_to_redis_bigquery,
    )

    t1 >> t2 >> t3