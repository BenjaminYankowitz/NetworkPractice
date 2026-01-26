import os
from datetime import datetime
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout
from dotenv import load_dotenv
import pika
import exchangeMessages_pb2

load_dotenv()
DATABASE_URL = os.getenv("EXCHANGE_DATABASE_URL")


pool = ConnectionPool(DATABASE_URL, min_size=2, max_size=10, kwargs={"row_factory": dict_row})


def init_db():
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS activeOrders (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(5) NOT NULL,
                    owner INT,
                    buyside bool,
                    price INT,
                    quantity INT,
                    filled INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY
                )
            """)



connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.queue_declare(queue='exchange_queue')
channel.queue_declare(queue='signup_queue')

def on_order(ch, method, props, body):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            symbol = body.symbol
            side = body.side
            sortBy =  "ASC" if side else "DESC"
            cur.execute("""
                        SELECT * FROM activeOrders
                        Where symbol=%s
                        WHERE side=%s
                        ORDER BY price %s;""",
                        (symbol, not side,sortBy))
            print(cur.fetchall())
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body="lol")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def on_signup(ch, method, props, body):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users RETURNING id")
            id = cur.fetchone()
            ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body=id.to_bytes(id.bit_length(),'little'))
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=5)
channel.basic_consume(queue='rpc_queue', on_message_callback=on_order)
channel.basic_consume(queue='signup_queue', on_message_callback=on_signup)

channel.start_consuming()
