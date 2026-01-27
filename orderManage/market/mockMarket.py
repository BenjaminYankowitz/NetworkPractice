import os
from datetime import datetime
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout
from dotenv import load_dotenv
import pika
import marketMessages_pb2

load_dotenv()
DATABASE_URL = os.getenv("EXCHANGE_DATABASE_URL")


pool = ConnectionPool(DATABASE_URL, min_size=2, max_size=10, kwargs={"row_factory": dict_row})


def init_db():
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS activeOrders(
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(5) NOT NULL,
                    owner BIGINT NOT NULL ,
                    buyside BOOL NOT NULL ,
                    price BIGINT NOT NULL ,
                    quantity BIGINT NOT NULL ,
                    filled BIGINT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users(
                    id BIGINT PRIMARY KEY,
                    name VARCHAR(20) UNIQUE
                )
            """)

init_db()

def exchangeName():
    return "MarketExchange"

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
channel.exchange_declare(exchange=exchangeName(), exchange_type='direct',durable=True)
channel.queue_declare(queue='order_queue',auto_delete=False,durable=True)
channel.queue_declare(queue='signup_queue',auto_delete=False,durable=True)
channel.queue_bind(exchange=exchangeName(),
                    queue='signup_queue',
                    routing_key='signup')

def on_order(ch, method, props, body):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            message = marketMessages_pb2.OrderMessage.ParseFromString(body)
            symbol = message.symbol
            side = message.buySide
            sortBy =  "ASC" if side else "DESC"
            cur.execute("""
                        SELECT * FROM activeOrders
                        Where symbol=%s
                        WHERE side=%s
                        ORDER BY price %s;""",
                        (symbol, not side,sortBy))
            print(cur.fetchall())
    ch.basic_publish(exchange=exchangeName(),
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body="lol")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def on_signup(ch, method, props, body):
    id = props.correlation_id
    print(props.reply_to)
    if(props.reply_to == None):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    message = marketMessages_pb2.SignupMSG()
    message.ParseFromString(body)
    name = message.name
    response = marketMessages_pb2.SignupResponseMSG()
    response.result = marketMessages_pb2.SignupResponseEnum.success
    print("id:",id,"name:",name)
    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("INSERT INTO users (id, name) VALUES (%s, %s)",(id, name))
            except psycopg.errors.UniqueViolation: 
                response.result = marketMessages_pb2.SignupResponseEnum.taken
    ch.basic_publish(exchange=exchangeName(),
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=id),
                     body=response.SerializeToString())
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=5)
channel.basic_consume(queue='order_queue', on_message_callback=on_order)
channel.basic_consume(queue='signup_queue', on_message_callback=on_signup)

channel.start_consuming()
