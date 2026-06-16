import os
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv
import pika

load_dotenv()
DATABASE_URL = os.getenv("EXCHANGE_DATABASE_URL")




def delete_db():
    pool = ConnectionPool(DATABASE_URL, min_size=2, max_size=10, kwargs={"row_factory": dict_row})
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""DROP TABLE IF EXISTS activeOrders""")
            cur.execute("""DROP TABLE IF EXISTS users""")

def exchangeName():
    return "MarketExchange"



def deleteExchange():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_delete(exchange=exchangeName())
    connection.close()

delete_db()
deleteExchange()
