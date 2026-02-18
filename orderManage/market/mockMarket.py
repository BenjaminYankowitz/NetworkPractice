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
                    id BIGSERIAL PRIMARY KEY,
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
channel.queue_bind(exchange=exchangeName(),
                    queue='order_queue',
                    routing_key='order')
channel.queue_declare(queue='signup_queue',auto_delete=False,durable=True)
channel.queue_bind(exchange=exchangeName(),
                    queue='signup_queue',
                    routing_key='signup')

def on_order(ch, method, props, body):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            id = props.reply_to
            message = marketMessages_pb2.OrderMSG()
            message.ParseFromString(body)
            symbol = message.symbol
            buySide = message.buySide
            price = message.price
            quantity = message.quantity
            if(buySide):
                cur.execute("""
                        SELECT id,owner,price,quantity,filled FROM activeOrders
                        WHERE symbol=%s
                        AND buyside=False
                        AND price <= %s
                        ORDER BY price ASC;""",
                        (symbol,price))
            else:
                cur.execute("""
                            SELECT id,owner,price,quantity,filled FROM activeOrders
                            WHERE symbol=%s
                            AND buyside=TRUE
                            AND price >= %s
                            ORDER BY price DESC;""",
                            (symbol,price))
            res = cur.fetchall()
            toFill = quantity
            totalCost = 0
            orderFillMSG = marketMessages_pb2.OrderFillMSG()
            print(res)
            for oSide in res:
                oId = oSide['id']
                oOwner = oSide['owner']
                oPrice = oSide['price']
                oQuantity = oSide['quantity']
                oFilled = oSide['filled']
                leftInOther = oQuantity-oFilled
                numFilled = min(leftInOther,toFill)
                toFill-=numFilled
                totalCost+=numFilled*oPrice
                if(leftInOther==numFilled):
                    cur.execute("DELETE FROM activeOrders WHERE id = %s", (oId,))
                else: 
                    cur.execute("UPDATE activeOrders SET filled = %s WHERE id = %s", (oFilled+numFilled,oId))    
                orderFillMSG.orderID = oId
                orderFillMSG.filled = numFilled
                ch.basic_publish(exchange=exchangeName(),
                            routing_key=str(oOwner)+".orderFill",
                            body=orderFillMSG.SerializeToString()) #this will still send if transaction fails.
                if(toFill==0):
                    break
                
            response = marketMessages_pb2.OrderResponseMSG()
            response.amountFilled = quantity-toFill
            if(toFill>0):
                cur.execute("""
                            INSERT INTO activeOrders(symbol,owner,buyside,price,quantity,filled)
                            VALUES(%s,%s,%s,%s,%s,%s) RETURNING id""",
                            (symbol,id,buySide,price,quantity,response.amountFilled))
                response.orderID = int(cur.fetchone()['id'])
            response.successful = True
            response.price = totalCost
            print(props.reply_to+".orderResponse")
            ch.basic_publish(exchange=exchangeName(),
                            routing_key=props.reply_to+".orderResponse",
                            properties=pika.BasicProperties(correlation_id = props.correlation_id),
                            body=response.SerializeToString())
            ch.basic_ack(delivery_tag=method.delivery_tag)

def on_signup(ch, method, props, body):
    if(props.reply_to == None):
        print("signup with no return address")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    message = marketMessages_pb2.SignupMSG()
    message.ParseFromString(body)
    name = message.name
    response = marketMessages_pb2.SignupResponseMSG()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("INSERT INTO users (name) VALUES (%s) RETURNING id",(name,))
                response.assignedId = int(cur.fetchone()["id"])
                print("id:",response.assignedId,"name:",name)
            except psycopg.errors.UniqueViolation:
                response.assignedId = 0
                print("name:", name, " already in use")
    print("replyto:",props.reply_to)
    ch.basic_publish(exchange=exchangeName(),
                     routing_key=props.reply_to,
                     body=response.SerializeToString())
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=5)
channel.basic_consume(queue='order_queue', on_message_callback=on_order)
channel.basic_consume(queue='signup_queue', on_message_callback=on_signup)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("Good bye")
pool.close()