import pika, json, time, logging
import psycopg2, mysql.connector

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

def get_pg():
    for i in range(12):
        try:
            return psycopg2.connect(
                host="postgres", user="user",
                password="password", dbname="finance"
            )
        except:
            time.sleep(5)

def get_mysql():
    for i in range(12):
        try:
            return mysql.connector.connect(
                host="mysql", user="root",
                password="root", database="webstore"
            )
        except:
            time.sleep(5)

def callback(ch, method, properties, body):
    order = json.loads(body)
    logging.info(f"Nhận đơn #{order['id']}")

    try:
        # 1. Insert vào PostgreSQL (Finance)
        pg  = get_pg()
        cur = pg.cursor()
        cur.execute(
            "INSERT INTO transactions (order_id, customer, amount) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
            (order['id'], order['customer'], order['amount'])
        )
        pg.commit()
        pg.close()

        # 2. Update status = SYNCED trong MySQL
        mc  = get_mysql()
        cur = mc.cursor()
        cur.execute(
            "UPDATE Orders SET status='SYNCED' WHERE id=%s",
            (order['id'],)
        )
        mc.commit()
        mc.close()

        logging.info(f"Order #{order['id']} synced OK")

    except Exception as e:
        logging.error(f"Lỗi xử lý order #{order['id']}: {e}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

def start():
    time.sleep(20)
    creds  = pika.PlainCredentials('user', 'password')
    params = pika.ConnectionParameters('rabbitmq', 5672, '/', creds)
    conn   = pika.BlockingConnection(params)
    ch     = conn.channel()
    ch.queue_declare(queue='order_queue', durable=True)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue='order_queue', on_message_callback=callback)
    logging.info("Worker sẵn sàng, đang lắng nghe queue...")
    ch.start_consuming()

if __name__ == "__main__":
    start()