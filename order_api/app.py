from flask import Flask, request, jsonify
import mysql.connector, pika, json, time, logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

def get_db():
    for i in range(12):
        try:
            return mysql.connector.connect(
                host="mysql", user="root",
                password="root", database="webstore"
            )
        except:
            time.sleep(5)

def get_rabbit():
    creds  = pika.PlainCredentials('user', 'password')
    params = pika.ConnectionParameters('rabbitmq', 5672, '/', creds)
    for i in range(12):
        try:
            conn = pika.BlockingConnection(params)
            ch   = conn.channel()
            ch.queue_declare(queue='order_queue', durable=True)
            return conn, ch
        except:
            time.sleep(5)

@app.route('/api/orders', methods=['POST'])
def create_order():
    data = request.get_json()
    if not data or 'customer' not in data or 'amount' not in data:
        return jsonify({"error": "Thiếu trường customer hoặc amount"}), 400

    # 1. Ghi vào MySQL (status = PENDING)
    conn = get_db()
    cur  = conn.cursor()
    cur.execute(
        "INSERT INTO Orders (customer, product_sku, amount, status) VALUES (%s,%s,%s,'PENDING')",
        (data['customer'], data.get('sku', ''), data['amount'])
    )
    conn.commit()
    order_id = cur.lastrowid
    conn.close()

    # 2. Gửi vào RabbitMQ
    mq_conn, ch = get_rabbit()
    message = json.dumps({
        "id": order_id,
        "customer": data['customer'],
        "amount": data['amount']
    })
    ch.basic_publish(
        exchange='',
        routing_key='order_queue',
        body=message,
        properties=pika.BasicProperties(delivery_mode=2)
    )
    mq_conn.close()

    logging.info(f"Order #{order_id} created and queued")
    return jsonify({"status": "PENDING", "order_id": order_id}), 200

@app.route('/api/orders', methods=['GET'])
def get_orders():
    page     = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    offset   = (page - 1) * per_page

    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM Orders ORDER BY created_at DESC LIMIT %s OFFSET %s",
        (per_page, offset)
    )
    orders = cur.fetchall()
    conn.close()
    return jsonify({"page": page, "data": orders})

if __name__ == '__main__':
    time.sleep(15)
    app.run(host='0.0.0.0', port=5000)