import streamlit as st
import mysql.connector, psycopg2, pika, pandas as pd, time, subprocess

st.set_page_config(page_title="NOAH Retail Dashboard", layout="wide")
st.title("NOAH Retail — Integration Dashboard")

def get_mysql():
    return mysql.connector.connect(
        host="mysql", user="root",
        password="root", database="webstore"
    )

def get_pg():
    return psycopg2.connect(
        host="postgres", user="user",
        password="password", dbname="finance"
    )

# ── Nút Trigger Sync ─────────────────────────────────────────────
if st.button("Trigger Sync ngay"):
    subprocess.Popen(["python", "/app/force_sync.py"])
    st.success("Đã kích hoạt sync!")

st.divider()

# ── Tồn kho hiện tại ─────────────────────────────────────────────
st.subheader("Tồn kho hiện tại (MySQL)")
try:
    mc = get_mysql()
    df_stock = pd.read_sql(
        "SELECT sku, stock, warehouse FROM Products ORDER BY sku LIMIT 50",
        mc
    )
    mc.close()
    st.dataframe(df_stock, use_container_width=True)
except Exception as e:
    st.error(f"Lỗi kết nối MySQL: {e}")

st.divider()

# ── Đối soát đơn hàng ────────────────────────────────────────────
st.subheader("Đối soát đơn hàng — Web Store vs Finance")
col1, col2 = st.columns(2)

df_orders = pd.DataFrame()
df_trans  = pd.DataFrame()

with col1:
    st.markdown("**Web Store (MySQL)**")
    try:
        mc = get_mysql()
        df_orders = pd.read_sql(
            "SELECT id, customer, amount, status FROM Orders ORDER BY id DESC LIMIT 10",
            mc
        )
        mc.close()
        st.dataframe(df_orders, use_container_width=True)
    except Exception as e:
        st.error(f"Lỗi: {e}")

with col2:
    st.markdown("**Finance (PostgreSQL)**")
    try:
        pg = get_pg()
        df_trans = pd.read_sql(
            "SELECT order_id, customer, amount, synced_at FROM transactions ORDER BY order_id DESC LIMIT 10",
            pg
        )
        pg.close()
        st.dataframe(df_trans, use_container_width=True)
    except Exception as e:
        st.error(f"Lỗi: {e}")

# ── Thống kê khớp / lệch ─────────────────────────────────────────
if not df_orders.empty and not df_trans.empty:
    merged    = pd.merge(df_orders, df_trans,
                         left_on='id', right_on='order_id', how='left')
    matched   = int(merged['order_id'].notna().sum())
    unmatched = int(merged['order_id'].isna().sum())
    c1, c2 = st.columns(2)
    c1.metric("Khớp (SYNCED)", matched)
    c2.metric("Chưa sync (PENDING)", unmatched)

st.divider()

# ── Monitor RabbitMQ Queue ────────────────────────────────────────
st.subheader("Trạng thái Queue (RabbitMQ)")
try:
    creds  = pika.PlainCredentials('user', 'password')
    params = pika.ConnectionParameters('rabbitmq', 5672, '/', creds)
    conn   = pika.BlockingConnection(params)
    ch     = conn.channel()
    q      = ch.queue_declare(queue='order_queue', durable=True, passive=True)
    msg_count = q.method.message_count
    conn.close()
    st.metric("Tin nhắn đang chờ trong queue", msg_count)
    if msg_count > 100:
        st.warning("Queue đang tồn đọng nhiều!")
except Exception as e:
    st.error(f"Không kết nối được RabbitMQ: {e}")