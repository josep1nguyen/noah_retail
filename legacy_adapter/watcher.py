import os, time, shutil, csv, logging
import mysql.connector

INPUT_DIR = '/app/input'
PROCESSED = '/app/processed'
ERROR_DIR = '/app/error'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

def get_db():
    for attempt in range(12):
        try:
            return mysql.connector.connect(
                host="mysql", user="root",
                password="root", database="webstore"
            )
        except Exception as e:
            logging.warning(f"DB chưa sẵn sàng, thử lại {attempt+1}/12... ({e})")
            time.sleep(5)
    raise Exception("Không kết nối được MySQL sau 12 lần thử")

def process_file(filepath):
    filename = os.path.basename(filepath)
    logging.info(f"Phát hiện file: {filename}")
    conn = get_db()
    cursor = conn.cursor()
    ok, skipped = 0, 0

    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                sku = row['sku'].strip()
                qty = int(row['qty'])
                if qty < 0:
                    raise ValueError(f"Số lượng âm: {qty}")
                wh = row.get('warehouse', '').strip()
                cursor.execute(
                    """INSERT INTO Products (sku, stock, warehouse)
                       VALUES (%s, %s, %s)
                       ON DUPLICATE KEY UPDATE stock=%s, warehouse=%s""",
                    (sku, qty, wh, qty, wh)
                )
                ok += 1
            except Exception as e:
                logging.warning(f"[SKIP] dòng lỗi {row}: {e}")
                skipped += 1

    conn.commit()
    conn.close()
    shutil.move(filepath, os.path.join(PROCESSED, filename))
    logging.info(f"Done: {ok} OK, {skipped} dòng bị skip")

def start_watching():
    logging.info("Watchdog started — quét mỗi 10s")
    while True:
        for f in os.listdir(INPUT_DIR):
            if f.endswith('.csv'):
                process_file(os.path.join(INPUT_DIR, f))
        time.sleep(10)

if __name__ == "__main__":
    start_watching()