import os, shutil, csv, logging
import mysql.connector

INPUT_DIR = '/app/input'
PROCESSED = '/app/processed'

logging.basicConfig(level=logging.INFO)

def get_db():
    return mysql.connector.connect(
        host="mysql", user="root",
        password="root", database="webstore"
    )

def trigger():
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')]
    if not files:
        logging.info("Không có file CSV nào trong input/")
        return
    for filename in files:
        filepath = os.path.join(INPUT_DIR, filename)
        conn = get_db()
        cursor = conn.cursor()
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    sku = row['sku'].strip()
                    qty = int(row['qty'])
                    if qty < 0:
                        raise ValueError(f"Âm: {qty}")
                    cursor.execute(
                        """INSERT INTO Products (sku, stock, warehouse)
                           VALUES (%s,%s,%s)
                           ON DUPLICATE KEY UPDATE stock=%s""",
                        (sku, qty, row.get('warehouse', ''), qty)
                    )
                except Exception as e:
                    logging.warning(f"Skip: {row} — {e}")
        conn.commit()
        conn.close()
        shutil.move(filepath, os.path.join(PROCESSED, filename))
        logging.info(f"Force sync xong: {filename}")

if __name__ == "__main__":
    trigger()