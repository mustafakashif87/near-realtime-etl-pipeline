import threading
import queue
import csv
import getpass
import time
import datetime
from collections import defaultdict, deque
import mysql.connector
from mysql.connector import Error

HASH_TABLE_SLOTS = 10000
PARTITION_SIZE = 500

TRANSACTION_FILE = 'data/transactional_data.csv'
CUSTOMER_MASTER_FILE = 'data/customer_master_data.csv'
PRODUCT_MASTER_FILE = 'data/product_master_data.csv'
SQL_SCRIPT_PATH = "sql/Create_DW.sql"

stream_buffer = queue.Queue()
hash_table = defaultdict(list)
hash_table_lock = threading.Lock()
fifo_queue = deque()
w_semaphore = threading.Semaphore(HASH_TABLE_SLOTS)

def get_db_connection():
    try:
        host = input("Enter DB Host (e.g., localhost): ")
        database = ""   # ----------------------------------> database fetched from create-dw.sql
        user = input("Enter DB User: ")
        password = getpass.getpass("Enter DB Password: ")

        conn = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            autocommit=False
        )

        if conn.is_connected():
            print("Database connection successful.")
            return conn

    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def run_sql_script(conn, filename):
    cursor = conn.cursor()
    print(f"Running SQL script: {filename}")

    try:
        with open(filename, "r", encoding="utf-8") as f:
            sql = f.read()
    except FileNotFoundError:
        print(f"ERROR: SQL script file '{filename}' not found.")
        return False

    # Remove -- comments
    cleaned_sql = []
    for line in sql.splitlines():
        line = line.strip()
        if line.startswith("--") or line == "":
            continue
        cleaned_sql.append(line)

    cleaned_sql = "\n".join(cleaned_sql)

    # Split by ;
    commands = cleaned_sql.split(";")

    for command in commands:
        cmd = command.strip()
        if cmd == "":
            continue

        try:
            cursor.execute(cmd)
        except Exception as e:
            print(f"Error executing SQL:\n{cmd}\n{e}")
            conn.rollback()
            return False

    conn.commit()
    print("SQL script executed successfully.")
    return True


def populate_master_data(conn):
    print("Populating master data (dimensions)...")
    cursor = conn.cursor()

    try:
        # ---- Customer Dimension ----
        with open(CUSTOMER_MASTER_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                sql = """
                INSERT IGNORE INTO customer_dim 
                (customer_ID, gender, age, occupation, city_category, 
                 stay_in_current_city_years, marital_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                stay_years = int(row['Stay_In_Current_City_Years'].replace('+', ''))

                cursor.execute(sql, (
                    int(row['Customer_ID']),
                    row['Gender'],
                    row['Age'],
                    row['Occupation'],
                    row['City_Category'],
                    stay_years,
                    int(row['Marital_Status'])
                ))

        # ---- Product, Store, Supplier Dimensions ----
        print("Populating product, store, and supplier dimensions...")

        products = {}
        stores = {}
        suppliers = {}

        with open(PRODUCT_MASTER_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                products[row['Product_ID']] = row['Product_Category']
                stores[row['storeID']] = row['storeName']
                suppliers[row['supplierID']] = row['supplierName']

        for pid, pcat in products.items():
            cursor.execute(
                "INSERT IGNORE INTO product_dim (product_ID, product_category) VALUES (%s, %s)",
                (pid, pcat)
            )

        for sid, sname in stores.items():
            cursor.execute(
                "INSERT IGNORE INTO store_dim (storeID, store_name) VALUES (%s, %s)",
                (sid, sname)
            )

        for supid, supname in suppliers.items():
            cursor.execute(
                "INSERT IGNORE INTO supplier_dim (supplierID, supplier_name) VALUES (%s, %s)",
                (supid, supname)
            )

        conn.commit()
        print("Master data population complete.")

    except Exception as e:
        print(f"Master data loading error: {e}")
        conn.rollback()

    finally:
        cursor.close()


def populate_time_dimension(conn, start_year=2015, end_year=2026):
    print(f"Populating time_dim...")
    cursor = conn.cursor()

    start_date = datetime.date(start_year, 1, 1)
    end_date   = datetime.date(end_year, 12, 31)

    insert_sql = """
        INSERT IGNORE INTO time_dim (
            DateID, Full_Date, Day, Is_Weekday, Month, Month_Name,
            Quarter, Year, Season, Half_Year
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    batch = []
    date = start_date

    while date <= end_date:
        date_id = int(date.strftime('%Y%m%d'))
        day = date.day
        weekday = 1 if date.weekday() < 5 else 0
        month = date.month
        month_name = date.strftime('%B')

        if month <= 3:
            q, season = 'Q1', 'Winter'
        elif month <= 6:
            q, season = 'Q2', 'Spring'
        elif month <= 9:
            q, season = 'Q3', 'Summer'
        else:
            q, season = 'Q4', 'Fall'

        half = 'H1' if month <= 6 else 'H2'

        batch.append((date_id, date, day, weekday, month, month_name, q, date.year, season, half))

        if len(batch) >= 1000:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            batch = []

        date += datetime.timedelta(days=1)

    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()

    cursor.close()
    print("Time dimension complete.")


def fetch_time_id(cursor, date_str):
    try:
        cursor.execute("SELECT DateID FROM time_dim WHERE Full_Date = %s", (date_str,))
        result = cursor.fetchone()
        return result[0] if result else None
    except:
        return None

def insert_fact_row(cur, fact):
    sql = """
    INSERT INTO sales_fact 
    (date_ID, customer_ID, product_ID, store_ID, supplier_ID,
     order_id, quantity, unit_price, total_sales_amount)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(sql, (
        fact['date_ID'], fact['customer_ID'], fact['product_ID'],
        fact['store_ID'], fact['supplier_ID'], fact['order_id'],
        fact['quantity'], fact['unit_price'], fact['total_sales_amount']
    ))

def stream_feeder(stop_event):
    print("[Feeder] Started")

    try:
        with open(TRANSACTION_FILE, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                if stop_event.is_set():
                    break

                w_semaphore.acquire()
                stream_buffer.put(row)

    except Exception as e:
        print("ERROR IN INPUT STREAM:", e)
        stop_event.set()

    finally:
        stream_buffer.put(None)
        print("Finished Input Stream.")


def hybrid_joiner(conn, stop_event):
    print("STARTING HYBRID JOIN...")

    # Load product master
    product_master = {}
    with open(PRODUCT_MASTER_FILE, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            product_master[row['Product_ID']] = row

    join_cursor = conn.cursor()
    relation_cursor = conn.cursor(dictionary=True, buffered=True)
    relation_cursor.execute("SELECT * FROM customer_dim")

    total_facts = 0
    stream_done = False

    try:
        while not stop_event.is_set():

            # Step 1 — load from stream to hash table
            while not stream_buffer.empty():
                s = stream_buffer.get()
                if s is None:
                    stream_done = True
                    w_semaphore.release()
                    break

                key = int(s['Customer_ID'])

                with hash_table_lock:
                    hash_table[key].append(s)
                    fifo_queue.append((key, s))

                stream_buffer.task_done()

            # Step 2 — fetch chunk of customer_dim
            disk_buffer = relation_cursor.fetchmany(PARTITION_SIZE)
            if not disk_buffer:
                if stream_done and not hash_table:
                    break
                relation_cursor.execute("SELECT * FROM customer_dim")
                disk_buffer = relation_cursor.fetchmany(PARTITION_SIZE)

            # Step 3 — probe + join
            batch = 0
            for r in disk_buffer:
                key = r['customer_ID']

                with hash_table_lock:
                    matches = hash_table.pop(key, [])

                for s in matches:
                    prod = product_master.get(s['Product_ID'])
                    if not prod:
                        w_semaphore.release()
                        continue

                    date_id = fetch_time_id(join_cursor, s['date']) 

                    qty = int(s['quantity']) # ------------------------------> error in  lookup fix karna
                    price = float(prod['price$'])

                    fact = {
                        'date_ID': date_id,
                        'customer_ID': key,
                        'product_ID': s['Product_ID'],
                        'store_ID': int(prod['storeID']),
                        'supplier_ID': int(prod['supplierID']),
                        'order_id': int(s['orderID']),
                        'quantity': qty,
                        'unit_price': price,
                        'total_sales_amount': qty * price
                    }

                    insert_fact_row(join_cursor, fact)
                    batch += 1

                    with hash_table_lock:
                        try:
                            fifo_queue.remove((key, s))
                        except:
                            pass

                    w_semaphore.release()

            if batch:
                conn.commit()
                total_facts += batch

            if stream_done and not hash_table:
                break

    except Exception as e:
        print("ERROR IN HYBRID-JOIN:", e)
        conn.rollback()

    finally:
        join_cursor.close()
        relation_cursor.close()
        stop_event.set()
        print("Finished. Total facts loaded:", total_facts)


def main():
    conn = get_db_connection()
    if not conn:
        print("Failed DB connection.")
        return

    # Run SQL script BEFORE ETL
    ok = run_sql_script(conn, SQL_SCRIPT_PATH)
    if not ok:
        conn.close()
        return

    # Now load dimensions
    populate_master_data(conn)
    populate_time_dimension(conn, 2015, 2026)

    print("\n===== Starting Near-Real-Time ETL =====\n")

    stop_event = threading.Event()
    feeder = threading.Thread(target=stream_feeder, args=(stop_event,))
    joiner = threading.Thread(target=hybrid_joiner, args=(conn, stop_event,))

    feeder.start()
    joiner.start()

    joiner.join()
    feeder.join()

    conn.close()
    print("--- ETL Process Complete ---")

if __name__ == "__main__":
    main()
