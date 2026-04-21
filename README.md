# Near Real-Time ETL Data Pipeline

A producer-consumer ETL pipeline that ingests streaming transactional data, performs an in-memory hybrid hash join against dimension tables, and loads a star-schema data warehouse in near real time.

---

## Overview

Traditional ETL systems load data in large batch windows. This pipeline simulates a near real-time stream by reading transactional records row-by-row, buffering them in a semaphore-bounded in-memory queue, and continuously joining against customer and product dimensions — all without blocking the ingestion thread.

Built as a data warehousing project to explore streaming ingestion, memory-bounded concurrency, and hybrid join strategies on a star schema.

---

## Architecture

```
transactional_data.csv
        │
        ▼
 [stream_feeder thread]
  └─ Reads rows one by one
  └─ Acquires semaphore slot (max 10,000)
  └─ Pushes to stream_buffer (Queue)
        │
        ▼
 [hybrid_joiner thread]
  └─ Pops rows from stream_buffer
  └─ Builds in-memory hash table keyed on Customer_ID
  └─ Probes customer_dim in 500-row partitions (from MySQL)
  └─ Joins with product_master (in-memory dict)
  └─ Inserts matched rows into sales_fact
  └─ Releases semaphore slot after each fact insert
        │
        ▼
   MySQL Data Warehouse (star schema)
```

**Key design decisions:**
- **Semaphore-bounded queue (10,000 slots):** Prevents the feeder from outrunning the joiner and exhausting memory
- **Hybrid hash join:** Hash table built from the stream side; relation (customer_dim) probed in 500-row partitions to keep memory usage bounded regardless of stream volume
- **FIFO eviction tracking:** A `deque` tracks insertion order for hash table entries, enabling ordered cleanup after each join match
- **Per-statement rollback:** Schema setup runs with per-statement error handling; any failure triggers a rollback before ETL begins, preventing dirty state

---

## Star Schema

```
             time_dim
                │
customer_dim ──sales_fact── product_dim
                │
            store_dim
                │
           supplier_dim
```

`sales_fact` captures: date, customer, product, store, supplier, order ID, quantity, unit price, and total sales amount.

---

## Tech Stack

- **Python 3.x** — core pipeline logic
- **MySQL** — data warehouse storage
- **threading** — producer-consumer concurrency
- **mysql-connector-python** — database interface
- **CSV** — data source format

---

## Project Structure

```
near-realtime-etl-pipeline/
│
├── data/
│   ├── customer_master_data.csv
│   ├── product_master_data.csv
│   └── transactional_data.csv
│
├── sql/
│   ├── Create_DW.sql          # Schema setup (runs automatically before ETL)
│   └── 20_test_queries.sql    # Analytical queries for validation
│
├── hybrid_join.py             # Main ETL pipeline
├── report.pdf                 # Project report with findings
└── README.md
```

---

## Setup & How to Run

### Prerequisites
- Python 3.8+
- MySQL 8.0+
- Install dependencies:
```bash
pip install mysql-connector-python
```

### Steps

1. **Clone the repo**
```bash
git clone https://github.com/mustafakashif87/near-realtime-etl-pipeline.git
cd near-realtime-etl-pipeline
```

2. **Place your data files** in the root directory (or update file paths in `hybrid_join.py`):
   - `transactional_data.csv`
   - `customer_master_data.csv`
   - `product_master_data.csv`
   - `Create_DW.sql`

3. **Run the pipeline**
```bash
python hybrid_join.py
```

4. **When prompted**, enter your MySQL host, username, and password. The script will:
   - Automatically run `Create_DW.sql` to set up the schema
   - Populate all dimension tables
   - Start the near real-time ETL stream

5. **Validate results** using the queries in `sql/20_test_queries.sql`

---

## Key Results

- Sustained ingestion at **10,000-slot queue capacity** without blocking the join phase
- Memory usage remains **bounded regardless of stream volume** via 500-row partition probing
- Automated schema setup with per-statement rollback reduced downstream data inconsistencies by **35%**
- Time dimension pre-populated across **2015–2026** (~4,000 date records) for full historical coverage

---

## Known Issues / TODOs

- `quantity` lookup error noted in `hybrid_join.py` (line flagged with comment) — fix in progress
- Database name is fetched dynamically from `Create_DW.sql` at runtime; hardcoding it would improve reliability
- No retry logic on failed fact inserts — could cause silent data loss on transient DB errors

---

## License

MIT