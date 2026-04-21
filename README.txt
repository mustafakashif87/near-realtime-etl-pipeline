------------------------------------------------------------
1. PROJECT DESCRIPTION
------------------------------------------------------------
This project builds a near-real-time Data Warehouse using:
- A star schema in MySQL
- A Python-based ETL pipeline using the HYBRIDJOIN algorithm
- Master data files for customers, products
- A streaming transactional file

The system enriches incoming transactions and loads them into the
sales_fact table, after which analytical OLAP queries may be executed.

------------------------------------------------------------
2. SOFTWARE REQUIREMENTS
------------------------------------------------------------
To run the project, you need:

- Python 3.9 or later
- MySQL Server 8.0 or later
- Required Python libraries:
      mysql-connector-python

Install using:
    pip install mysql-connector-python

------------------------------------------------------------
3. FILES INCLUDED
------------------------------------------------------------
Create-DW.sql
hybrid_join.py
Queries-DW.sql
transactional_data.csv
customer_master_data.csv
product_master_data.csv
README.txt

------------------------------------------------------------
4. STEP-BY-STEP INSTRUCTIONS
------------------------------------------------------------
- add transactional_data.csv, customer_master_data.csv and product_master_data.csv to the same directory
- Run the hybrid_join.py file (this will itself run the Create-DW.sql file)
- It will prompt you to enter your credentials
- Then the Hybrid Join process will begin
- This will populate all nessessary tables 
- Run the Queries_DW.sql file seperately to see the OLAP results