import duckdb
from datetime import datetime

DB_PATH = "/opt/airflow/data/warehouse.duckdb"

con = duckdb.connect(DB_PATH)

con.execute("""
CREATE TABLE IF NOT EXISTS orders (
  order_id BIGINT,
  product_id BIGINT,
  quantity BIGINT,
  price DOUBLE,
  order_date TIMESTAMP
)
""")

con.execute("INSERT INTO orders VALUES (1,101,2,9.99,?)", [datetime.utcnow()])
con.execute("INSERT INTO orders VALUES (2,102,1,19.99,?)", [datetime.utcnow()])
con.execute("INSERT INTO orders VALUES (3,101,1,11.99,?)", [datetime.utcnow()])

print("seed ok")
print(con.execute("SELECT * FROM orders").fetchall())

con.close()