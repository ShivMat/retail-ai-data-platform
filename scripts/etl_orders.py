import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")  # works inside Airflow container
CSV_PATH = "/opt/airflow/data/raw/orders.csv"  # Linux path inside container

def connect():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

def main():
    # 1) Extract: read CSV
    df = pd.read_csv(CSV_PATH)
    print("Rows in CSV:", len(df))

    conn = connect()
    try:
        # 2) Load to RAW (append, ignore duplicates)
        raw_df = df.copy()
        for c in raw_df.columns:
            raw_df[c] = raw_df[c].astype(str)

        raw_rows = raw_df[
            ["order_id","order_date","product_id","product_name","quantity","unit_price","customer_id"]
        ].values.tolist()

        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO raw.raw_orders
                (order_id, order_date, product_id, product_name, quantity, unit_price, customer_id)
                VALUES %s
                ON CONFLICT (order_id) DO NOTHING
                """,
                raw_rows
            )
        conn.commit()
        print("Loaded raw.raw_orders (incremental)")

        # 3) Transform (same cleaning)
        stg = df.drop_duplicates(subset=["order_id"]).copy()
        stg["order_id"] = pd.to_numeric(stg["order_id"], errors="raise").astype("int64")
        stg["order_date"] = pd.to_datetime(stg["order_date"], errors="raise").dt.date
        stg["quantity"] = pd.to_numeric(stg["quantity"], errors="raise").astype("int64")
        stg["unit_price"] = pd.to_numeric(stg["unit_price"], errors="raise").astype("float64")
        stg = stg[(stg["quantity"] > 0) & (stg["unit_price"] >= 0)]
        stg["revenue"] = (stg["quantity"] * stg["unit_price"]).round(2)

        stg_rows = stg[
            ["order_id","order_date","product_id","product_name","quantity","unit_price","customer_id","revenue"]
        ].values.tolist()

        # 4) Upsert into STAGING (insert new, update existing order_id)
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO staging.stg_orders
                (order_id, order_date, product_id, product_name, quantity, unit_price, customer_id, revenue)
                VALUES %s
                ON CONFLICT (order_id) DO UPDATE SET
                  order_date = EXCLUDED.order_date,
                  product_id = EXCLUDED.product_id,
                  product_name = EXCLUDED.product_name,
                  quantity = EXCLUDED.quantity,
                  unit_price = EXCLUDED.unit_price,
                  customer_id = EXCLUDED.customer_id,
                  revenue = EXCLUDED.revenue
                """,
                stg_rows
            )
        conn.commit()
        print("Loaded staging.stg_orders (upsert)")

        # 5) Analytics rebuild (safe + simple for now)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE analytics.fact_sales;")
            cur.execute(
                """
                INSERT INTO analytics.fact_sales (order_date, product_id, product_name, total_qty, total_revenue)
                SELECT
                  order_date,
                  product_id,
                  product_name,
                  SUM(quantity) AS total_qty,
                  ROUND(SUM(revenue)::numeric, 2) AS total_revenue
                FROM staging.stg_orders
                GROUP BY 1,2,3
                ORDER BY 1,2;
                """
            )
        conn.commit()
        print("Built analytics.fact_sales")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
