import os
from datetime import timedelta

import numpy as np
import pandas as pd
import psycopg2
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error


DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "retail_dw")
DB_USER = os.getenv("DB_USER", "retail")
DB_PASSWORD = os.getenv("DB_PASSWORD", "retail")

FORECAST_DAYS = int(os.getenv("FORECAST_DAYS", "7"))


def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def main():
    # 1) Read daily revenue from analytics.fact_sales
    query = """
    SELECT order_date::date AS ds, SUM(total_revenue)::numeric AS revenue
    FROM analytics.fact_sales
    GROUP BY 1
    ORDER BY 1;
    """

    with get_conn() as conn:
        df = pd.read_sql(query, conn)

    if df.empty or df.shape[0] < 3:
        raise ValueError("Not enough data in analytics.fact_sales to train a model (need at least 3 days).")

    df["ds"] = pd.to_datetime(df["ds"])
    df["revenue"] = df["revenue"].astype(float)

    # 2) Create a numeric time feature (days since first date)
    df = df.sort_values("ds").reset_index(drop=True)
    df["day_index"] = (df["ds"] - df["ds"].min()).dt.days

    # 3) Simple train/test split (last 20% as test, at least 1 row)
    split_idx = max(int(len(df) * 0.8), len(df) - 1)
    train = df.iloc[:split_idx].copy()
    test = df.iloc[split_idx:].copy()

    X_train = train[["day_index"]].values
    y_train = train["revenue"].values

    X_test = test[["day_index"]].values
    y_test = test["revenue"].values

    # 4) Train model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # 5) Evaluate on test
    y_pred = model.predict(X_test)
    mae = float(mean_absolute_error(y_test, y_pred))
    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))

    # 6) Forecast next N days
    last_date = df["ds"].max()
    last_index = int(df["day_index"].max())

    future_dates = [last_date + timedelta(days=i) for i in range(1, FORECAST_DAYS + 1)]
    future_idx = np.array([[last_index + i] for i in range(1, FORECAST_DAYS + 1)])
    forecast_vals = model.predict(future_idx)

    forecast_df = pd.DataFrame(
        {"ds": future_dates, "forecast_revenue": forecast_vals.astype(float)}
    )

    # 7) Write outputs to Postgres
    ddl = """
    CREATE SCHEMA IF NOT EXISTS analytics;

    CREATE TABLE IF NOT EXISTS analytics.forecast_metrics (
        run_ts timestamptz DEFAULT now(),
        model_name text NOT NULL,
        mae double precision NOT NULL,
        rmse double precision NOT NULL,
        train_rows int NOT NULL,
        test_rows int NOT NULL
    );

    CREATE TABLE IF NOT EXISTS analytics.revenue_forecast (
        run_ts timestamptz DEFAULT now(),
        ds date NOT NULL,
        model_name text NOT NULL,
        forecast_revenue double precision NOT NULL
    );
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)

            # insert metrics
            cur.execute(
                """
                INSERT INTO analytics.forecast_metrics (model_name, mae, rmse, train_rows, test_rows)
                VALUES (%s, %s, %s, %s, %s);
                """,
                ("linear_regression", mae, rmse, int(len(train)), int(len(test))),
            )

            # insert forecasts
            for _, row in forecast_df.iterrows():
                cur.execute(
                    """
                    INSERT INTO analytics.revenue_forecast (ds, model_name, forecast_revenue)
                    VALUES (%s, %s, %s);
                    """,
                    (row["ds"].date(), "linear_regression", float(row["forecast_revenue"])),
                )

        conn.commit()

    print("✅ Forecast completed")
    print(f"MAE={mae:.4f}, RMSE={rmse:.4f}")
    print("Forecast preview:")
    print(forecast_df.head(FORECAST_DAYS))


if __name__ == "__main__":
    main()