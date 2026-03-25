# =========================================================
# NOTEBOOK 1 - SETUP + SAMPLE SOURCE DATA
# =========================================================

# ---------------------------------------------------------
# 1. DEFINE NAMES AND PATHS
# ---------------------------------------------------------

catalog_name = "contoso_poc"
metadata_schema = "metadata"
datahub_schema = "datahub"
landing_schema = "landing"
volume_name = "contoso_files"

source_base_path = f"/Volumes/{catalog_name}/{landing_schema}/{volume_name}/source"
raw_base_path = f"/Volumes/{catalog_name}/{landing_schema}/{volume_name}/raw"

print("Configuration defined successfully.")
print(f"Catalog: {catalog_name}")
print(f"Metadata schema: {metadata_schema}")
print(f"DataHub schema: {datahub_schema}")
print(f"Landing schema: {landing_schema}")
print(f"Volume: {volume_name}")
print(f"Source base path: {source_base_path}")
print(f"Raw base path: {raw_base_path}")


# ---------------------------------------------------------
# 2. CREATE CATALOG, SCHEMAS AND VOLUME
# ---------------------------------------------------------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{metadata_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{datahub_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{landing_schema}")

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {catalog_name}.{landing_schema}.{volume_name}
""")

print("Catalog, schemas and volume created successfully.")


# ---------------------------------------------------------
# 3. CREATE SAMPLE SOURCE 1 - CUSTOMERS (CSV)
# ---------------------------------------------------------

customers_data = [
    (1, "Alice", "London", "Retail"),
    (2, "Bob", "Paris", "Finance"),
    (3, "Charlie", "Madrid", "Healthcare")
]

customers_columns = ["customer_id", "customer_name", "city", "segment"]

customers_df = spark.createDataFrame(customers_data, customers_columns)

customers_source_path = f"{source_base_path}/customers_csv"

customers_df.write.mode("overwrite").option("header", "true").csv(customers_source_path)

print(f"Customers CSV written to: {customers_source_path}")
display(customers_df)


# ---------------------------------------------------------
# 4. CREATE SAMPLE SOURCE 2 - ORDERS (JSON)
# ---------------------------------------------------------

orders_data = [
    (1001, 1, "2026-03-01", 250.50),
    (1002, 2, "2026-03-02", 180.00),
    (1003, 1, "2026-03-05", 99.99)
]

orders_columns = ["order_id", "customer_id", "order_date", "amount"]

orders_df = spark.createDataFrame(orders_data, orders_columns)

orders_source_path = f"{source_base_path}/orders_json"

orders_df.write.mode("overwrite").json(orders_source_path)

print(f"Orders JSON written to: {orders_source_path}")
display(orders_df)


# ---------------------------------------------------------
# 5. CREATE SAMPLE SOURCE 3 - SALES SUMMARY (PARQUET)
#    THIS WILL REPRESENT A SIMULATED OLAP EXTRACT
# ---------------------------------------------------------

sales_summary_data = [
    ("2026-01", "Retail", "UK", 12000.0),
    ("2026-01", "Finance", "FR", 8500.0),
    ("2026-02", "Retail", "UK", 13300.0),
    ("2026-02", "Healthcare", "ES", 9100.0)
]

sales_summary_columns = ["period", "business_unit", "country", "revenue"]

sales_summary_df = spark.createDataFrame(sales_summary_data, sales_summary_columns)

sales_summary_source_path = f"{source_base_path}/sales_olap_extract"

sales_summary_df.write.mode("overwrite").parquet(sales_summary_source_path)

print(f"Sales summary Parquet written to: {sales_summary_source_path}")
display(sales_summary_df)




print("Reading back source files for validation...")

customers_check_df = spark.read.option("header", "true").csv(customers_source_path)
orders_check_df = spark.read.json(orders_source_path)
sales_check_df = spark.read.parquet(sales_summary_source_path)

print("Customers source:")
display(customers_check_df)

print("Orders source:")
display(orders_check_df)

print("Sales summary source:")
display(sales_check_df)



print("Notebook 1 completed successfully.")
print("Sample source data is ready for the metadata-driven ingestion framework.")
