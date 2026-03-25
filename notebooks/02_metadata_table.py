# =========================================================
# NOTEBOOK 2 - CREATE METADATA TABLE
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

metadata_table_name = f"{catalog_name}.{metadata_schema}.source_config"

print("Configuration loaded successfully.")
print(f"Metadata table: {metadata_table_name}")
print(f"Source base path: {source_base_path}")
print(f"Raw base path: {raw_base_path}")


# ---------------------------------------------------------
# 2. CREATE METADATA DATA
# ---------------------------------------------------------

metadata_data = [
    (
        "customers_csv",                     # source_name
        "csv",                               # source_type
        f"{source_base_path}/customers_csv", # source_path
        f"{raw_base_path}/customers_csv",    # raw_path
        "customers",                         # target_table
        1,                                   # active
        "full",                              # load_type
        ",",                                 # delimiter
        "true"                               # has_header
    ),
    (
        "orders_json",
        "json",
        f"{source_base_path}/orders_json",
        f"{raw_base_path}/orders_json",
        "orders",
        1,
        "full",
        None,
        None
    ),
    (
        "sales_olap_extract",
        "parquet",
        f"{source_base_path}/sales_olap_extract",
        f"{raw_base_path}/sales_olap_extract",
        "sales_summary",
        1,
        "full",
        None,
        None
    )
]

metadata_columns = [
    "source_name",
    "source_type",
    "source_path",
    "raw_path",
    "target_table",
    "active",
    "load_type",
    "delimiter",
    "has_header"
]

metadata_df = spark.createDataFrame(metadata_data, metadata_columns)

print("Metadata dataframe created successfully.")
display(metadata_df)


# ---------------------------------------------------------
# 3. WRITE METADATA TABLE IN DELTA FORMAT
# ---------------------------------------------------------

metadata_df.write.mode("overwrite").format("delta").saveAsTable(metadata_table_name)

print(f"Metadata table created successfully: {metadata_table_name}")


# ---------------------------------------------------------
# 4. VALIDATE METADATA TABLE
# ---------------------------------------------------------

metadata_check_df = spark.table(metadata_table_name)

print("Reading metadata table back for validation...")
display(metadata_check_df)




print("Optional SQL validation:")
spark.sql(f"SELECT * FROM {metadata_table_name}").show(truncate=False)




print("Notebook 2 completed successfully.")
print("The metadata table is ready for the generic ingestion framework.")
