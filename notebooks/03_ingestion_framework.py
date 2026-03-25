# =========================================================
# NOTEBOOK 3 - GENERIC METADATA-DRIVEN INGESTION FRAMEWORK
# =========================================================

from pyspark.sql.functions import current_timestamp

# ---------------------------------------------------------
# 1. DEFINE NAMES AND PATHS
# ---------------------------------------------------------

catalog_name = "contoso_poc"
metadata_schema = "metadata"
datahub_schema = "datahub"
landing_schema = "landing"
volume_name = "contoso_files"

metadata_table_name = f"{catalog_name}.{metadata_schema}.source_config"

print("Configuration loaded successfully.")
print(f"Metadata table: {metadata_table_name}")


# ---------------------------------------------------------
# 2. READ ACTIVE SOURCE CONFIGURATIONS
# ---------------------------------------------------------

config_df = spark.table(metadata_table_name).filter("active = 1")

print("Active source configurations:")
display(config_df)

configs = [row.asDict() for row in config_df.collect()]

print(f"Number of active sources found: {len(configs)}")


# ---------------------------------------------------------
# 3. DEFINE GENERIC SOURCE READER
# ---------------------------------------------------------

def read_source(config):
    source_type = config["source_type"]
    source_path = config["source_path"]
    delimiter = config["delimiter"]
    has_header = config["has_header"]

    print(f"Reading source: {config['source_name']}")
    print(f"Source type: {source_type}")
    print(f"Source path: {source_path}")

    if source_type == "csv":
        df = (
            spark.read
            .option("header", has_header if has_header is not None else "true")
            .option("delimiter", delimiter if delimiter is not None else ",")
            .csv(source_path)
        )
    elif source_type == "json":
        df = spark.read.json(source_path)
    elif source_type == "parquet":
        df = spark.read.parquet(source_path)
    else:
        raise ValueError(f"Unsupported source_type: {source_type}")

    return df


# ---------------------------------------------------------
# 4. DEFINE RAW WRITER
# ---------------------------------------------------------

def write_raw(df, config):
    raw_path = config["raw_path"]
    source_type = config["source_type"]

    print(f"Writing raw data to: {raw_path}")

    if source_type == "csv":
        (
            df.write
            .mode("overwrite")
            .option("header", "true")
            .csv(raw_path)
        )
    elif source_type == "json":
        (
            df.write
            .mode("overwrite")
            .json(raw_path)
        )
    elif source_type == "parquet":
        (
            df.write
            .mode("overwrite")
            .parquet(raw_path)
        )
    else:
        raise ValueError(f"Unsupported source_type for raw write: {source_type}")


# ---------------------------------------------------------
# 5. DEFINE DATA HUB WRITER
# ---------------------------------------------------------

def write_datahub(df, config):
    target_table = config["target_table"]
    full_table_name = f"{catalog_name}.{datahub_schema}.{target_table}"

    print(f"Writing Data Hub Delta table: {full_table_name}")

    (
        df.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(full_table_name)
    )


# ---------------------------------------------------------
# 6. PROCESS EACH ACTIVE SOURCE
# ---------------------------------------------------------

for config in configs:
    source_name = config["source_name"]

    print("--------------------------------------------------")
    print(f"Starting processing for source: {source_name}")

    # Read source dynamically
    df = read_source(config)

    # Add ingestion timestamp for traceability
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    # Write raw layer
    write_raw(df, config)

    # Write Data Hub layer in Delta format
    write_datahub(df, config)

    print(f"Finished processing for source: {source_name}")


# ---------------------------------------------------------
# 7. SHOW DATA HUB TABLES
# ---------------------------------------------------------

print("Data Hub tables created:")
spark.sql(f"SHOW TABLES IN {catalog_name}.{datahub_schema}").show(truncate=False)




print("Customers table:")
display(spark.table(f"{catalog_name}.{datahub_schema}.customers"))

print("Orders table:")
display(spark.table(f"{catalog_name}.{datahub_schema}.orders"))

print("Sales summary table:")
display(spark.table(f"{catalog_name}.{datahub_schema}.sales_summary"))




print("Notebook 3 completed successfully.")
print("All active sources were ingested into the Raw zone and Data Hub zone.")
