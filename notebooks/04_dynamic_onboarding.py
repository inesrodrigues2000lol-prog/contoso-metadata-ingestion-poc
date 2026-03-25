# =========================================================
# NOTEBOOK 4 - DEMONSTRATE DYNAMIC ONBOARDING OF A NEW SOURCE
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
# 2. CREATE A NEW SAMPLE SOURCE - PRODUCTS (CSV)
# ---------------------------------------------------------

products_data = [
    (101, "Laptop", "Electronics"),
    (102, "Chair", "Furniture"),
    (103, "Phone", "Electronics")
]

products_columns = ["product_id", "product_name", "category"]

products_df = spark.createDataFrame(products_data, products_columns)

products_source_path = f"{source_base_path}/products_csv"

products_df.write.mode("overwrite").option("header", "true").csv(products_source_path)

print(f"Products CSV written to: {products_source_path}")
display(products_df)


# ---------------------------------------------------------
# 3. CREATE NEW METADATA ENTRY
# ---------------------------------------------------------

new_metadata_data = [
    (
        "products_csv",                    # source_name
        "csv",                             # source_type
        products_source_path,              # source_path
        f"{raw_base_path}/products_csv",   # raw_path
        "products",                        # target_table
        1,                                 # active
        "full",                            # load_type
        ",",                               # delimiter
        "true"                             # has_header
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

new_metadata_df = spark.createDataFrame(new_metadata_data, metadata_columns)

print("New metadata entry created:")
display(new_metadata_df)


# ---------------------------------------------------------
# 4. APPEND NEW ENTRY TO METADATA TABLE
# ---------------------------------------------------------

# Optional check to avoid duplicates
existing_count = spark.sql(f"""
SELECT COUNT(*) AS cnt
FROM {metadata_table_name}
WHERE source_name = 'products_csv'
""").collect()[0]["cnt"]

if existing_count == 0:
    new_metadata_df.write.mode("append").format("delta").saveAsTable(metadata_table_name)
    print("New source added to metadata table successfully.")
else:
    print("Source 'products_csv' already exists in metadata table. Skipping insert.")


# ---------------------------------------------------------
# 5. VALIDATE METADATA TABLE AFTER INSERT
# ---------------------------------------------------------

print("Updated metadata table:")
display(spark.table(metadata_table_name))


# ---------------------------------------------------------
# 6. RE-RUN THE GENERIC INGESTION FOR THE NEW SOURCE ONLY
# ---------------------------------------------------------

from pyspark.sql.functions import current_timestamp

new_source_config_df = spark.table(metadata_table_name).filter("source_name = 'products_csv' AND active = 1")
new_configs = [row.asDict() for row in new_source_config_df.collect()]

print(f"Number of new active sources found: {len(new_configs)}")


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


def write_raw(df, config):
    raw_path = config["raw_path"]
    source_type = config["source_type"]

    print(f"Writing raw data to: {raw_path}")

    if source_type == "csv":
        df.write.mode("overwrite").option("header", "true").csv(raw_path)
    elif source_type == "json":
        df.write.mode("overwrite").json(raw_path)
    elif source_type == "parquet":
        df.write.mode("overwrite").parquet(raw_path)
    else:
        raise ValueError(f"Unsupported source_type for raw write: {source_type}")


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


for config in new_configs:
    source_name = config["source_name"]

    print("--------------------------------------------------")
    print(f"Starting processing for source: {source_name}")

    df = read_source(config)
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    write_raw(df, config)
    write_datahub(df, config)

    print(f"Finished processing for source: {source_name}")


# ---------------------------------------------------------
# 7. VALIDATE THE NEW DATA HUB TABLE
# ---------------------------------------------------------

print("Validating new Data Hub table:")
display(spark.table(f"{catalog_name}.{datahub_schema}.products"))




print("All Data Hub tables after onboarding new source:")
spark.sql(f"SHOW TABLES IN {catalog_name}.{datahub_schema}").show(truncate=False)




print("Notebook 4 completed successfully.")
print("New source onboarding was achieved by metadata update only, without changing the main ingestion framework.")
