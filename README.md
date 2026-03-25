# contoso-metadata-ingestion-poc

# Contoso - Metadata Driven Ingestion Framework (PoC)

## Overview

This Proof of Concept (PoC) demonstrates a metadata-driven ingestion framework built using Databricks and PySpark.

The goal is to show how multiple data sources can be dynamically ingested using a reusable framework, without hardcoding logic for each source.

---

##  Objectives

- Build a reusable ingestion framework in Databricks
- Use a metadata-driven approach
- Ingest multiple data sources dynamically
- Store data in:
  - Raw Zone (original format)
  - Data Hub (Delta format)
- Demonstrate onboarding of new sources without changing code

---

##  Architecture

Source Files
↓
Metadata Table (source_config)
↓
Databricks Ingestion Framework
↓
RAW Zone (original data)
↓
Data Hub (Delta tables)

---

##  Technologies Used

- Databricks (PySpark)
- Unity Catalog
- Delta Lake
- Unity Catalog Volumes (for storage)

---

##  Project Structure


notebooks/
│
├── 01_setup_and_sample_data.py
├── 02_metadata_table.py
├── 03_ingestion_framework.py
├── 04_dynamic_onboarding.py


---

##  Solution Components

### 1. Sample Data Sources

- CSV → Customers
- JSON → Orders
- Parquet → Sales Summary (simulated OLAP extract)

---

### 2. Metadata Table

Central table controlling ingestion:

- source_name
- source_type
- source_path
- raw_path
- target_table
- active

---

### 3. Ingestion Framework

Generic logic that:
- Reads metadata table
- Dynamically processes sources
- Writes:
  - Raw data
  - Delta tables (Data Hub)

---

### 4. Dynamic Onboarding

New sources can be added by:
- inserting a new row in metadata table

No code changes required.

---

##  How to Run

1. Run notebook 1 → setup and sample data
2. Run notebook 2 → create metadata table
3. Run notebook 3 → ingestion framework
4. Run notebook 4 → onboarding new source

---

##  Key Benefits

- Reusable ingestion logic
- Scalable solution
- Faster onboarding of sources
- Reduced code duplication
- Foundation for modern data platform

---

##  Future Improvements

- Incremental loads (CDC)
- JDBC sources
- Data quality checks
- Azure Data Factory orchestration
- Key Vault integration
- OLAP cube direct integration

Note: For simplicity, Unity Catalog Volumes are used in this PoC. In a production environment, these paths would typically point to Azure Data Lake Storage (ADLS).

---

##  Conclusion

This PoC demonstrates how metadata-driven ingestion simplifies data integration and enables scalable and maintainable data pipelines.
