# Self-Host-Hadoop-Spark-Ubuntu

## Overview
This documentation outlines the complete setup and configuration of a big data environment on a virtualized Ubuntu system.  
The project demonstrates how to build an end-to-end data pipeline—from infrastructure setup to data transformation and analytics—using **Hadoop**, **PySpark**, and **PostgreSQL**, following a **Medallion Architecture** approach.

---

## 1. Virtual Machine Setup
1. Installed **VMware Workstation Pro** on Windows as the virtualization platform.  
2. Created a new **Ubuntu** virtual machine and configured system resources (CPU, memory, and disk).  
3. Installed essential tools and dependencies:
   - **Python**
   - **VS Code**
   - **PostgreSQL**
4. Verified environment stability and networking within the VM.

---

## 2. Hadoop and PySpark Environment Setup
1. Installed **Java JDK**, ensuring compatibility with Hadoop and PySpark versions.  
2. Downloaded and configured **Hadoop**:
   - Edited key configuration files:  
     - `core-site.xml`  
     - `hdfs-site.xml`  
     - `yarn-site.xml`  
   - Set up the **NameNode** and **DataNode** directories.  
3. Formatted the NameNode and launched the **HDFS** cluster.  
4. Configured **YARN** for resource management.  
5. Installed and integrated **PySpark** with Hadoop, ensuring correct environment variables (`JAVA_HOME`, `HADOOP_HOME`, `SPARK_HOME`) were set.  
6. Validated setup by running sample PySpark jobs on HDFS.

---

## 3. Enabling Remote Access
1. Enabled **SSH** service in Ubuntu to allow secure terminal access to the VM.  
2. Verified SSH connectivity from the host machine.  
3. Configured port forwarding to ensure external access to the VM if required.

---

## 4. Implementing the Medallion Architecture
Data processing followed a structured **Bronze–Silver–Gold** model to ensure quality, scalability, and traceability.

### 4.1 Bronze Layer (Raw Data)
- Ingested raw source data into **HDFS**.  
- Stored data in **Parquet** format for efficient storage and schema evolution.  
- Served as a single source of truth for all ingested data.

### 4.2 Silver Layer (Refined Data)
- Used **PySpark** for data cleaning, validation, and standardization.  
- Handled missing values, data type corrections, and deduplication.  
- Output stored as refined Parquet files in dedicated HDFS directories.

### 4.3 Gold Layer (Business-Ready Data)
- Transformed Silver data into aggregated and analytical datasets.  
- Exported as **CSV files** for simplicity and compatibility.  
- These datasets were used to populate **PostgreSQL Data Marts** for downstream analysis.

---

## 5. PostgreSQL Data Marts
1. Installed and configured **PostgreSQL** within the Ubuntu environment.  
2. Created schemas and tables representing the **Gold Layer** outputs.  
3. Loaded curated CSV datasets into PostgreSQL.  
4. Optimized tables with indexing and basic normalization for efficient querying.  

---

## 6. Analytics and Data Access
1. From the Windows host machine, established a secure connection to PostgreSQL using an **SSH Tunnel**.  
2. Connected via Python using the **psycopg2** library.  
3. Extracted data from the PostgreSQL Data Marts for:
   - Exploratory data analysis  
   - Machine learning and predictive modeling  

---

## 7. Key Achievements
- Built a fully functional Hadoop environment on a virtualized Ubuntu system.  
- Integrated PySpark for distributed data processing.  
- Implemented a scalable Medallion Architecture (Bronze → Silver → Gold).  
- Established seamless data flow from HDFS to PostgreSQL Data Marts.  
- Enabled secure access and analytics through SSH and psycopg2. 

