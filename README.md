# PySpark Pattern Detection Pipeline – Offline Assignment

This project simulates a simplified real-time transaction pattern detection system using **Apache Spark (PySpark)** on **Databricks**, with **AWS S3** as the streaming source and **PostgreSQL** for stateful tracking.

The pipeline simulates streaming ingestion that:

- Detects specific patterns from transaction data
- Updates stats in PostgreSQL
- Outputs detection results to AWS S3 as batch CSV files

---

## 🎯 Objective

The primary objectives of the project:

- Simulate streaming data processing using chunk-based ingestion from S3  
- Detect three behavioral/demographic patterns in near real-time  
- Store intermediate statistics in PostgreSQL for state management  
- Output pattern detections as CSV files to an S3 output directory  

---

## 📂 Dataset

- Input data is split into **CSV chunks**, simulating streaming events  
- Each chunk is uploaded to S3 under a path like:  
  `s3://pattern-detection-pyspark/streaming/input/chunk_0.csv/`
- Since the dataset is limited to ~1000 rows, thresholds for patterns have been **scaled down** for meaningful detection, while keeping logic intact  

---

## ✅ Patterns Detected

### `PatId1 – UPGRADE`
**Goal**: Detect top customers with low transaction weight for a merchant

- Customers in **top 10%** by transaction count  
- With **average weight in bottom 10%**  
- Merchant must have **> 500 transactions** (scaled from 50K)  
- **ActionType**: `UPGRADE`  

---

### `PatId2 – CHILD`
**Goal**: Detect low-value, high-frequency customers

- Customers with **> 2 transactions** with a merchant (scaled from 80)  
- **Average transaction value < ₹800** (scaled from ₹23)  
- **ActionType**: `CHILD`  

---

### `PatId3 – DEI-NEEDED`
**Goal**: Detect merchants with gender imbalance

- **> 10 female customers** overall (scaled from 100)  
- **Female count < Male count**  
- **ActionType**: `DEI-NEEDED`  

---

## ⚙️ Technologies Used

- **PySpark** – Distributed data processing  
- **PostgreSQL** – To track stats across chunks (stateful processing)  
- **Databricks** – To run and orchestrate the pipeline  
- **AWS S3** – Source and target for streaming chunks and detections  
- **Python libraries** – `boto3`, `pandas`, `s3fs` (used for exporting and testing)

---

## 🧩 Components

### 🔹 Mechanism X – Chunk Uploader
- Uploads CSV chunks from Google Drive to S3  
- Stores each chunk in a unique subfolder like:  
  `s3://pattern-detection-pyspark/streaming/input/chunk_0.csv/`  
- Simulates a streaming source for offline analysis  

### 🔹 Mechanism Y – Pattern Detection Pipeline
- PySpark script/notebook triggered periodically  
- Detects new folders in the input prefix on S3  
- Reads new chunk data into Spark  
- Updates running stats in PostgreSQL  
- Applies pattern detection logic  
- Writes batch-wise detections to S3:  
  `s3://pattern-detection-pyspark/streaming/output/pattern_detected_<timestamp>.csv/`  

---

## 📤 Output

- Output is saved in timestamped CSV folders in:  
  `s3://pattern-detection-pyspark/streaming/output/`  
- Each folder contains batch-wise results for pattern detections  
- Final output can be downloaded from S3 and viewed in Excel or other tools  

---

## 📁 Folder Structure (S3)

```bash
streaming/
│
├── input/
│   ├── chunk_0.csv/
│   ├── chunk_1.csv/
│   └── ...
│
└── output/
    ├── pattern_detected_<timestamp>_0.csv/
    ├── pattern_detected_<timestamp>_1.csv/
    └── ...
