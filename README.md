# pattern-detection-pyspark-s3

📌 PySpark Pattern Detection Pipeline – Offline Assignment
This project is a simplified version of a real-time transaction pattern detection system, implemented using Apache Spark (PySpark) on Databricks, with AWS S3 as the streaming source and PostgreSQL for intermediate state management.

The pipeline is designed to simulate a streaming ingestion system that continuously detects specific patterns from incoming transaction data, updates stats, and outputs detection results in batch files to S3.

🎯 Objective
The main objective is to:

Simulate streaming data processing using batch chunk ingestion from S3.

Detect three specific behavioral or demographic patterns from the transaction stream.

Store intermediate statistics in PostgreSQL to maintain state across chunks.

Save detection results in batch-wise CSV files to a specified S3 output folder.

📂 Dataset
The transaction data is provided in chunks (CSV format), each simulating a mini-stream. Due to the limited row count (1000 rows), all pattern thresholds were downscaled to match the data size, while preserving the original logic structure.

✅ Patterns Detected
PatId1 – UPGRADE
Goal: Detect high-frequency but low-weight customers for a merchant.

Customers in the top 10% of transaction count with a merchant.

Whose average weight is in the bottom 10%.

Only considered if the merchant has more than 500 total transactions (scaled from original 50K).

ActionType: UPGRADE

PatId2 – CHILD
Goal: Detect low-value, high-volume customers.

Customers with more than 2 transactions with a merchant (scaled from 80).

And average transaction amount less than ₹800 (scaled from ₹23).

ActionType: CHILD

PatId3 – DEI-NEEDED
Goal: Detect merchants with potential gender imbalance.

More than 10 female customers (scaled from 100).

Where female customer count is less than male.

ActionType: DEI-NEEDED

⚙️ Technologies Used
PySpark for distributed processing

PostgreSQL for maintaining stats across chunks

Databricks for orchestrating the pipeline

AWS S3 as both the source and target for streaming data

Python libraries: boto3, pandas, s3fs (for validation and export)

Components
Mechanism X

Uploads transaction chunks from Google Drive to S3 under a structured prefix (e.g. streaming/input/chunk_0.csv/)

Simulates chunk-based streaming.

Mechanism Y

A PySpark script (run periodically) that:

Detects newly arrived chunks in S3.

Reads and processes each chunk.

Updates stats for each pattern in PostgreSQL.

Applies pattern detection logic.

Writes detections to S3 as CSV.

Output
Output CSVs are written to:
s3://pattern-detection-pyspark/streaming/output/

Each detection batch is saved with a timestamp-based folder structure for traceability.

Final output can be downloaded and reviewed locally or in Excel.

