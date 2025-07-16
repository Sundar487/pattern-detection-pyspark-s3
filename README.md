# pattern-detection-pyspark-s3

📌 PySpark Pattern Detection Pipeline – Offline Assignment
This project is a simplified version of a real-time transaction pattern detection system, implemented using Apache Spark (PySpark) on Databricks, with AWS S3 as the streaming source and PostgreSQL for intermediate state management.

The pipeline simulates streaming ingestion that continuously:

Detects specific patterns from transaction data,

Updates stats in PostgreSQL,

Outputs detection results in batch files to S3.

🎯 Objective
The main goals of the project are to:

Simulate streaming data processing using chunk-based ingestion from S3.

Detect three behavioral or demographic patterns in near real-time.

Store intermediate statistics in PostgreSQL to maintain state across batches.

Save detection results in batch-wise CSVs to an S3 output folder.

📂 Dataset
The transaction data is ingested from S3 in chunks (CSV format).

Each chunk simulates a mini data stream.

Because the dataset contains only 1000 rows, all threshold values for pattern detection were scaled down (while preserving the core logic).

✅ Patterns Detected
PatId1 – UPGRADE
Detect top customers with low weight per merchant:

Customers in the top 10% by transaction count.

With average weight in bottom 10%.

Considered only if merchant has > 500 transactions.

ActionType: UPGRADE

PatId2 – CHILD
Detect low-value, high-frequency customers:

Customers with > 2 transactions with a merchant.

Average transaction value < ₹800.

ActionType: CHILD

PatId3 – DEI-NEEDED
Detect merchants with gender imbalance:

More than 10 female customers overall.

But female count < male count.

ActionType: DEI-NEEDED

⚙️ Technologies Used
PySpark for distributed data processing

PostgreSQL for maintaining intermediate statistics

Databricks for orchestrating the pipeline

AWS S3 as source and sink for streaming data

Python Libraries: boto3, pandas, s3fs (for file validation & export)

🧩 Components
🔸 Mechanism X – Chunk Uploader
Uploads transaction chunks from Google Drive to S3.

Stores them under a structured path like:
s3://pattern-detection-pyspark/streaming/input/chunk_0.csv/

Simulates data stream behavior for offline setup.

🔸 Mechanism Y – Pattern Detection Pipeline
A PySpark notebook/script that is run periodically.

Detects new incoming chunks in S3.

Reads data and updates stats in PostgreSQL.

Applies all three pattern detection rules.

Writes detection results as CSV files to S3, in batch format.

📤 Output
CSV output files are saved under:
s3://pattern-detection-pyspark/streaming/output/

Each batch is stored in a timestamped folder.

The output can be downloaded and viewed locally (e.g. in Excel).

