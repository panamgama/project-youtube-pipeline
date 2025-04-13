# 📊 project-youtube-pipeline

**project-youtube-pipeline** is an end-to-end data pipeline that extracts video and channel metrics from the **YouTube Data API**, processes and stores the data using **AWS services**, and visualizes insights on a **Looker dashboard**. This project automates the entire workflow, enabling daily updates and real-time monitoring.

---

## 🚀 Key Features

- Automates daily extraction of YouTube video data  
- Performs scalable data transformation and cataloging  
- Supports querying and exploration with Athena  
- Publishes analytics to Google Sheets for visualization in Looker  
- Sends notifications on pipeline status  
- Fully serverless and cost-effective using AWS  

---

## 🛠️ AWS Architecture

The pipeline is orchestrated using the following AWS services:

1. **Amazon EventBridge** – Triggers the pipeline daily  
2. **AWS Step Functions** – Manages the sequence and logic of the pipeline  
3. **AWS Lambda** – Executes modular Python functions for data extraction, transformation, and export  
4. **Amazon S3** – Stores raw and processed data in a structured format  
5. **AWS Glue** – Transforms and cleans data, and catalogs it in the AWS Data Catalog (using AWS Glue Crawlers) 
6. **Amazon Athena** – Performs SQL queries on the processed data stored in S3
7. **Amazon SNS** – Notifies stakeholders of success/failure  
8. **Amazon CloudWatch** – Monitors logs and metrics for observability and debugging  

---

## 🌐 Google APIs Used

The pipeline integrates with Google Cloud APIs to extract and publish data:

1. **YouTube Data API v3** – Retrieves video, channel, and analytics metadata  
2. **Google Sheets API** – Writes transformed data to a Google Sheet  
3. **Google Drive API** – Shares the Google Sheet for Looker access  

---

## 🧱 Data Flow Overview

- **Raw data ingestion:**  
  Data is extracted daily from the YouTube Data API and saved in **Parquet format** to:  
  `s3://bucket-name/raw/yyyy/mm/dd/`  

- **Transformed data storage:**  
  Data is cleaned and transformed using AWS Glue, and partitioned by `channelId` and date. It is stored in the following S3 paths:
  - Channel-level metrics:  
    `s3://bucket-name/analysis/channel_data/channelid=XXXXXX/extractYear=YYYY/extractMonth=MM/extractDay=DD/`
  - Video-level metrics:  
    `s3://bucket-name/analysis/video_data/channelid=XXXXXX/extractYear=YYYY/extractMonth=MM/extractDay=DD/`

- **Master dataset creation:**  
  A consolidated dataset is built by a Lambda function. This master view is stored in:  
  `s3://bucket-name/lambda_query_results/`

- **Publishing to Google Sheets:**  
  The latest data is exported to a **Google Sheet** hosted in a shared **Google Drive** folder, accessible to Looker.

- **Visualization:**  
  Looker is connected to the Google Sheet and is configured to **automatically refresh every 4 hours**, ensuring the dashboard stays up-to-date with the latest metrics.

---

## 🗺️ Architecture Diagram

![alt text](https://github.com/panamgama/project-youtube-pipeline/blob/main/Flow-1.png "Architecture flow diagram")

---

## 📂 Sample Dataset

📎 [Explore the dataset](https://docs.google.com/spreadsheets/d/1NEH8B7rigmO0bXc8RhGnYhc3kSwuLrGRsU0I-80r9CQ/edit?usp=sharing)

---

## 📈 Looker Dashboard

📎 [View the dashboard](https://lookerstudio.google.com/reporting/ee2852d5-50cf-4588-939f-266a045ac6cf/page/vk9FF)

---

## ✍️ Author

**Pulasthi Panamgama** – Data Geek  
*Automating insight delivery, one pipeline at a time.*
