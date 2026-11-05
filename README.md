# YouTube Trending Data Pipeline (AWS)

An end-to-end, serverless data pipeline on AWS that collects YouTube trending video data for 10 regions, processes it through a **Bronze → Silver → Gold** medallion architecture, validates it with an automated data quality gate, and exposes analytics-ready tables through Amazon Athena.

> **Credit:** This project is based on the YouTube data pipeline tutorial by [Darshil Parmar](https://github.com/darshilparmar). I deployed it in my own AWS account and extended it (see [My Additions](#my-additions)).

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [How the Pipeline Works](#how-the-pipeline-works)
- [Gold Layer Tables](#gold-layer-tables)
- [Setup and Deployment](#setup-and-deployment)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [My Additions](#my-additions)
- [Key Learnings](#key-learnings)

---

## Overview

The pipeline pulls the top trending videos per region from the **YouTube Data API v3**, stores the raw responses in S3, cleans and standardizes them with AWS Glue (PySpark), checks the results against data quality rules, and then builds three sets of analytics tables:

| Table | What it answers |
| --- | --- |
| `trending_analytics` | How many videos trend each day per region, and how do views and engagement compare? |
| `channel_analytics` | Which channels trend most often, and how do they rank within each region? |
| `category_analytics` | Which categories drive the most views, and what share of total views do they hold? |

Historical Kaggle trending data can also be loaded into the Bronze layer for backfill and testing.

---

## Architecture

```text
Data Sources        Bronze            Silver            Quality Gate        Gold              Consumption
┌───────────┐    ┌───────────┐    ┌───────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐
│ YouTube   │───>│ Raw JSON  │───>│ Cleansed  │───>│ DQ Lambda    │───>│ trending_    │───>│          │
│ API v3    │    │ (S3)      │    │ Parquet   │    │ row count    │    │ analytics    │    │  Athena  │
├───────────┤    │           │    │ (S3)      │    │ null %       │    │ channel_     │    │          │
│ Kaggle    │───>│ Raw CSV   │    │           │    │ schema       │    │ analytics    │    └──────────┘
│ (history) │    │ (S3)      │    │ Reference │    │ value ranges │    │ category_    │
└───────────┘    └───────────┘    │ Parquet   │    │ freshness    │    │ analytics    │
                                  └───────────┘    └──────┬───────┘    └──────────────┘
                                                      fail │
                                                           ▼
                                                     ┌───────────┐
                                                     │ SNS alert │
                                                     └───────────┘
```

AWS Step Functions orchestrates every stage, with retries, parallel branches, and failure notifications. Amazon EventBridge triggers it on a schedule.

---

## Tech Stack

| Area | Technology |
| --- | --- |
| Compute | AWS Lambda, AWS Glue (PySpark) |
| Storage | Amazon S3 (Parquet, Snappy compression) |
| Orchestration | AWS Step Functions |
| Scheduling | Amazon EventBridge |
| Metadata | AWS Glue Data Catalog |
| Query | Amazon Athena |
| Alerting | Amazon SNS |
| Monitoring | Amazon CloudWatch |
| Security | AWS IAM |
| Languages and libraries | Python 3, PySpark, SQL, Pandas, AWS Wrangler, Boto3 |

---

## Project Structure

<!-- TODO: update this tree so it matches the folders actually in your repo -->

```text
youtube-data-pipeline/
├── lambda/                  # Lambda functions
│   ├── youtube_ingestion/   #   Fetches trending videos and categories from the API
│   ├── json_to_parquet/     #   Converts category reference data to Parquet
│   └── data_quality/        #   Data quality gate
├── glue_jobs/               # PySpark jobs
│   ├── bronze_to_silver_statistics.py
│   └── silver_to_gold_analytics.py
├── step_functions/          # State machine definition
├── scripts/                 # Helper scripts (e.g. upload historical data to S3)
├── data/                    # Reference and historical data
└── README.md
```

---

## How the Pipeline Works

### 1. Ingestion (Bronze)

A Lambda function calls the YouTube Data API v3 for each configured region and saves the raw JSON to S3, partitioned by region, date, and hour. A second dataset of category ID-to-name mappings is saved separately as reference data.

### 2. Transformation (Silver)

Two steps run in parallel:

- **Statistics (Glue PySpark job):** enforces a consistent schema across API JSON and Kaggle CSV inputs, casts data types, handles nulls, standardizes region codes, removes duplicates (keeping the latest record per video, region, and date), and derives `like_ratio` and `engagement_rate`. Output is Snappy-compressed Parquet partitioned by region.
- **Reference data (Lambda):** flattens the category mappings into a tabular format, removes duplicates, and writes Parquet partitioned by region.

### 3. Data Quality Gate

Before anything reaches Gold, a Lambda function checks the Silver data:

| Check | Rule |
| --- | --- |
| Row count | At least 10 rows |
| Null percentage | At most 5% on critical columns |
| Schema | All required columns present |
| Value ranges | View counts are within sensible bounds |
| Freshness | Latest data is less than 48 hours old |

If any check fails, the pipeline stops, Gold is not built, and an SNS alert is sent with the failure details.

### 4. Aggregation (Gold)

A second Glue job reads the validated Silver data and writes the three analytics tables. They are stored as partitioned Parquet and registered in the Glue Data Catalog for Athena.

### 5. Orchestration

Step Functions runs the steps in this order: ingest, short wait, Silver transforms (in parallel), data quality gate, Gold aggregation, then a success or failure notification. Each step retries up to 3 times with exponential backoff.

---

## Gold Layer Tables

**`trending_analytics`** (daily metrics per region)
`region`, `trending_date_parsed`, `total_videos`, `total_views`, `total_likes`, `avg_views_per_video`, `avg_like_ratio`, `avg_engagement_rate`, `unique_channels`, `unique_categories`

**`channel_analytics`** (channel performance and ranking)
`channel_title`, `region`, `total_videos`, `total_views`, `avg_engagement_rate`, `times_trending`, `rank_in_region`, `categories`

**`category_analytics`** (category breakdown)
`category`, `region`, `trending_date_parsed`, `video_count`, `total_views`, `avg_engagement_rate`, `view_share_pct`

Example Athena query:

```sql
SELECT channel_title, total_views, times_trending
FROM yt_pipeline_gold_dev.channel_analytics
WHERE region = 'US'
ORDER BY total_views DESC
LIMIT 10;
```

---

## Setup and Deployment

### Prerequisites

- An AWS account with permissions for Lambda, Glue, S3, Step Functions, SNS, IAM, Athena, EventBridge, and CloudWatch
- A YouTube Data API v3 key from the [Google Cloud Console](https://console.cloud.google.com/apis/credentials)
- AWS CLI configured with credentials
- Python 3.9+

### 1. Create the AWS resources

```bash
# S3 buckets for each layer and for Glue scripts
aws s3 mb s3://yt-data-pipeline-bronze-<region>-<env>
aws s3 mb s3://yt-data-pipeline-silver-<region>-<env>
aws s3 mb s3://yt-data-pipeline-gold-<region>-<env>
aws s3 mb s3://yt-data-pipeline-script-<region>-<env>

# Glue databases
aws glue create-database --database-input '{"Name": "yt_pipeline_bronze_<env>"}'
aws glue create-database --database-input '{"Name": "yt_pipeline_silver_<env>"}'
aws glue create-database --database-input '{"Name": "yt_pipeline_gold_<env>"}'

# SNS topic for alerts
aws sns create-topic --name yt-data-pipeline-alerts-<env>
aws sns subscribe --topic-arn <topic-arn> --protocol email --notification-endpoint <your-email>
```

### 2. Configure environment variables

**Ingestion Lambda**

| Variable | Description |
| --- | --- |
| `YOUTUBE_API_KEY` | YouTube Data API v3 key |
| `S3_BUCKET_BRONZE` | Bronze bucket name |
| `YOUTUBE_REGIONS` | Comma-separated region codes, e.g. `US,GB,CA,DE,FR,IN,JP,KR,MX,RU` |

**Data quality Lambda**

| Variable | Description | Default |
| --- | --- | --- |
| `S3_BUCKET_SILVER` | Silver bucket name | none |
| `GLUE_DB_SILVER` | Silver Glue database | `yt_pipeline_silver_dev` |
| `SNS_ALERT_TOPIC_ARN` | SNS topic for alerts | none |
| `DQ_MIN_ROW_COUNT` | Minimum row count | `10` |
| `DQ_MAX_NULL_PERCENT` | Maximum null percentage | `5.0` |

> Never commit API keys. Store them as Lambda environment variables or in AWS Secrets Manager.

### 3. Deploy

```bash
# Upload Glue scripts
aws s3 cp glue_jobs/ s3://yt-data-pipeline-script-<region>-<env>/glue_jobs/ --recursive

# Package and create a Lambda function (repeat for each Lambda)
cd lambda/youtube_ingestion
zip -r function.zip lambda_function.py
aws lambda create-function \
  --function-name yt-data-pipeline-youtube-ingestion-<env> \
  --runtime python3.9 \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://function.zip \
  --role <lambda-execution-role-arn> \
  --timeout 300 --memory-size 256

# Create a Glue job (repeat for the Gold job)
aws glue create-job \
  --name yt-data-pipeline-bronze-to-silver-<env> \
  --role <glue-role-arn> \
  --command '{"Name":"glueetl","ScriptLocation":"s3://yt-data-pipeline-script-<region>-<env>/glue_jobs/bronze_to_silver_statistics.py"}' \
  --glue-version "4.0" --number-of-workers 2 --worker-type G.1X

# Create the state machine
aws stepfunctions create-state-machine \
  --name yt-data-pipeline \
  --definition file://step_functions/pipeline_orchestration.json \
  --role-arn <step-functions-role-arn>
```

Optional: load historical Kaggle data into Bronze with `scripts/aws_copy.sh`.

---

## Running the Pipeline

**On a schedule (EventBridge):**

```bash
aws events put-rule --name yt-pipeline-schedule --schedule-expression "rate(6 hours)"
aws events put-targets --rule yt-pipeline-schedule \
  --targets '[{"Id":"1","Arn":"<state-machine-arn>","RoleArn":"<eventbridge-role-arn>"}]'
```

**Manually:**

```bash
aws stepfunctions start-execution --state-machine-arn <state-machine-arn>
```

---

## Monitoring and Alerting

- **Step Functions console:** visual execution history and per-step status
- **CloudWatch Logs:** logs from Lambda functions and Glue jobs
- **SNS:** email alerts when the pipeline succeeds or fails
- **Athena:** query the Gold tables directly to spot-check results

---

## Data Sources

- **YouTube Data API v3:** live trending video data (primary source)
- **Kaggle YouTube Trending Dataset:** historical data for backfill and testing
