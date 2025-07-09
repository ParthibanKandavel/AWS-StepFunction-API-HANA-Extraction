# Healthcare Claims Data Migration - AWS Glue to Redshift/Snowflake

## Project Overview
This project demonstrates an ETL pipeline that extracts healthcare/claims data from CSV files in Amazon S3, transforms it using AWS Glue, and loads it into Redshift or Snowflake. It includes logging, error handling, and basic monitoring logic.

---

## Components
- **AWS S3**: Source storage for raw healthcare claim CSV files.
- **AWS Glue**: Serverless ETL for transformation and loading.
- **AWS Redshift / Snowflake**: Target data warehouse.
- **AWS Lambda (optional)**: Trigger for job orchestration or post-processing.
- **Python Scripts**: Local transformation and schema mapping support.

---

## Project Structure

```
healthcare_claims_etl_project/
├── glue_jobs/
│   └── claims_etl_job.py
├── sample_data/
│   └── sample_claims.csv
├── configs/
│   └── schema_mapping.json
├── utils/
│   └── logger.py
├── scripts/
│   └── validate_data.py
└── README.md
```

---

## Setup Instructions
1. Upload `sample_claims.csv` to your S3 bucket.
2. Update connection configurations in `claims_etl_job.py`.
3. Deploy and run Glue job via AWS Console or boto3 SDK.
4. Monitor logs in CloudWatch.

---

## Sample Use Cases
- Migrating legacy IMS/mainframe data to cloud warehouse
- Standardizing claims data for analytics and reporting
- Preparing clean data for Power BI and ML models

---

## Author
[Parthiban Kandavel](https://www.linkedin.com/in/parthi261728) | [GitHub](https://github.com/ParthibanKandavel)
