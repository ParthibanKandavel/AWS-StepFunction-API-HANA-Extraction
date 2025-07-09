import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from S3
datasource = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://your-bucket/sample_claims.csv"], "recurse": True},
    transformation_ctx="datasource"
)

# Data transformations
transformed_df = datasource.toDF()
transformed_df = transformed_df.withColumnRenamed("ClaimID", "claim_id") \
                               .withColumnRenamed("PatientName", "patient_name")

# Convert back to DynamicFrame
final_dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "final_dynamic_frame")

# Write to Redshift (or Snowflake via JDBC)
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=final_dynamic_frame,
    catalog_connection="your-redshift-conn",
    connection_options={"dbtable": "claims_staging", "database": "healthcare"},
    redshift_tmp_dir="s3://your-temp-dir/temp/",
    transformation_ctx="datasink"
)

job.commit()
