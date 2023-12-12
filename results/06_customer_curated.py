import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1702410313708 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1702410313708",
)

# Script generated for node customer_trusted
customer_trusted_node1702410253824 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1702410253824",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT * FROM cust
WHERE EXISTS (
    SELECT 1 FROM acc
    WHERE acc.user = cust.email
)
"""
SQLQuery_node1702410369845 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "acc": accelerometer_trusted_node1702410313708,
        "cust": customer_trusted_node1702410253824,
    },
    transformation_ctx="SQLQuery_node1702410369845",
)

# Script generated for node Amazon S3
AmazonS3_node1702410514390 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1702410369845,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-alex-udacity/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1702410514390",
)

job.commit()
