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

# Script generated for node customer_curated
customer_curated_node1702411936562 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated_2",
    transformation_ctx="customer_curated_node1702411936562",
)

# Script generated for node step_trainer
step_trainer_node1702411969566 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer",
    transformation_ctx="step_trainer_node1702411969566",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT * FROM step
WHERE EXISTS (
    SELECT 1 FROM cust
    WHERE cust.serialnumber = step.serialnumber
)
"""
SQLQuery_node1702411997543 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step": step_trainer_node1702411969566,
        "cust": customer_curated_node1702411936562,
    },
    transformation_ctx="SQLQuery_node1702411997543",
)

# Script generated for node Amazon S3
AmazonS3_node1702412184700 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1702411997543,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-alex-udacity/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1702412184700",
)

job.commit()
