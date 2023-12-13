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
accelerometer_trusted_node1702412620580 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1702412620580",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1702412624321 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1702412624321",
)

# Script generated for node SQL Query
SqlQuery0 = """
select step.*, acc.x, acc.y, acc.z from step
inner join acc
on acc.timestamp = step.sensorReadingTime
"""
SQLQuery_node1702412755658 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "acc": accelerometer_trusted_node1702412620580,
        "step": step_trainer_trusted_node1702412624321,
    },
    transformation_ctx="SQLQuery_node1702412755658",
)

# Script generated for node Amazon S3
AmazonS3_node1702413079038 = glueContext.getSink(
    path="s3://stedi-lake-house-alex-udacity/ml/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1702413079038",
)
AmazonS3_node1702413079038.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="ml_curated"
)
AmazonS3_node1702413079038.setFormat("json")
AmazonS3_node1702413079038.writeFrame(SQLQuery_node1702412755658)
job.commit()
