import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1702398397380 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1702398397380",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1702398450784 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1702398450784",
)

# Script generated for node Join
Join_node1702398512999 = Join.apply(
    frame1=AccelerometerLanding_node1702398397380,
    frame2=CustomerTrusted_node1702398450784,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1702398512999",
)

# Script generated for node Drop Fields
DropFields_node1702398737360 = DropFields.apply(
    frame=Join_node1702398512999,
    paths=[
        "serialnumber",
        "birthday",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "sharewithfriendsasofdate",
        "lastupdatedate",
        "phone",
    ],
    transformation_ctx="DropFields_node1702398737360",
)

# Script generated for node Amazon S3
AmazonS3_node1702403377136 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1702398737360,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-alex-udacity/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1702403377136",
)

job.commit()
