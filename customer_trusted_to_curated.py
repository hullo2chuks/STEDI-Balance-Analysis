import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1706186097597 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1706186097597",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1706185976397 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1706185976397",
)

# Script generated for node Join Customer
JoinCustomer_node1706186022402 = Join.apply(
    frame1=AccelerometerTrusted_node1706185976397,
    frame2=CustomerTrusted_node1706186097597,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1706186022402",
)

# Script generated for node Drop Fields
DropFields_node1706187686043 = DropFields.apply(
    frame=JoinCustomer_node1706186022402,
    paths=["z", "y", "x", "timestamp", "user"],
    transformation_ctx="DropFields_node1706187686043",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1706196149708 = DynamicFrame.fromDF(
    DropFields_node1706187686043.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1706196149708",
)

# Script generated for node Customer Curated
CustomerCurated_node1706186566044 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1706196149708,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://philip-lake-house/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1706186566044",
)

job.commit()
