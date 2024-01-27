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

# Script generated for node Customer Trusted
CustomerTrusted_node1706186097597 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1706186097597",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1706185976397 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1706185976397",
)

# Script generated for node Join Customer
JoinCustomer_node1706186022402 = Join.apply(
    frame1=AccelerometerLanding_node1706185976397,
    frame2=CustomerTrusted_node1706186097597,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1706186022402",
)

# Script generated for node Drop Fields
DropFields_node1706187686043 = DropFields.apply(
    frame=JoinCustomer_node1706186022402,
    paths=[
        "serialnumber",
        "sharewithpublicasofdate",
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "lastupdatedate",
        "phone",
    ],
    transformation_ctx="DropFields_node1706187686043",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1706186566044 = glueContext.getSink(
    path="s3://philip-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1706186566044",
)
AccelerometerTrusted_node1706186566044.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1706186566044.setFormat("json")
AccelerometerTrusted_node1706186566044.writeFrame(DropFields_node1706187686043)
job.commit()
