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

# Script generated for node Customer Curated
CustomerCurated_node1706212655243 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://philip-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1706212655243",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1706202295598 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://philip-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1706202295598",
)

# Script generated for node SQL Customer Curated
SqlQuery3275 = """
SELECT
  step_trainer_landing.sensorreadingtime as sensorreadingtime,
  step_trainer_landing.serialnumber as serialnumber,
  step_trainer_landing.distancefromobject as distancefromobject
from step_trainer_landing join customer_curated
on step_trainer_landing.serialnumber = customer_curated.serialnumber
"""
SQLCustomerCurated_node1706221253520 = sparkSqlQuery(
    glueContext,
    query=SqlQuery3275,
    mapping={
        "step_trainer_landing": StepTrainerLanding_node1706202295598,
        "customer_curated": CustomerCurated_node1706212655243,
    },
    transformation_ctx="SQLCustomerCurated_node1706221253520",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1706203589591 = glueContext.getSink(
    path="s3://philip-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1706203589591",
)
StepTrainerTrusted_node1706203589591.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1706203589591.setFormat("json")
StepTrainerTrusted_node1706203589591.writeFrame(SQLCustomerCurated_node1706221253520)
job.commit()
