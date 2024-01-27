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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1706218076242 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1706218076242",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1706217939993 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1706217939993",
)

# Script generated for node SQL Query
SqlQuery629 = """
select 
step_trainer_trusted.sensorreadingtime,
step_trainer_trusted.serialnumber,
step_trainer_trusted.distancefromobject,
accelerometer_trusted.timestamp,
accelerometer_trusted.user,
accelerometer_trusted.x,
accelerometer_trusted.y,
accelerometer_trusted.z
from accelerometer_trusted join step_trainer_trusted
on accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime
"""
SQLQuery_node1706220113817 = sparkSqlQuery(
    glueContext,
    query=SqlQuery629,
    mapping={
        "accelerometer_trusted": AccelerometerTrusted_node1706217939993,
        "step_trainer_trusted": StepTrainerTrusted_node1706218076242,
    },
    transformation_ctx="SQLQuery_node1706220113817",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1706218245629 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1706220113817,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://philip-lake-house/accelerometer/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node1706218245629",
)

job.commit()
