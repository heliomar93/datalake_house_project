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

# Script generated for node step_trainer
step_trainer_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="heliomar-first-database",
    table_name="step_trainer",
    transformation_ctx="step_trainer_node1",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1696293248177 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://heliomar-teste-1/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1696293248177",
)

# Script generated for node Join
Join_node1696293319916 = Join.apply(
    frame1=step_trainer_node1,
    frame2=accelerometer_trusted_node1696293248177,
    keys1=["sensorreadingtime"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node1696293319916",
)

# Script generated for node drop_fields
drop_fields_node1696293412370 = DropFields.apply(
    frame=Join_node1696293319916,
    paths=["user"],
    transformation_ctx="drop_fields_node1696293412370",
)

# Script generated for node sate_to_bucket
sate_to_bucket_node1696293441182 = glueContext.write_dynamic_frame.from_options(
    frame=drop_fields_node1696293412370,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://heliomar-teste-1/machine+learning_training/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="sate_to_bucket_node1696293441182",
)

job.commit()