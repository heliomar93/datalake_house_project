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

# Script generated for node customer_curated
customer_curated_node1695860594664 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://heliomar-teste-1/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1695860594664",
)

# Script generated for node step_trainer_data
step_trainer_data_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="heliomar-first-database",
    table_name="step_trainer",
    transformation_ctx="step_trainer_data_node1",
)

# Script generated for node Join
Join_node1695860642987 = Join.apply(
    frame1=customer_curated_node1695860594664,
    frame2=step_trainer_data_node1,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1695860642987",
)

# Script generated for node drop_fields
drop_fields_node1695860682582 = DropFields.apply(
    frame=Join_node1695860642987,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="drop_fields_node1695860682582",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1695860746997 = glueContext.write_dynamic_frame.from_options(
    frame=drop_fields_node1695860682582,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://heliomar-teste-1/step_trainer/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node1695860746997",
)

job.commit()
