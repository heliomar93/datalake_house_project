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
AccelerometerLanding_node1695339622100 = glueContext.create_dynamic_frame.from_catalog(
    database="heliomar-first-database",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1695339622100",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://heliomar-teste-1/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1",
)

# Script generated for node Join Customer
JoinCustomer_node1695339543537 = Join.apply(
    frame1=CustomerTrustedZone_node1,
    frame2=AccelerometerLanding_node1695339622100,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomer_node1695339543537",
)

# Script generated for node Drop Fields
DropFields_node1695340291714 = DropFields.apply(
    frame=JoinCustomer_node1695339543537,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "timestamp",
    ],
    transformation_ctx="DropFields_node1695340291714",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1695340192584 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1695340291714,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://heliomar-teste-1/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1695340192584",
)

job.commit()
