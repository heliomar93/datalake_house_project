import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://heliomar-teste-1/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node Filter node
Filternode_node1695507273302 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filternode_node1695507273302",
)

# Script generated for node Trusted bucket
Trustedbucket_node1695507347450 = glueContext.write_dynamic_frame.from_options(
    frame=Filternode_node1695507273302,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://heliomar-teste-1/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Trustedbucket_node1695507347450",
)

job.commit()