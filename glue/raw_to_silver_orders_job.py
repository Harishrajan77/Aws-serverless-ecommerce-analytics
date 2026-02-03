import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql("(select * from source1) UNION " + unionType + " (select * from source2)")
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node ApiJson
ApiJson_node1766849551488 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://cloud-analytics-project/raw/orders_api/"], "recurse": True}, transformation_ctx="ApiJson_node1766849551488")

# Script generated for node Amazon S3
AmazonS3_node1766831223244 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://cloud-analytics-project/raw/orders_csv/"], "recurse": True}, transformation_ctx="AmazonS3_node1766831223244")

# Script generated for node Change Schema
ChangeSchema_node1766849774030 = ApplyMapping.apply(frame=ApiJson_node1766849551488, mappings=[("product_id", "string", "product_id", "string"), ("product_name", "string", "product_name", "string"), ("category", "string", "category", "string"), ("price", "double", "price", "double"), ("review_score", "double", "review_score", "double"), ("review_count", "int", "review_count", "int"), ("sales_month_1", "int", "sales_month_1", "int"), ("sales_month_2", "int", "sales_month_2", "int"), ("sales_month_3", "int", "sales_month_3", "int"), ("sales_month_4", "int", "sales_month_4", "int"), ("sales_month_5", "int", "sales_month_5", "int"), ("sales_month_6", "int", "sales_month_6", "int"), ("sales_month_7", "int", "sales_month_7", "int"), ("sales_month_8", "int", "sales_month_8", "int"), ("sales_month_9", "int", "sales_month_9", "int"), ("sales_month_10", "int", "sales_month_10", "int"), ("sales_month_11", "int", "sales_month_11", "int"), ("sales_month_12", "int", "sales_month_12", "int")], transformation_ctx="ChangeSchema_node1766849774030")

# Script generated for node Change Schema
ChangeSchema_node1766831315610 = ApplyMapping.apply(frame=AmazonS3_node1766831223244, mappings=[("product_id", "string", "product_id", "string"), ("product_name", "string", "product_name", "string"), ("category", "string", "category", "string"), ("price", "string", "price", "double"), ("review_score", "string", "review_score", "double"), ("review_count", "string", "review_count", "int"), ("sales_month_1", "string", "sales_month_1", "int"), ("sales_month_2", "string", "sales_month_2", "int"), ("sales_month_3", "string", "sales_month_3", "int"), ("sales_month_4", "string", "sales_month_4", "int"), ("sales_month_5", "string", "sales_month_5", "int"), ("sales_month_6", "string", "sales_month_6", "int"), ("sales_month_7", "string", "sales_month_7", "int"), ("sales_month_8", "string", "sales_month_8", "int"), ("sales_month_9", "string", "sales_month_9", "int"), ("sales_month_10", "string", "sales_month_10", "int"), ("sales_month_11", "string", "sales_month_11", "int"), ("sales_month_12", "string", "sales_month_12", "int")], transformation_ctx="ChangeSchema_node1766831315610")

# Script generated for node Union
Union_node1766849867143 = sparkUnion(glueContext, unionType = "ALL", mapping = {"source1": ChangeSchema_node1766849774030, "source2": ChangeSchema_node1766831315610}, transformation_ctx = "Union_node1766849867143")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Union_node1766849867143, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766831192765", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766831419264 = glueContext.write_dynamic_frame.from_options(frame=Union_node1766849867143, connection_type="s3", format="glueparquet", connection_options={"path": "s3://cloud-analytics-project/silver/orders/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1766831419264")

job.commit()