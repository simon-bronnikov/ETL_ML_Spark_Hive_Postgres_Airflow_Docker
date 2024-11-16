from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

spark = SparkSession.builder\
    .config("spark.jars", "./postgresql-42.7.4 20.13.35.jar")\
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .appName("csv-postgres")\
    .master("local[*]")\
    .getOrCreate()

model_path = './model'

model = PipelineModel.load(model_path)

test_data = spark.read.table("test_data")

predictions = model.transform(test_data)

predictions = predictions.select(
    'avg_session_length',
    'time_on_app',
    'time_on_website',
    'length_of_membership',
    'yearly_amount_spent'
)

predictions.write.mode("overwrite").saveAsTable("price_prediction")
