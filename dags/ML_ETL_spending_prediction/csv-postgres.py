from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder\
    .config("spark.jars", "/Users/workspace/Desktop/DE/.venv/bin/postgresql-42.7.4 20.13.35.jar")\
    .appName("csv-postgres")\
    .master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

class Postgres:
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/postgres"
    user = "workspace"
    password = "7352"

class Ecommerce_customers:
    structType = StructType([
        StructField('email', StringType()),
        StructField('address', StringType()),
        StructField('avatar', StringType()),
        StructField('avg_session_length', FloatType()),
        StructField('time_on_app', FloatType()),
        StructField('time_on_website', FloatType()),
        StructField('length_of_membership', FloatType()),
        StructField('yearly_amount_spent', FloatType())
    ])
    path = './dags/data/ecommerce_customers.csv'
    name = 'ecommerce_customers'

ecommerce_customers = spark.read\
                        .options(delimiter=';')\
                        .options(header=True)\
                        .schema(Ecommerce_customers.structType)\
                        .csv(Ecommerce_customers.path)


ecommerce_customers.write\
        .format("jdbc")\
        .option("driver", Postgres.driver)\
        .option("url", Postgres.url)\
        .option("user", Postgres.user)\
        .option("password", Postgres.password)\
        .option("dbtable", Ecommerce_customers.name)\
        .mode("overwrite")\
        .save()
