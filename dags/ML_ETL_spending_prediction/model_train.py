from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import concat_ws
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder\
    .config("spark.jars", "./postgresql-42.7.4 20.13.35.jar")\
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .appName("csv-postgres")\
    .master("local[*]")\
    .getOrCreate()

class Postgres:
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/postgres"
    user = "workspace"
    password = "7352"

#Загружаем данные из таблицы
data = spark.read\
    .format("jdbc")\
    .option("driver", Postgres.driver)\
    .option("url", Postgres.url)\
    .option("user", Postgres.user)\
    .option("password", Postgres.password)\
    .option("dbtable", 'ecommerce_customers')\
    .load()

data = data.dropna(how='any')

#Объединение всех признаков в вектор
assembler = VectorAssembler(
    inputCols=['avg_session_length', 'time_on_app', 'time_on_website', 'length_of_membership'],
    outputCol="features"
)

data = assembler.transform(data)

final_data = data.select('features', 'yearly_amount_spent')

#Разделение данных на 80% для тренировки и 20% для теста
train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)

lr = LinearRegression(featuresCol="features", labelCol="yearly_amount_spent")

#Обучение модели
lr_model = lr.fit(train_data)

spark.sql("""
            CREATE TABLE IF NOT EXISTS test_data (
                avg_session_length float,
                time_on_app float,
                time_on_website float,
                length_of_membership float,
                yearly_amount_spent float
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
""")
test_data.write.mode("overwrite").insertInto("test_data")

#Сохранение модели
model_path = './model'

lr_model.write().overwrite().save(model_path)
