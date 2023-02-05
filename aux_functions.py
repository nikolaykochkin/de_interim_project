# Импорт стандартных библиотек
import sys
# Импорт компонентов pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# параметры подключения 
HOST = '158.160.52.27'
POSTGRES_URL = f'jdbc:postgresql://{HOST}:5432/jovyan'
POSTGRES_CONNECTION = {'user':'jovyan', 'password':'jovyan', 'driver': 'org.postgresql.Driver'}

# зaпускаю spark
def spark_init(app_name):
    return SparkSession.builder \
                    .master("local") \
                    .appName(app_name) \
                    .getOrCreate()

# Загружаю df
def load_df(spark, url):
    return spark.read.json(url)

# вывод в Postgres
def output_to_ps(df, tablename):
    df.write.format('jdbc')\
            .mode('overwrite')\
            .option('url', POSTGRES_URL)\
            .option('dbtable', tablename)\
            .options(**POSTGRES_CONNECTION)\
            .save()
