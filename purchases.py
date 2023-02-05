# Импорт стандартных библиотек
import sys
# Импорт компонентов pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# параметры подключения 
HOST = '158.160.52.27'
POSTGRES_URL = f'jdbc:postgresql://{HOST}:5432/postgres'
POSTGRES_CONNECTION = {'user':'jovyan', 'password':'jovyan', 'driver': 'org.postgresql.Driver'}

# Ожидаемые колонки для вывода
OUT_COLS = [
    'event_type',
    'events_count',
    'event_hour'
    'product_type'
    'geo_country', 
    'geo_latitude', 
    'geo_longitude', 
    'geo_region_name', 
    'geo_timezone', 
    'referer_medium', 
    'referer_url', 
    'referer_url_port', 
    'referer_url_scheme', 
    'utm_campaign', 
    'utm_content', 
    'utm_medium', 
    'tm_source',
]
GROUP_COLS = [
        'event_type',
    'event_hour',
    'product_type'
    'geo_country', 
    'geo_latitude', 
    'geo_longitude', 
    'geo_region_name', 
    'geo_timezone', 
    'referer_medium', 
    'referer_url', 
    'referer_url_port', 
    'referer_url_scheme', 
    'utm_campaign', 
    'utm_content', 
    'utm_medium', 
    'tm_source',
]

# Обработка входных параметров скрипта
try:
    DATA_URL = sys.argv[1]
except IndexError: 
    DATA_URL  ='stage/events-2022-Sep-30-2134.json'

# зaпускаю spark
def spark_init(app_name):
    return SparkSession.builder \
                    .master("local") \
                    .appName(app_name) \
                    .getOrCreate()

# Загружаю df
def load_df(spark, url):
    return spark.read.json(url)


# преобразования и группировка

def transform(df):
    return df\
            .withColumn('event_hour', F.date_trunc('hour', 'event_timestamp'))\
            .where(r"page_url_path = '/confirmation'")    

# Вывод в Postgres
def output_to_ps(df, tablename):
    df.write.format('jdbc')\
            .mode('overwrite')\
            .option('url', POSTGRES_URL)\
            .option('dbtable', tablename)\
            .options(**POSTGRES_CONNECTION)\
            .save()


def main():
    # запускаем spark
    spark = spark_init('events_hourly')
    # загружаем json
    df = load_df(spark, DATA_URL)
    # парсим json
    df = transform(df)
    # запускаем отправку в PS
    output_to_ps(df, 'cdm.dm_purchases')

if __name__ == "__main__":
    main()