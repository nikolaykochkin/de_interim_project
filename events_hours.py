# Импорт стандартных библиотек
from datetime import datetime
import sys
# Импорт компонентов pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# параметры подключения 
HOST = '158.160.52.27'
POSTGRES_URL = f'jdbc:postgresql://{HOST}:5432/jovyan'
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
        .withColumn('product_type', F.expr(f"""
            case 
                when page_url_path like '/product%' then substring(page_url_path, 10, length(page_url_path)-9)
                else 'None'
            end
            """))\
        .withColumn('event_type', F.expr(f"""
            case 
                when page_url_path like '/product%' then 'product'
                else substring(page_url_path, 2, length(page_url_path))
            end
            """))\
            .withColumn('event_hour', F.date_trunc('hour', 'event_timestamp'))\
            .groupBy(GROUP_COLS).agg(F.count('event_hour').alias('events_count'))\
            .select(OUT_COLS)

# Вывод в Postgres
def output_to_ps(df):
    df = df.withColumn('feedback', F.lit(''))
    df.write.format('jdbc')\
            .mode('append')\
            .option('url', POSTGRES_URL)\
            .option('dbtable', 'subscribers_feedback')\
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
    output_to_ps(df)

if __name__ == "__main__":
    main()