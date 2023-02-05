import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F


spark_jars_packages = ",".join(
        [
            "org.postgresql:postgresql:42.4.0",
        ]
    )

spark = SparkSession.builder \
                    .master("yarn") \
                    .appName("read_json") \
                    .config("spark.sql.session.timeZone", "UTC") \
                    .config("spark.jars.packages", spark_jars_packages) \
                    .getOrCreate()

df = spark.read.json('stage/events-2022-Sep-30-2134.json')
df.printSchema()


window = Window().partitionBy(F.col('user_custom_id')).orderBy(F.desc('hour'))

# result = df.select(
#             F.date_trunc('hour','event_timestamp').alias('hour'),
#             'user_custom_id',
#             'referer_url',
#             'page_url_path',
#         ) \
#         .withColumn(
#             'prev_page_url_path',
#             F.lag('page_url_path', 1).over(window),        
#         ) \
#         .withColumn(
#             'prev2_page_url_path',
#             F.lag('page_url_path', 2).over(window),        
#         ) \
#         .where(
#             F.col('page_url_path') == '/confirmation'
#         ) \
#         .groupBy('prev2_page_url_path').count() \
#         .orderBy(F.desc('count'))

# result.show()

result = df.select(
            F.col('event_timestamp').alias('hour'),
            'user_custom_id',
            'referer_url',
            'page_url_path',
        ) \
        .orderBy('user_custom_id', 'hour') \
        .withColumn(
            'num_confirm',
            F.when(
                F.col('page_url_path') == '/confirmation',
                F.monotonically_increasing_id()
            )
        ) \
        .withColumn(
            'confirm_ts',
            F.when(
                F.col('page_url_path') == '/confirmation',
                F.col('hour'),
            )
        ) \
        .withColumn(
            'num_confirm_no_null',
            F.min('num_confirm').over(window)
        ) \
        .withColumn(
            'confirm_ts_no_null',
            F.min('confirm_ts').over(window)
        ) \
        .withColumn(
            'product',
            F.when(
                F.col('page_url_path').like('%product%'),
                F.col('page_url_path')
            )
        ) \
        .groupBy(
            'confirm_ts_no_null',
            'user_custom_id',
            'referer_url',
            'num_confirm_no_null',
        ) \
        .agg(F.countDistinct('product').alias('count')) \
        .where(F.col('confirm_ts_no_null').isNotNull()) \
        .orderBy('user_custom_id', 'confirm_ts_no_null')


result.show()

result.write \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://158.160.52.27/postgres') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'cdm.dm_purchases_products') \
        .option('user', 'jovyan') \
        .option('password', 'jovyan') \
        .mode('overwrite') \
        .save()
        
        
