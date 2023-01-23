from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, split, when, current_timestamp
from pyspark.sql import SparkSession
import sys

# Forcing the day to process
day = 7
month = 12
year = 2022

# Using today to process automatic
# year = datetime.today().year
# month = datetime.today().month
# day = datetime.today().day

try:
    sparkSession = (SparkSession
                    .builder
                    .getOrCreate()
                    )

    sparkSession.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs",
                                                             "false")

    cleaned_bucket_name = sys.argv[1]
    curated_bucket_name = sys.argv[2]
    paths = sys.argv[3]

    paths = paths.replace('s3', 's3a')

    obj_paths = paths.split(',')

    print(obj_paths)

    movie_data_path = f's3a://{cleaned_bucket_name}/movies.csv'

    destination_path = f"s3a://{curated_bucket_name}/cinema_sell_data/"

    sparkSession = (SparkSession
                    .builder
                    .getOrCreate())

    sell_data = sparkSession.read.parquet(*obj_paths)

    sell_data = sell_data.drop('imdb_id')

    movie_data = sparkSession.read.option("header", True).csv(movie_data_path)

    try:
        movie_data = movie_data.withColumn("production_companies", when(col("production_companies").contains("name"),
                                                                        split(col("production_companies"), "'")[3])
                                           .otherwise(lit('NO_NAME')))
        movie_data = movie_data.withColumn("production_countries", when(col("production_countries").contains("name"),
                                                                        split(col("production_countries"), "'")[7])
                                           .otherwise(lit('NO_COUNTRIE')))
        movie_data = movie_data.withColumn("spoken_languages", when(col("spoken_languages").contains("name"),
                                                                    split(col("spoken_languages"), "'")[7])
                                           .otherwise(lit('NO_LANGUAGE')))
        movie_data = movie_data.withColumn("genres", when(col("genres").contains("name"), split(col("genres"), "'")[5])
                                           .otherwise(lit('NO_GENRE')))
    except Exception as e:
        movie_data = movie_data.withColumn("production_companies", lit('NO_NAME'))
        movie_data = movie_data.withColumn("production_countries", lit('NO_COUNTRIE'))
        movie_data = movie_data.withColumn("spoken_languages", lit('NO_LANGUAGE'))
        movie_data = movie_data.withColumn("genres", lit('NO_GENRE'))
        print(f"ERROR: {e}")

    df = sell_data.join(movie_data, ['id'])

    complete_sell_data = df.withColumn('release_year', split(df.release_date, "-")[0]) \
        .withColumn('sell_day', split(df.sell_date, "-")[2]) \
        .withColumn('year', lit(f"{datetime.today().year}")) \
        .withColumn('month', lit(f"{month}")) \
        .withColumn('day', lit(f"{day}")) \
        .withColumn('process_timestamp', current_timestamp()) \
        .drop("release_date") \
        .drop("homepage") \
        .drop("original_title")

    complete_sell_data = complete_sell_data.where(col("status") == lit("Released"))

    complete_sell_data.coalesce(1).write.mode('append').partitionBy("year", "month", "day").parquet(destination_path)

except Exception as e:
    print(f'ERROR: {e}')
    raise Exception(f'ERROR: {e}')
