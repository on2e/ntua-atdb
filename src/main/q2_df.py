from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os, sys, time

load_dotenv()

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master('spark://master:7077') \
        .appName('Q2DF') \
        .getOrCreate()

    # Loads data from HDFS
    taxi_trips_df   = spark.read.parquet('hdfs://master:9000/data/parquet')
    zone_lookups_df = spark.read.option('header', 'true').csv('hdfs://master:9000/data/csv')

    df1 = taxi_trips_df.alias('df1')
    df2 = zone_lookups_df.alias('df2')

    # Keeps only year 2022 and months Jan-Jun
    df1 = df1 \
        .filter((F.year('tpep_dropoff_datetime') == 2022) & \
                 F.month('tpep_dropoff_datetime').between(1, 6))

    # --------------- Q2 query ---------------

    start = time.time()

    df1 = df1 \
        .filter(F.col('tolls_amount') != 0) \
        .groupBy(F.month('tpep_dropoff_datetime').alias('dropoff_month')) \
        .agg(F.max('tolls_amount')) \

    df1.show(truncate=False)

    elapsed = time.time() - start
    base    = os.path.basename(sys.argv[0]).split('.')[0]
    home    = os.getenv('ATDB_PROJECT_HOME')

    # Gets number of workers
    sc = spark._jsc.sc()
    nworkers = len([executor.host() for executor in sc.statusTracker().getExecutorInfos()])-1

    with open(f'{home}/out/{base}_w{nworkers}.time', 'w') as f:
        f.write(f'{elapsed:.2f} s\n')

    spark.stop()
