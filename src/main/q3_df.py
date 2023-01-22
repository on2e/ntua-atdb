from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os, sys, time

load_dotenv()

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master('spark://master:7077') \
        .appName('Q3DF') \
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

    # --------------- Q3 query ---------------

    start = time.time()

    w = F.window('tpep_dropoff_datetime', '15 days', startTime='70 hours')

    cond1 = df1.PULocationID == df2.LocationID
    cond2 = [df1.DOLocationID == df2.LocationID, F.col('pickup_zone') != df2.Zone]

    df1 = df1 \
        .join(df2, cond1) \
        .select('df1.*', F.col('Zone').alias('pickup_zone')) \
        .join(df2, cond2, 'left_semi') \
        .groupBy(w) \
        .agg(F.avg('trip_distance'), F.avg('total_amount')) \
        .orderBy('window.start')

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
