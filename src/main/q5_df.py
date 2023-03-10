from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os, sys, time

load_dotenv()

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master('spark://master:7077') \
        .appName('Q5DF') \
        .getOrCreate()

    # Loads data from HDFS
    taxi_trip_df   = spark.read.parquet('hdfs://master:9000/data/parquet')
    zone_lookup_df = spark.read.option('header', 'true').csv('hdfs://master:9000/data/csv')

    df1 = taxi_trip_df.alias('df1')
    df2 = zone_lookup_df.alias('df2')

    # --------------- Q5 query ---------------

    start = time.time()

    # Cleans data - See `tests.py`
    clean = (F.year('tpep_dropoff_datetime') == 2022) & \
            F.month('tpep_dropoff_datetime').between(1, 6) & \
            (F.col('tip_amount') >= 0) & \
            (F.col('tolls_amount') >= 0) & \
            (F.col('trip_distance') >= 0) & \
            (F.col('total_amount') >= 0) & \
            (F.col('passenger_count') >= 0) & \
            (F.col('fare_amount') >= 0)

    df1 = df1.filter(clean)

    w = Window \
        .partitionBy('dropoff_month') \
        .orderBy(F.desc('avg(tip_to_fare_pct)'))

    df1 = df1 \
        .withColumn('dropoff_month', F.month('tpep_dropoff_datetime')) \
        .withColumn('dropoff_day', F.dayofweek('tpep_dropoff_datetime')) \
        .withColumn('tip_to_fare_pct', df1.tip_amount / df1.fare_amount * 100) \
        .select('dropoff_month', 'dropoff_day', 'tip_to_fare_pct') \
        .groupBy('dropoff_month', 'dropoff_day') \
        .agg(F.avg('tip_to_fare_pct')) \
        .orderBy('dropoff_month') \
        .withColumn('row', F.row_number().over(w)) \
        .filter(F.col('row') <= 5) \
        .drop('row')

    df1.persist().collect()

    end = time.time()

    # Outputs result
    df1.show(30, truncate=False)

    base    = os.path.basename(sys.argv[0]).split('.')[0]
    home    = os.getenv('ATDB_PROJECT_HOME')
    elapsed = end - start

    # Gets number of workers
    sc = spark._jsc.sc()
    nworks = len([executor.host() for executor in sc.statusTracker().getExecutorInfos()])-1

    # Outputs elapsed query execution time to file
    with open(f'{home}/out/{base}_w{nworks}.time', 'w') as f:
        f.write(f'{elapsed:.2f} s\n')

    spark.stop()
