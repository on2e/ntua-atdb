from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import Row
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os, sys, time

load_dotenv()

def is_diff_pudozone(row: Row) -> bool:
    """ Returns true if given row has different pickup and dropoff zones.
    Uses shared `zone_lookup_table` dictionary.
    """
    return zone_lookup_table[str(row.PULocationID)] != \
           zone_lookup_table[str(row.DOLocationID)]


def to_window(row: Row) -> list:
    """ Checks if given row falls into any of the desired time windows.
    Returns list of single key-value tuple if it does, where key is the window
    tuple of `start` and `end` timestamps of :class:`datetime.datetime`,
    and value is a tuple that serves aggregations that come next.
    Otherwise, returns empty list so rows falling out of desired ranges
    are discarded using flatMap.
    """
    td    = timedelta(15)        # Time duration of 15 days
    start = datetime(2022, 1, 1) # Naive datetime object of '2022-01-01 00:00:00'
    end   = start + td
    while start.month < 7:
        if start <= row.tpep_dropoff_datetime < end:
            k, v = (start, end), (row.trip_distance, row.total_amount, 1)
            return [(k, v)]
        start, end = end, end + td
    return []


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master('spark://master:7077') \
        .appName('Q3RDD') \
        .getOrCreate()

    # Loads data from HDFS
    taxi_trip_df   = spark.read.parquet('hdfs://master:9000/data/parquet')
    zone_lookup_df = spark.read.option('header', 'true').csv('hdfs://master:9000/data/csv')

    df1 = taxi_trip_df.alias('df1')
    df2 = zone_lookup_df.alias('df2')

    # --------------- Q1 query ---------------

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

    # Creates RDDs
    rdd1 = df1.rdd
    rdd2 = df2.rdd

    # Creates `LocationID`: `Zone` dictionary
    zone_lookup_table = dict( rdd2.map(lambda x: (x.LocationID, x.Zone)).collect() )

    rdd1 = rdd1 \
        .filter(is_diff_pudozone) \
        .flatMap(to_window) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])) \
        .mapValues(lambda x: (x[0]/x[2], x[1]/x[2])) \
        .sortByKey()

    collected = rdd1.collect()

    end = time.time()

    # Outputs result
    for row in collected:
        print(row)

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
