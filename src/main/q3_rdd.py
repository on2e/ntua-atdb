from pyspark.sql import SparkSession
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
    taxi_trips_df   = spark.read.parquet('hdfs://master:9000/data/parquet')
    zone_lookups_df = spark.read.option('header', 'true').csv('hdfs://master:9000/data/csv')

    df1 = taxi_trips_df.alias('df1')
    df2 = zone_lookups_df.alias('df2')

    rdd1 = df1.rdd
    rdd2 = df2.rdd

    # --------------- Q1 query ---------------

    start = time.time()

    zone_lookup_table = dict( rdd2.map(lambda x: (x.LocationID, x.Zone)).collect() )

    rdd1 = rdd1 \
        .filter(is_diff_pudozone) \
        .flatMap(to_window) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])) \
        .mapValues(lambda x: (x[0]/x[2], x[1]/x[2])) \
        .sortByKey()

    #rdd1.collect()
    for r in rdd1.collect():
        print(r)

    elapsed = time.time() - start
    base    = os.path.basename(sys.argv[0]).split('.')[0]
    home    = os.getenv('ATDB_PROJECT_HOME')

    # Gets number of workers
    sc = spark._jsc.sc()
    nworkers = len([executor.host() for executor in sc.statusTracker().getExecutorInfos()])-1

    with open(f'{home}/out/{base}_w{nworkers}.time', 'w') as f:
        f.write(f'{elapsed:.2f} s\n')

    spark.stop()
