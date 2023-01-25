from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master('spark://master:7077') \
        .appName('Tests') \
        .getOrCreate()

    # Loads data from HDFS
    taxi_trips_df   = spark.read.parquet('hdfs://master:9000/data/parquet')
    zone_lookups_df = spark.read.option('header', 'true').option("inferSchema", "true").csv('hdfs://master:9000/data/csv')

    df1 = taxi_trips_df.alias('df1')
    df2 = zone_lookups_df.alias('df2')

    # --------------- Tests ---------------

    df1.count()
    """
    19817583
    """

    df2.count()
    """
    265
    """

    df1.printSchema()
    """
    root
     |-- VendorID: long (nullable = true)
     |-- tpep_pickup_datetime: timestamp (nullable = true)
     |-- tpep_dropoff_datetime: timestamp (nullable = true)
     |-- passenger_count: double (nullable = true)
     |-- trip_distance: double (nullable = true)
     |-- RatecodeID: double (nullable = true)
     |-- store_and_fwd_flag: string (nullable = true)
     |-- PULocationID: long (nullable = true)
     |-- DOLocationID: long (nullable = true)
     |-- payment_type: long (nullable = true)
     |-- fare_amount: double (nullable = true)
     |-- extra: double (nullable = true)
     |-- mta_tax: double (nullable = true)
     |-- tip_amount: double (nullable = true)
     |-- tolls_amount: double (nullable = true)
     |-- improvement_surcharge: double (nullable = true)
     |-- total_amount: double (nullable = true)
     |-- congestion_surcharge: double (nullable = true)
     |-- airport_fee: double (nullable = true)
    """

    df2.printSchema()
    """
    root
     |-- LocationID: integer (nullable = true)
     |-- Borough: string (nullable = true)
     |-- Zone: string (nullable = true)
     |-- service_zone: string (nullable = true)
    """

    # Counts Null or NaN values in each column
    dropped = df1.drop('tpep_pickup_datetime', 'tpep_dropoff_datetime')

    df1 \
        .select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in dropped.columns]) \
        .show()
    """
    +--------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+
    |VendorID|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|airport_fee|
    +--------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+
    |       0|         671901|            0|    671901|            671901|           0|           0|           0|          0|    0|      0|         0|           0|                    0|           0|              671901|     671901|
    +--------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+
    """

    # There are 671901 rows where columns:
    #
    #  `passenger_count`
    #  `RatecodeID`
    #  `store_and_fwd_flag`
    #  `congestion_surcharge`
    #  `airport_fee`
    #
    # are all Null
    df1.filter(F.col('passenger_count').isNull() & \
                F.col('RatecodeID').isNull() & \
                F.col('store_and_fwd_flag').isNull() & \
                F.col('congestion_surcharge').isNull() & \
                F.col('airport_fee').isNull()) \
        .count()
    """
    671901
    """

    # Checks years involved
    df1 \
        .select(F.year('tpep_dropoff_datetime').alias('dropoff_year')) \
        .groupBy('dropoff_year') \
        .count() \
        .orderBy('dropoff_year') \
        .show()
    """
    +------------+--------+                                                         
    |dropoff_year|   count|
    +------------+--------+
    |        2001|       1|
    |        2002|     300|
    |        2003|       7|
    |        2009|      53|
    |        2012|       1|
    |        2021|       1|
    |        2022|19817219|
    |        2023|       1|
    +------------+--------+
    """

    # Checks months involved
    df1 \
        .select(F.month('tpep_dropoff_datetime').alias('dropoff_month')) \
        .groupBy('dropoff_month') \
        .count() \
        .orderBy('dropoff_month') \
        .show()
    """
    +-------------+-------+                                                         
    |dropoff_month|  count|
    +-------------+-------+
    |            1|2457656|
    |            2|2978574|
    |            3|3614271|
    |            4|3598113|
    |            5|3595108|
    |            6|3554840|
    |            7|  18719|
    |            8|      1|
    |           10|    300|
    |           12|      1|
    +-------------+-------+
    """

    # Checks for negative values on aggregation columns
    # where values are expected to be non-negative
    df1.filter(F.col('tip_amount') < 0).count()
    """
    1769
    """

    df1.filter(F.col('tolls_amount') < 0).count()
    """
    4220
    """

    df1.filter(F.col('trip_distance') < 0).count()
    """
    0
    """

    df1.filter(F.col('total_amount') < 0).count()
    """
    111474
    """

    df1.filter(F.col('passenger_count') < 0).count()
    """
    0
    """

    df1.filter(F.col('fare_amount') < 0).count()
    """
    110150
    """

    # Cleans data.
    # Keeps only rows with non-Null values.
    # Keeps only year 2022 and months Jan-Jun.
    # Keeps only non-negative values for fields of interest.
    clean = F.col('passenger_count').isNotNull() & \
            (F.year('tpep_dropoff_datetime') == 2022) & \
            F.month('tpep_dropoff_datetime').between(1, 6) & \
            (F.col('tip_amount') >= 0) & \
            (F.col('tolls_amount') >= 0) & \
            (F.col('trip_distance') >= 0) & \
            (F.col('total_amount') >= 0) & \
            (F.col('passenger_count') >= 0) & \
            (F.col('fare_amount') >= 0)

    df1.filter(clean).count()
    """
    19016170
    """

    # Proves there are zones with more than one location id
    df2 \
        .groupBy('Zone') \
        .count() \
        .orderBy(F.desc('count')) \
        .show(5, truncate=False)
    """
    +---------------------------------------------+-----+                           
    |Zone                                         |count|
    +---------------------------------------------+-----+
    |Governor's Island/Ellis Island/Liberty Island|3    |
    |Corona                                       |2    |
    |Homecrest                                    |1    |
    |Bensonhurst West                             |1    |
    |Newark Airport                               |1    |
    +---------------------------------------------+-----+
    only showing top 5 rows
    """

    spark.stop()
