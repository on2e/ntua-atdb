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

    df2.count()

    df1.printSchema()

    df2.printSchema()

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
