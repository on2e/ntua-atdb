# NTUA ECE - Advanced Topics in Databases project

## Project

The project objective was to process a large data set on Hadoop distributed file system using the Apache Spark processing engine. For this purpose, a set of queries was implemented and run using both the Spark DataFrame and RDD APIs. The data set used is a subset of the Yellow Taxi Trip Records, found [here](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), involving the year 2022 and months January to June (the auxiliary Taxi Zone Lookup Table that contains the foreign key of location ids that map to specific boroughs and zones is also included).

| Query | Description                                                                                                                                                                                                                                                                                                                                |
|-------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Q1    | Find the taxi trip with the maximum tip amount for the month of March where the dropoff zone is 'Battery Park'.                                                                                                                                  |
| Q2    | For each month, find the taxi trip with the maximum tolls amount. Ignore zero values.                                                                                                                                                                                                                                                    |
| Q3    | For each consecutive 15-day time window, find the averages of total distance and amount, where the pickup and dropoff zones are different.                                                                                                                                                                                                                        |
| Q4    | For each day of the week, find the top 3 peak hours, meaning the 1-hour time slots of the day with the maximum number of passengers in a single taxi trip. This includes all given months, not a for-each.                                                                                                                                                         |
| Q5    | For each month, find the top 5 days with the maximum average tip-to-fare amount ratio. |

## Technologies

* Apache Spark v3.1.3
* Apache Hadoop v2.7.7
* Python v3.8.0
* OpenJDK v1.8.0_292

## Cluster

A Hadoop & Spark cluster of 2 nodes was deployed using resources provided by GRNET's Okeanos-Knossos [service](https://okeanos-knossos.grnet.gr). Specifically, 2 VMs were created to act as a master-slave cluster using a shared private network. Each VM was given 2 CPUs, 4GB RAM, 30GB HDD and hosted Ubuntu 16.04.7 LTS.

| Node    | IP            | Namenode  | Datanode  | Master    | Worker    |
| :---    |    :----:     |   :----:  |   :----:  |   :----:  |   :----:  |
| Master  | 192.168.0.1   | Yes       | 1         | Yes       | 0,1       |
| Slave   | 192.168.0.2   | No        | 1         | No        | 1         |

## Configuration

This section describes the steps to configure the cluster of the 2 VMs to run the code of this repository. This mainly includes system and config files edited and environment variables set. We assume that both systems have Python 3.8 and JDK 1.8 already installed.

### Hostnames and IP addresses

1. Change hostnames to `master` and `slave` on master and slave nodes respectively. So on master:
    ```console
    $ sudo hostname master
    ```
    and on slave:
    ```console
    $ sudo hostname slave
    ```

2. Edit `/etc/hosts` and map hostnames to IP address:
    ```bash
    192.168.0.1     master
    192.168.0.2     slave
    ```

### Passwordless SSH

The master node, which will host the namenode and master servers, needs to be able to connect through `ssh` to all other nodes in the cluster, in our case just the slave node. This process needs to be done without a password prompt.

1. On master, create ssh rsa public-private key pair:
    ```console
    $ ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
    ```

2. Copy the public key `id_rsa.pub` to `~/.ssh/authorized_keys` file:
    ```console
    $ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    ```

3. Copy the `authorized_keys` to slave. This enables the master node to passwordless-ly `ssh` to the slave node.
    ```console
    $ scp ~/.ssh/authorized_keys slave:~/.ssh/
    ```

### Setup Apache Hadoop

This section describes how to download and setup Apache Hadoop to host 1 namenode and 2 datanodes.

1. Download Hadoop on master node:
    ```console
    $ wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz ~/
    ```

2. Unzip and rename Hadoop folder:
    ```console
    $ tar -xzf ~/hadoop-2.7.7.tar.gz
    $ rm ~/hadoop-2.7.7.tar.gz
    $ mv ~/hadoop-2.7.7 ~/hadoop
    ```

3. Export Hadoop environment variables and add binaries to `$PATH`:
    ```console
    $ echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc
    $ echo 'export HADOOP_HOME=$HOME/hadoop' >> ~/.bashrc
    $ echo 'export HADOOP_COMMON_HOME=$HADOOP_HOME' >> ~/.bashrc
    $ echo 'export HADOOP_HDFS_HOME=$HADOOP_HOME' >> ~/.bashrc
    $ echo 'export PATH=$PATH:$HADOOP_HOME/bin' >> ~/.bashrc
    $ echo 'export PATH=$PATH:$HADOOP_HOME/sbin' >> ~/.bashrc
    ```

4. Source `.bashrc`:
    ```console
    $ source ~/.bashrc
    ```

Under `$HADOOP_HOME/etc/hadoop` edit the below files:

5. `hadoop-env.sh`:
    ```bash
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
    ```

6. `core-site.xml`:
    ```bash
    <configuration>
        <property>
            <name>dfs.replication</name>
            <value>2</value>
        </property>
        <property>
            <name>dfs.namenode.name.dir</name>
            <value>file:///home/user/hdfs/name</value>
        </property>
        <property>
            <name>dfs.datanode.data.dir</name>
            <value>file:///home/user/hdfs/data</value>
        </property>
        <property>
            <name>dfs.blocksize</name>
            <value>64m</value>
        </property>
        <property>
            <name>dfs.support.append</name>
            <value>true</value>
        </property>
        <property>
            <name>dfs.webhdfs.enabled</name>
            <value>true</value>
        </property>
    </configuration>
    ```

7. `hdfs-site.xml`:
    ```bash
    <configuration>
        <property>
            <name>fs.defaultFS</name>
            <value>hdfs://master:9000</value>
        </property>
    </configuration>
    ```

8. `slaves`:
    ```console
    $ echo 'master' > $HADOOP_HOME/etc/hadoop/slaves
    $ echo 'slave' >> $HADOOP_HOME/etc/hadoop/slaves
    ```

9. Copy everything to slave node:
    ```console
    $ scp -r ~/hadoop slave:~/
    $ scp -r ~/hadoop/etc/hadoop slave:~/hadoop/etc/
    $ scp ~/.bashrc slave:~/
    $ ssh slave source ~/.bashrc
    ```

10. Format the HDFS on master:
    ```console
    $ hdfs namenode -format
    ```

11. Start the HDFS cluster by running `start-dfs.sh` on master:
    ```console
    $ start-dfs.sh
    ```

12. Finally, run the `jps` command and verify that every component is up and running:
    ```console
    $ jps
    9618 DataNode
    9483 NameNode
    10204 Jps
    9853 SecondaryNameNode
    $ ssh slave jps
    23831 Jps
    23690 DataNode
    ```

### Setup Apache Spark

This section describes how to download and setup Apache Spark on Hadoop cluster, on standalone mode, that will run applications on `client` deployment mode.

1. Download Spark on master node:
    ```console
    $ wget https://downloads.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop2.7.tgz ~/
    ```

2. Unzip and rename Spark folder:
    ```console
    $ tar -xzf ~/spark-3.1.3-bin-hadoop2.7.tar.gz
    $ rm ~/spark-3.1.3-bin-hadoop2.7.tar.gz
    $ mv ~/spark-3.1.3-bin-hadoop2.7 ~/spark
    ```

3. Export Spark environment variables, add binary to `$PATH` and set aliases:
    ```console
    $ echo 'export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop' >> ~/.bashrc
    $ echo 'export SPARK_HOME=$HOME/spark' >> ~/.bashrc
    $ echo 'export PYSPARK_PYTHON=python3.8' >> ~/.bashrc
    $ echo 'export PYSPARK_DRIVER_PYTHON=python3.8' >> ~/.bashrc
    $ echo 'export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH' >> ~/.bashrc
    $ echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
    $ echo 'alias start-all.sh="$SPARK_HOME/sbin/start-all.sh"' >> ~/.bashrc
    $ echo 'alias stop-all.sh="$SPARK_HOME/sbin/stop-all.sh"' >> ~/.bashrc
    $ echo 'alias start-worker.sh="$SPARK_HOME/sbin/start-worker.sh spark://master:7077"' >> ~/.bashrc
    $ echo 'alias stop-worker.sh="$SPARK_HOME/sbin/stop-worker.sh"' >> ~/.bashrc
    ```

4. Source `.bashrc`:
    ```console
    $ source ~/.bashrc
    ```

Under `$SPARK_HOME/conf` edit the below files:

5. `spark-env.sh`:
    ```console
        $ cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
        $ echo 'SPARK_WORKER_CORES=2' > $SPARK_HOME/conf/spark-env.sh
        $ echo 'SPARK_WORKER_MEMORY=3g' >> $SPARK_HOME/conf/spark-env.sh
    ```

6. `spark-defaults.conf`:
    ```console
    $ cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
    $ echo 'spark.master              spark://master:7077' > $SPARK_HOME/conf/spark-defaults.conf
    $ echo 'spark.submit.deployMode   client' >> $SPARK_HOME/conf/spark-defaults.conf
    $ echo 'spark.executor.instances  2' >> $SPARK_HOME/conf/spark-defaults.conf
    $ echo 'spark.executor.cores      2' >> $SPARK_HOME/conf/spark-defaults.conf
    $ echo 'spark.executor.memory     1536m' >> $SPARK_HOME/conf/spark-defaults.conf
    $ echo 'spark.driver.memory       512m' >> $SPARK_HOME/conf/spark-defaults.conf
    ```

7. `workers`:
    ```console
    $ cp $SPARK_HOME/conf/workers.template $SPARK_HOME/conf/workers
    $ echo 'master' > $SPARK_HOME/conf/workers
    $ echo 'slave' >> $SPARK_HOME/conf/workers
    ```

8. Copy everything to slave node:
    ```console
    $ scp -r ~/spark slave:~/
    $ scp -r ~/spark/conf slave:~/spark/
    $ scp ~/.bashrc slave:~/
    $ ssh slave source ~/.bashrc
    ```

9. Start Spark by running `start-all.sh` on master:
    ```console
    $ start-all.sh
    ```

10. Finally, run the `jps` command and verify that every component is up and running:
    ```console
    $ jps
    15442 Jps
    9618 DataNode
    15062 Worker
    9483 NameNode
    9853 SecondaryNameNode
    14943 Master
    $ ssh slave jps
    24233 Worker
    23690 DataNode
    24351 Jps
    ```

## Run queries

This section describes how to clone this repository on the master node and run the queries under `src/main`.

1. Make directory to clone this repository, call it `atdb-project`:
    ```console
    $ mkdir atdb-project
    $ cd atdb-project
    ```

2. Clone this repository:
    ```console
    $ git clone https://github.com/...
    ```

3. Export `$ATDB_PROJECT_HOME` environment variable pointing at `atdb-project` directory:
    ```console
    $ echo 'export ATDB_PROJECT_HOME=/path/to/atdb-project' >> ~/.bashrc
    ```

4. Source `.bashrc`:
    ```console
    $ source ~/.bashrc
    ```

5. Install required python dependencies:
    ```console
    $ pip3.8 install -r requirements.txt
    ```

6. Make the directories for the Yellow Taxi Trip Records and the Taxi Zone Lookup Table files:
    ```console
    $ mkdir -p data/parquet data/csv
    ```

7. Download the required files:
    ```console
    $ for i in {1..6}
    > do
    >    wget "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-0${i}.parquet" -P data/parquet/
    > done
    $ wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv -P data/csv/
    ```

8. Upload the files to HDFS:
    ```console
    $ hdfs dfs -put data /
    ```

9. Test by running a query:
    ```console
    $ spark-submit src/main/q1_df.py
    ```
    or run all using `submit-all.sh` and output logs and results under `logs` and `out` directories:
    ```console
    $ src/submit-all.sh
    Submitting file q1_df.py...
    Submitting file q2_df.py...
    ```

10. Finally, to scale down to 1 worker:
    ```console
    $ stop-worker.sh
    ```
    and to scale back up:
    ```console
    $ start-worker.sh
    ```
