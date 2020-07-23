[UDACITY DATA ENGINEERING NANODEGREE](https://classroom.udacity.com/nanodegrees)

Project: Data Lake
----

# **Introduction**

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

# **Project Description**
In this project, we'll apply what we've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3 :  
- load data from S3  
- process the data into analytics tables using Spark  
- load them back into S3  
- deploy this Spark process on a cluster using AWS  

# **Project Datasets**
You'll be working with two datasets that reside in S3. Here are the S3 links for each:

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

# **Project Files**

- **requirements.txt** PIP install requirements to install necessary python packages  (including AWS CLI packages to work with Amazon Web Services)

- **docker-compose.yaml** Docker-compose configuration to spin-up a Spark local cluster + Jupyter lab server for local development. Having a local spark cluster provide a very flexible and cheap way to develop spark code.  

- **./scripts/emr_create_cluster.sh** example of bash script to create an EMR cluster using AWS CLI

- **sparkify_etl_spark_datalake.ipynb** Jupyter notebook to use for developing and testing Spark ETL process
    - Extract data from json file (song_data and log_data) from S3 bucket
    - Transform song_data and log_data into Fact and Dimension tables
    - Load Fact and Dimension tables in parquet files into S3 bucket

- **etl.py** script to run ETL process on Spark (Local mode or cluster mode)
    - rafactoring into functions of the python code from **sparkify_etl_spark_datalake.ipynb** notebook
    - Extract data from json file (song_data and log_data) from S3 bucket
    - Transform song_data and log_data into Fact and Dimension tables
    - Load Fact and Dimension tables in parquet files into S3 bucket  

- **./data** folder containing a small subset of the song_data and log_data dataset - useful for local developement

# **ETL logic**

## **Initial dataset - Raw JSON data structures**

- **log_data**: log_data contains data about what users have done (columns: event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId)
- **song_data**: song_data contains data about songs and artists (columns: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)

## **Schema for staging tables**

We will create a star schema optimized for queries on song play analysis. This includes the following tables. Using the songs and log datasets in S3 buckets, data will be transformed and loaded into the fact and dimensions tables in S3 buckets.

### **Dimension Tables**

- **songs_table**: song info (columns: song_id, title, artist_id, year, duration) -> extracted from songs_data. Table **partitionned by year and then artist_id**
- **users_table**: user info (columns: user_id, first_name, last_name, gender, level) -> extracted from song_data
- **artists_table**: artist info (columns: artist_id, name, location, latitude, longitude) -> extracted from log_data.
- **time_table**: detailed time info about song plays (columns: start_time, hour, day, week, month, year, weekday) -> extracted from log_data. Table **partitionned by year and month**

### **Fact Table

- **songplays**: song play data together with user, artist, and song info (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) -> created by joining log_data and songs_table. Table **partitionned by year and month**

# **Running ETL process**

## **Developing and running Spark code in Local Mode (1 node)**

We will use Docker to setup 2 services :  
- a local Spark cluster (single node) running Spark 2.45 with Pyspark installed
- a Jupyter notebook server that will be used to develop, test, debug Pyspark code

For more details on the docker-compose file, read the Medium article [Running PySpark and Jupyter using Docker](https://blog.k2datascience.com/running-pyspark-with-jupyter-using-docker-61ca0aa7da6b)

Description of Dockerfile is found on Github : [Jupyter Notebook Python, Spark Stack](https://github.com/jupyter/docker-stacks/tree/master/pyspark-notebook)


### **Running Spark in Local Mode using Docker**

Spin-up a Spark cluster in Local Mode and run Jupyter Server :

```docker
docker-compose up -d
```

- Jupyter notebook server is exposed on : http://localhost:8888/ (you we'll need to use the token provided in Docker container's log)  
- Pyspark shell is accessed from the Jupyter terminal  
- Spark UI is exposed on : http://localhost:4040/  

You can open Jupyter notebook **sparkify_etl_spark_datalake.ipynb** and follow the ETL steps.  
For learning purpose, the ETL transformation steps in the notebook were processed using both Spark SQL API and Spark Dataframe SQL.

However, when the code was refactored in etl.py script, only Spark Dataframe API is used.

### **Running etl.py in local mode**

From the Jupyter terminal, just run the etl.py script :
```python
python etl.py
```

## **Developing and Running Spark in AWS EMR Cluster**

## **Setup AWS EMR Cluster**

Create an AWS EMR cluster using AWS CLI. Run the shell script **scripts/emr_create_cluster.sh**
```bash
bash ./scripts/emr_create_cluster.sh
```
Once the EMR cluster is running, you need :  
- to allow SSH connection  
- open tunnel to EMR cluster to access web UI
- if needed, setup Jupyter Notebook to use the running EMR cluster
- In some EMR version, if python 3 is required to run the script, you need to run the follwing command on EMR cluster shell
```bash
sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh
```

### **Running etl.py on AWS EMR cluster**

On EMR cluster, you will need :  
- transfer using SCP etl.py script to the EMR cluster
- submit a Spark job to run the etl.py script
```python
spark-submit --master yarn ./etl.py
```

**Warning :** Please be aware that using S3 bucket with S3n "s3n://aws_bucket/" path results in very long processing time on AWS. This is a known issue.  
So run the etl.py script using S3 path "s3://aws_bucket/" to have faster etl process.

See [Udacity - Knowledge discussion - How long does the the ETL should take on the whole dataset?](https://knowledge.udacity.com/questions/172931)

Sparkify Schema/S3 buckets checking after ETL :

- artist_table 201 files, 736.9 kb
- songplays_table 201 files, 925.6 kb
- songs_table 14886 files, 16.1 MB
- time_table 201 files, 375.4 kb
- users_table 86 - 104.6 kb
