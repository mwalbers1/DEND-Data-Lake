## Data Lake Project - Apache Spark and AWS S3
The purpose of this project is to build an ETL pipeline for music streaming data which resides in AWS S3 as json files. There are two categories of data, the log events detailing songs listened to by users, and the song library consisting of song and artist details.  The ETL pipeline extracts the json log event and song data from AWS S3 and loads the relevant data attributes into dimension and fact tables residing on an AWS EMR cluster server. The newly created dimension and fact tables are then saved to separate parquet files on a destination S3 bucket.

There are advantages to leveraging PySpark for this ETL process.  The Spark AWS EMR cluster can process the source json files in parallel which is faster than loading into a traditional RDBMS system or a data warehouse server.  Because Spark processes the source json files in memory, it is much faster than staging the data into physical storage before transforming and loading the data.  Spark is also scalable both vertically and horizontally to meet increased volumes of data.  The Spark cluster design described below reduces the need of running an AWS EMR cluster full time.

### Spark Cluster Server

<img src="Data Lake Project.png"></img>


1. The client issues a spark-submit command to the AWS EMR server to initiate the PySpark ETL process

2.  Then the Spark job will read in the source song and event data from the S3 bucket which are in json format.

3.  The Spark job calls etl.py which initiates a spark session and builds out the dimension and fact tables by extracting the relevant data points from songs and events json files.

4. The transformed data elements are then loaded into new Spark dataframes for further analysis.  

5. The new dimension and fact tables created in step 4 are written out to parquet files in an AWS S3 destination bucket which can be read back into Spark dataframes at a later time, on a new Spark session running on a new AWS EMR cluster.  This allows us to use our AWS EMR cluster server on as-needed basis without the need to keep the Spark cluster server continuously running and accruing costs.



## PySpark ETL Script

The project contains the following Python scripts.

- etl.py

The `etl.py` script is responsible for loading data into the dimension tables, and fact table.

### Instructions

1. Open a terminal session and issue **spark-submit --master local[*] etl.py** command to execute the ETL process from a local installation of Spark.

2. To deploy on an Amazon EMR cluster, follow the instructions at the link provided.

https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html

<font color='red'>Important Note</font><br>
Before running the `etl.py` script, update the **AWS Secret Keys** in the dl.cfg configuration file.


## Extract Input Data

The log events json data consists of listening activity of users on the music streaming service called Sparkify. The log event data json files are read into the Spark cluster from the S3 source bucket. The song data is a collection of json music files which represents the universe of songs and associated artists. These files are read into the Spark cluster from the S3 source bucket.

An excerpt of the song and log event data is listed below.

**Song json data**

    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

**Log event data**

    {"artist":"Stephen Lynch","auth":"Logged In","firstName":"Jayden","gender":"M","itemInSession":0,"lastName":"Bell","length":182.85669,"level":"free","location":"Dallas-Fort Worth-Arlington, TX","method":"PUT","page":"NextSong","registration":1540991795796.0,"sessionId":829,"song":"Jim Henson's Dead","status":200,"ts":1543537327796,"userAgent":"Mozilla\/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident\/6.0)","userId":"91"}

## Transform Data
### Dimension Tables

- The distinct users of the music streaming service are extracted from the log event dataframe and loaded into the `users` dimension table.

- The timestamps are extracted from the log events dataframe and loaded into the `time` dimension table.

- The artists information including artist id, artist name, and location are loaded into the `artists` table.

- The song data such as song id, song title, duration and associated artist id are loaded into the `songs` dimension table.

- Duplicate records are removed from each table.


### Fact Table

- The fact table called `songplays` is derived from the the log events dataframe joined with the songs and time dataframes

## Load Data to S3 Parquet files

The pySpark DataFrame.write.parquet command is used to write the dimension tables and fact table to a destination AWS S3 bucket.  

The songs table is partitioned by year and artist id. The time table is partitioned by year and month.  The songplays table is partitioned by year and month.
