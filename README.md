## Data modeling with Spark

### Summary
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3. In this project, you'build an ETL pipeline using Spark and data stored in S3.

The Redshift database contains these tables:

* Staging Tables
  - staging_events
  - staging_songs
* Fact Table:
  - songplays: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
* Dimension Tables: 
  - users: user_id, first_name, last_name, gender, level
  - songs: song_id, title, artist_id, year, duration
  - artists: artist_id, name, location, latitude, longitude
  - time: start_time, hour, day, week, month, year, weekday


The project includes six files:
* dwh.cfg access keys
* etl.py reads songdata from S3, process it with Spark to dim and fact tables and finally save it partitioned in S3.
* etl.ipynb was used for developmenent of the workflow


### Build ETL Pipeline
1. ssh to your EMR cluster.
2. cope script to EMR cluster (scp -i myKey.pem etl.py hadoop@<server>.compute-1.amazonaws.com:~/)
3. exetute script by running spark-submit --master yarn etl.py
