# Database design and ETL process described.

We need to design a database in Redshift which enable us to run analytical queries despite the size of the database (could handle even Petabytes of data).

On the S3 service, we have the event logs from the application and the song data logs. We want to copy the json contents of these files on Redshift tables staging_events and staging_songs. We need the staging tables in order to put all the data without any filtering applied. 

After that step, we insert the pulled data to the fact and dimension tables of the star schema (songplays, users, artists, songs, times) with filtering and constraints applied.


# Table design (distribution and sort keys)

First of all, our fact table is songplays. This means that this table could possibly grow big despite the fact that its entries are in short right now.

So, in order to be foresighted we need to distribute the fact table based on a key. We noticed that the times table has 8023 entries with the possibility to grow at a big rate in the future. So, we need to distribute the times table based on a key also. The foreign key which joins the two tables (songplays and times) is start_time, so this is a good candidate for the distribution key and sort key for these two tables.

For the rest of the dimension tables the best strategy for distribution among slices, is the AUTO configuration which lets Redshift to determine between (ALL and EVEN) based on their scale. For small tables it uses the ALL distribution style and as the tables grows it uses the EVEN distribution style.



