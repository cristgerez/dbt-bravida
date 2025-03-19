# Bravida Assigment

### Structure of the solution:

![Bravida Board](https://github.com/user-attachments/assets/9d7c1c37-9c71-44ad-953c-79ecbc998c8d)

### Snowflake
1.I created an External Stage in a schema I created previously. This way I linked the public azure object storage to my Snowflake account azure://azureopendatastorage.blob.core.windows.net/nyctlc/yellow

2.I took a look at the structure of the storage and used the following query to see the structure of the data:

```sql
LIST @"YELLOW_TAXI";
```

```sql
CREATE OR REPLACE TABLE schem_table_test
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@YELLOW_TAXI/',
            FILE_FORMAT => 'PARQUET_FORMAT'
        )
    )
);

```
3. Since the columns tpepPickupDateTime and tpepDropoffDateTime have TIMESTAMP as type and the assigment ask for a casting in dbt I created a table with other types for those columns.
   
```sql
create or replace TABLE RAW_YELLOW_TAXI.PUBLIC.RAW_TAXI_TRIPS (
	"tpepPickupDateTime" VARCHAR(16777216),
	"tpepDropoffDateTime" VARCHAR(16777216),
	"tollsAmount" FLOAT,
	"storeAndFwdFlag" VARCHAR(16777216),
	"paymentType" VARCHAR(16777216),
	"rateCodeId" NUMBER(38,0),
	"vendorID" VARCHAR(16777216),
	"doLocationId" VARCHAR(16777216),
	"startLon" FLOAT,
	"endLat" FLOAT,
	"totalAmount" FLOAT,
	"extra" FLOAT,
	"improvementSurcharge" VARCHAR(16777216),
	"mtaTax" FLOAT,
	"tipAmount" FLOAT,
	"fareAmount" FLOAT,
	"startLat" FLOAT,
	"endLon" FLOAT,
	"tripDistance" FLOAT,
	"passengerCount" NUMBER(38,0),
	"puLocationId" VARCHAR(16777216)
);
```

4. I created a file format for Snowflake to handle the files correctly:
   
```sql
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
TYPE = PARQUET;
```

5. With the table and the file format created I proceeded to copy the info into it with the following Snowflake command.
   
```sql
COPY INTO RAW_YELLOW_TAXI.PUBLIC.RAW_TAXI_TRIPS
FROM @TAXI_TEST.PUBLIC.YELLOW_TAXI/
FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet$';

```

```sql
SELECT *
FROM RAW_YELLOW_TAXI.PUBLIC.RAW_TAXI_TRIPS
LIMIT 10
;
```

### dbt
1. Configured the connections needed [Github, Snowflake]
2. Created a developtment environment and initiated the project
3. Edited some basic configuration in the dbt_project and created some useful folders in models
4. Inside models/staging I created a [schema.yml](https://github.com/cristgerez/dbt-bravida/blob/main/models/staging/schema.yml) where I defined the sources of the data, a staging model and some tests.

   4.b The test where done during the staging phase because the columns tested need to be in the final select in order to run the test
     Instead of doing trip_duration_minutes > 0 I added a test comparing "tpep_dropoff_datetime" and "tpep_pickup_datetime".

   4.c Added a [packages.yml](https://github.com/cristgerez/dbt-bravida/blob/main/packages.yml) and imported 2 packages dbt_utils and dbt_expectations.
5. I created the query for the creaation of the [stagin table](https://github.com/cristgerez/dbt-bravida/blob/main/models/staging/stg_raw_taxi_trips.sql)
   5.b I added the trip_id column using a dbt_utils function called generate_surrogate_key that generates a hash using the content of selected columns
   5.c I renamed all columns to be snake case
   5.d Casted tpep_dropoff_datetime and tpep_pickup_datetime as TIMESTAMP
   5.e Added a deduplication query
6. Once I had my staging phase, I added a [mart folder](https://github.com/cristgerez/dbt-bravida/tree/main/models/mart) with my final with the schema.yml and the [facts_taxi_trips.sql](https://de207.us1.dbt.com/develop/70471823440175/projects/70471823449511) to generate the final table
