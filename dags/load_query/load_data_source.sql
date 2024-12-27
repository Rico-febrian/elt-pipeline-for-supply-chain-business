USE DATABASE warehouse_db;

TRUNCATE TABLE staging.customer; 
TRUNCATE TABLE staging.supplier; 
TRUNCATE TABLE staging.orders; 
TRUNCATE TABLE staging.lineitem; 
TRUNCATE TABLE staging.part;
TRUNCATE TABLE staging.partsupp;
TRUNCATE TABLE staging.nation;
TRUNCATE TABLE staging.region;   

CREATE OR REPLACE TABLE staging.customer AS
SELECT * FROM snowflake_sample_data.tpch_sf1.customer;

CREATE OR REPLACE TABLE staging.supplier AS
SELECT * FROM snowflake_sample_data.tpch_sf1.supplier;

CREATE OR REPLACE TABLE staging.orders AS
SELECT * FROM snowflake_sample_data.tpch_sf1.orders;

CREATE OR REPLACE TABLE staging.lineitem AS
SELECT * FROM snowflake_sample_data.tpch_sf1.lineitem;

CREATE OR REPLACE TABLE staging.part AS
SELECT * FROM snowflake_sample_data.tpch_sf1.part;

CREATE OR REPLACE TABLE staging.partsupp AS
SELECT * FROM snowflake_sample_data.tpch_sf1.partsupp;

CREATE OR REPLACE TABLE staging.nation AS
SELECT * FROM snowflake_sample_data.tpch_sf1.nation;

CREATE OR REPLACE TABLE staging.region AS
SELECT * FROM snowflake_sample_data.tpch_sf1.region;