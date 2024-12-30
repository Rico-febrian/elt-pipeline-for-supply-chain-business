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
SELECT * FROM source_db.data_source.customer;

CREATE OR REPLACE TABLE staging.supplier AS
SELECT * FROM source_db.data_source.supplier;

CREATE OR REPLACE TABLE staging.orders AS
SELECT * FROM source_db.data_source.orders;

CREATE OR REPLACE TABLE staging.lineitem AS
SELECT * FROM source_db.data_source.lineitem;

CREATE OR REPLACE TABLE staging.part AS
SELECT * FROM source_db.data_source.part;

CREATE OR REPLACE TABLE staging.partsupp AS
SELECT * FROM source_db.data_source.partsupp;

CREATE OR REPLACE TABLE staging.nation AS
SELECT * FROM source_db.data_source.nation;

CREATE OR REPLACE TABLE staging.region AS
SELECT * FROM source_db.data_source.region;