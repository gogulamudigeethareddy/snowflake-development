use role sysadmin;

CREATE WAREHOUSE EXERCISE_WH
WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 600  -- automatically suspend the virtual warehouse after 10 minutes of not being used
AUTO_RESUME = TRUE 
COMMENT = 'This is a virtual warehouse of size X-SMALL that can be used to process queries.';

DROP WAREHOUSE EXERCISE_WH;

CREATE DATABASE EXERCISE_DB;

Create or replace table customers
(
ID INT,
first_name varchar,
last_name varchar,
email varchar,
age int,
city varchar
);

COPY INTO CUSTOMERS
FROM s3://snowflake-assignments-mc/gettingstarted/customers.csv
FILE_FORMAT = (TYPE = 'CSV'
               FIELD_DELIMITER = ','
               SKIP_HEADER = 1);

SELECT * FROM CUSTOMERS;

CREATE STAGE customers_stage
url = 's3://snowflake-assignments-mc/loadingdata/'
file_format = (type = 'csv' 
               field_delimiter = ','
               skip_header = 1);

list @customers_stage;  

DESC STAGE customers_stage;

copy into EXERCISE_DB.PUBLIC.CUSTOMERS
from @EXERCISE_DB.PUBLIC.CUSTOMERS_STAGE
file_format = (type = 'csv' field_delimiter = ';' skip_header = 1);

select count(*) from EXERCISE_DB.PUBLIC.CUSTOMERS;

create or replace file format new_file_format
type = 'csv',
field_delimiter = '|'
skip_header = 1;

create or replace stage new_stage
url = s3://snowflake-assignments-mc/fileformat/
file_format = (format_name = new_file_format);

list @new_stage;

copy into customers
from @new_stage;

Create or replace table customers
(
  customer_id int,
  first_name varchar(50),
  last_name varchar(50),
  email varchar(50),
  age int,
  department varchar(50)
)

create or replace stage copy_options_stage
url = 's3://snowflake-assignments-mc/copyoptions/example1';

list @copy_options_stage;

create or replace file format copy_options_file_format
TYPE = CSV
FIELD_DELIMITER=','
SKIP_HEADER=1;

copy into customers
from @copy_options_stage
file_format = copy_options_file_format
validation_mode = return_errors;

copy into customers
from @copy_options_stage
file_format = copy_options_file_format
on_error = 'continue';

select * from customers;

Create or replace table employee
(
customer_id int,
first_name varchar(50),
last_name varchar(50),
email varchar(50),
age int,
department varchar(50)
);

create or replace stage copy_options2_stage
url = 's3://snowflake-assignments-mc/copyoptions/example2';

create or replace file format copy_options2_file_format
TYPE = CSV
FIELD_DELIMITER=','
SKIP_HEADER=1;

copy into customers
from @copy_options2_stage
file_format = copy_options2_file_format
validation_mode = return_errors

copy into customers
from @copy_options2_stage
file_format = copy_options2_file_format
truncatecolumns = TRUE;

select * from employee;

create or replace stage unstructured_data_stage
url = 's3://snowflake-assignments-mc/unstructureddata/';

create or replace file format unstructured_data_file_format
type = 'json';

create or replace table JSON_RAW
(
Raw variant
);

copy into JSON_RAW
from @unstructured_data_stage
file_format = unstructured_data_file_format;

select * from json_raw;

select raw:first_name, raw:last_name, raw:Skills from json_raw;

create or replace table json_processed as
(
select $1:first_name::string as first_name, 
       $1:last_name::string as last_name, 
       $1:Skills[0]::string as skills_1, 
       $1:Skills[1]::string as skills_2
from json_raw
);

select * from json_processed;

USE ROLE ACCOUNTDMIN;
USE DATABASE DEMO_DB;
USE WAREHOUSE COMPUTE_WH;
 
CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.PART
AS
SELECT * FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PART";
 
SELECT * FROM PART
ORDER BY P_MFGR DESC;

UPDATE DEMO_DB.PUBLIC.PART
SET P_MFGR='Manufacturer#CompanyX'
WHERE P_MFGR='Manufacturer#5';
 
----> Note down query id here:
 
SELECT * FROM PART
ORDER BY P_MFGR DESC;

select * from part at (offset => -2*60);

select * from part before (statement => '01b60cf5-0001-f732-0006-35720003e1fa');

CREATE DATABASE TIMETRAVEL_EXERCISE;
CREATE SCHEMA TIMETRAVEL_EXERCISE.COMPANY_X;

CREATE TABLE CUSTOMER AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
LIMIT 500;

DROP SCHEMA TIMETRAVEL_EXERCISE.COMPANY_X;

undrop schema TIMETRAVEL_EXERCISE.COMPANY_X;
 
USE ROLE ACCOUNTDMIN;
USE DATABASE DEMO_DB;
USE WAREHOUSE COMPUTE_WH;
 
CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.SUPPLIER
AS
SELECT * FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."SUPPLIER";

CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.SUPPLIER_CLONE
CLONE DEMO_DB.PUBLIC.SUPPLIER;

UPDATE SUPPLIER_CLONE
SET S_PHONE='###';

QUERY_ID = '01b61165-0001-f732-0006-35720003e4d6';

CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.SUPPLIER_CLONE_UPDATED
CLONE DEMO_DB.PUBLIC.SUPPLIER_CLONE before (STATEMENT => '01b61165-0001-f732-0006-35720003e4d6');

DROP TABLE DEMO_DB.PUBLIC.SUPPLIER;

DROP TABLE DEMO_DB.PUBLIC.SUPPLIER_CLONE;

select * from DEMO_DB.PUBLIC.SUPPLIER_CLONE_UPDATED;


CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.CUSTOMER_ADDRESS_SAMPLE_5 AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER_ADDRESS
SAMPLE ROW(5) SEED(2);

CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.CUSTOMER_SAMPLE_1 AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER
SAMPLE SYSTEM(1) SEED(2);

SELECT * FROM DEMO_DB.PUBLIC.CUSTOMER_SAMPLE_1;


SELECT 
AVG(PS_SUPPLYCOST) as PS_SUPPLYCOST_AVG,
AVG(PS_AVAILQTY) as PS_AVAILQTY_AVG,
MAX(PS_COMMENT) as PS_COMMENT_MAX
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.PARTSUPP;

CREATE MATERIALIZED VIEW MAT_VIEW AS
SELECT 
AVG(PS_SUPPLYCOST) as PS_SUPPLYCOST_AVG,
AVG(PS_AVAILQTY) as PS_AVAILQTY_AVG,
MAX(PS_COMMENT) as PS_COMMENT_MAX
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.PARTSUPP;


USE DEMO_DB;
USE ROLE ACCOUNTADMIN;
 
-- Prepare table --
create or replace table customers(
  id number,
  full_name varchar, 
  email varchar,
  phone varchar,
  spent number,
  create_date DATE DEFAULT CURRENT_DATE);
 
 
-- insert values in table --
insert into customers (id, full_name, email,phone,spent)
values
  (1,'Lewiss MacDwyer','lmacdwyer0@un.org','262-665-9168',140),
  (2,'Ty Pettingall','tpettingall1@mayoclinic.com','734-987-7120',254),
  (3,'Marlee Spadazzi','mspadazzi2@txnews.com','867-946-3659',120),
  (4,'Heywood Tearney','htearney3@patch.com','563-853-8192',1230),
  (5,'Odilia Seti','oseti4@globo.com','730-451-8637',143),
  (6,'Meggie Washtell','mwashtell5@rediff.com','568-896-6138',600);
 
 
-- set up roles
CREATE OR REPLACE ROLE ANALYST_MASKED;
CREATE OR REPLACE ROLE ANALYST_FULL;
 
-- grant select on table to roles
GRANT SELECT ON TABLE DEMO_DB.PUBLIC.CUSTOMERS TO ROLE ANALYST_MASKED;
GRANT SELECT ON TABLE DEMO_DB.PUBLIC.CUSTOMERS TO ROLE ANALYST_FULL;
 
GRANT USAGE ON SCHEMA DEMO_DB.PUBLIC TO ROLE ANALYST_MASKED;
GRANT USAGE ON SCHEMA DEMO_DB.PUBLIC TO ROLE ANALYST_FULL;
 
-- grant warehouse access to roles
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_MASKED;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_FULL;
 
-- assign roles to a user
GRANT ROLE ANALYST_MASKED TO USER <YOUR-USER-NAME>;
GRANT ROLE ANALYST_FULL TO USER <YOUR-USER-NAME>;
               





               