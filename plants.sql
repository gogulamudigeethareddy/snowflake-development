create or replace TABLE GARDEN_PLANTS.VEGGIES.ROOT_DEPTH (
	ROOT_DEPTH_ID NUMBER(1,0),
	ROOT_DEPTH_CODE VARCHAR(1),
	ROOT_DEPTH_NAME VARCHAR(7),
	UNIT_OF_MEASURE VARCHAR(2),
	RANGE_MIN NUMBER(2,0),
	RANGE_MAX NUMBER(2,0)
);

INSERT INTO ROOT_DEPTH(
        ROOT_DEPTH_ID,
        ROOT_DEPTH_CODE,
        ROOT_DEPTH_NAME,
        UNIT_OF_MEASURE,
        RANGE_MIN,
        RANGE_MAX
)
VALUES(
1,
'S',
'Shallow',
'cm',
30,
45
);

INSERT INTO ROOT_DEPTH(
        ROOT_DEPTH_ID,
        ROOT_DEPTH_CODE,
        ROOT_DEPTH_NAME,
        UNIT_OF_MEASURE,
        RANGE_MIN,
        RANGE_MAX
)
VALUES(
2,
'M',
'Medium',
'cm',
45,
60
);

INSERT INTO ROOT_DEPTH(
        ROOT_DEPTH_ID,
        ROOT_DEPTH_CODE,
        ROOT_DEPTH_NAME,
        UNIT_OF_MEASURE,
        RANGE_MIN,
        RANGE_MAX
)
VALUES(
3,
'D',
'Deep',
'cm',
60,
90
);

SELECT * FROM ROOT_DEPTH;

create table garden_plants.veggies.vegetable_details(
plant_name varchar(25),
root_depth_code varchar(1)
);

DELETE FROM VEGETABLE_DETAILS
WHERE PLANT_NAME = 'Spinach'and ROOT_DEPTH_CODE = 'D';

SELECT * FROM VEGETABLE_DETAILS;

create file format garden_plants.veggies.PIPECOLSEP_ONEHEADROW 
    TYPE = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    FIELD_DELIMITER = '|' --pipes as column separators
    SKIP_HEADER = 1 --one header row to skip
    ;

create file format garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW 
    TYPE = 'CSV'--csv for comma separated files
    SKIP_HEADER = 1 --one header row  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them;

SHOW FILE FORMATS IN ACCOUNT;

list @like_a_Window_into_an_s3_bucket;

create or replace table vegetable_details_soil_type
( plant_name varchar(25)
 ,soil_type number(1,0)
);

copy into vegetable_details_soil_type
from @util_db.public.like_a_window_into_an_s3_bucket
files = ( 'VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.PIPECOLSEP_ONEHEADROW );

--The data in the file, with no FILE FORMAT specified
select $1
from @util_db.public.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv;

--Same file but with one of the file formats we created earlier  
select $1, $2, $3
from @util_db.public.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW);

--Same file but with the other file format we created earlier
select $1, $2, $3
from @util_db.public.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.PIPECOLSEP_ONEHEADROW );

create or replace file format garden_plants.veggies.L8_CHALLENGE_FF 
    TYPE = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    FIELD_DELIMITER = '\t' --pipes as column separators
    SKIP_HEADER = 1 --one header row to skip
    ;

--Same file but with the other file format we created earlier
select $1, $2, $3
from @util_db.public.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.L8_CHALLENGE_FF);

create or replace table LU_SOIL_TYPE(
SOIL_TYPE_ID number,	
SOIL_TYPE varchar(15),
SOIL_DESCRIPTION varchar(75)
 );

COPY INTO LU_SOIL_TYPE
from @util_db.public.like_a_window_into_an_s3_bucket
files = ('LU_SOIL_TYPE.tsv')
file_format = (format_name = garden_plants.veggies.L8_CHALLENGE_FF);

create or replace table VEGETABLE_DETAILS_PLANT_HEIGHT (
PLANT_NAME VARCHAR(50),
UOM VARCHAR(1),
Low_End_Of_Range number(2),
High_End_Of_Range number(2)
);

create or replace file format garden_plants.veggies.PLANT_HEIGHT_FILE_FORMAT
    TYPE = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    FIELD_DELIMITER = ',' --pipes as column separators
    SKIP_HEADER = 1 --one header row to skip
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them;
    ;
    
COPY INTO VEGETABLE_DETAILS_PLANT_HEIGHT
from @util_db.public.like_a_window_into_an_s3_bucket
files = ('veg_plant_height.csv')
file_format = (format_name = garden_plants.veggies.plant_height_file_format);


