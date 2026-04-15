CREATE DATABASE IF NOT EXISTS SNFAWSINT;
CREATE SCHEMA IF NOT EXISTS SNFAWSINT.RAW;

SNFAWSINT.RAWCREATE OR REPLACE STORAGE INTEGRATION S3_SNF_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_ALLOWED_LOCATIONS = ('s3://snf-integration-demo/')
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::666278775161:role/snowflake-s3-integration';

  DESC INTEGRATION S3_SNF_INT;

  CREATE OR REPLACE FILE FORMAT S3_INT_CSV_FF
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

--CREATE EXTERNAL STAGE

CREATE OR REPLACE STAGE S3AWS_external_stage
  URL = 's3://snf-integration-demo/'
  STORAGE_INTEGRATION = S3_SNF_INT
  FILE_FORMAT = S3_INT_CSV_FF;

  DESC STAGE S3AWS_external_stage

  LIST @S3AWS_external_stage;

--TABLE 
  CREATE OR REPLACE TABLE SNFAWSINT.RAW.CUSTOMER_FROM_S3
  (ID STRING PRIMARY KEY,
  NAME STRING,
  SEGMENT STRING,
  STATE STRING,
  CITY STRING
  );

--COPY COMMAND 

  COPY INTO SNFAWSINT.RAW.CUSTOMER_FROM_S3
FROM @S3AWS_external_stage/customers.csv
ON_ERROR = 'CONTINUE'; -- Skips rows with errors instead of failing the whole job

SELECT * FROM SNFAWSINT.RAW.CUSTOMER_FROM_S3;

REMOVE @S3AWS_external_stage/customers.csv;

-----
--SNOWPIPE FOR AUTO INGEST

CREATE OR REPLACE PIPE S3_SNF_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO SNFAWSINT.RAW.CUSTOMER_FROM_S3
  FROM  @S3AWS_external_stage
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
  

--Connect AWS to the Pipe
--Snowflake creates a hidden "queue" for every pipe. You ----need to tell S3 to send notifications to that queue.

SHOW PIPES;
--NOTIFICATION
arn:aws:sqs:ap-south-1:514422154735:sf-snowpipe-AIDAXPRPTEXX22ZYQU7HG-HRVc4aP8LJpq_pYZxSn3qA

-- In AWS Console:

-- Go to your S3 Bucket > Properties tab.

-- Scroll down to Event notifications > Create event notification.

-- Event types: Select "All object create events" (e.g., s3:ObjectCreated:*).

-- Destination: Select SQS Queue.

-- Specify SQS Queue: Choose "Enter SQS Queue ARN" and paste the string from Snowflake.

DELETE FROM SNFAWSINT.RAW.CUSTOMER_FROM_S3

SELECT * FROM SNFAWSINT.RAW.CUSTOMER_FROM_S3;

REMOVE @S3AWS_external_stage/customers.csv;

SELECT SYSTEM$PIPE_STATUS('S3_SNF_PIPE');

PIPE WORKING
