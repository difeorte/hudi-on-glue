# hudi-on-aws-glue
This is a generic AWS Glue job that uses Apache Hudi over Glue 3.0 or 4.0 to either make a bulk insert, upsert, or delete operation. It is suggested to be used together with the Glue job bookmark.

Required parameters for the job are:

--CDC_FOLDER
--CURATED_BUCKET
--FULL_LOAD_FOLDER
--PARTITION_KEY
--PRECOMBINE_KEY
--PRIMARY_KEY
--RAWZONE_BUCKET
--SOURCE_DB
--SOURCE_TABLE
--TARGET_DB
--TARGET_HUDI_TABLE
--datalake-formats

Parameters description:
--CDC_FOLDER: Amazon S3 path where CDC changes are being replicated by services such as Amazon DMS or any other service that includes the operation column (I, U, D).
--CURATED_BUCKET: Amazon S3 path where the Hudi dataset is located.
--FULL_LOAD_FOLDER: Prefix that should be added to the 
--PARTITION_KEY: Name of the column that will be used as partition key for the hudi table. If no partition is defined, include the word None.
--PRECOMBINE_KEY: Name of the column that will be used as precombine key for the hudi table.
--PRIMARY_KEY: Name of the column that will be used as primary key for the hudi table.
--RAWZONE_BUCKET: Amazon S3 path where the initial load of the data is located. Usually loaded by services such as Amazon DMS with a full load task.
--SOURCE_DB: Name of the source database in case the full load is already in the Glue data catalog. If it is not, input None.
--SOURCE_TABLE: Name of the source table in case the full load is already in the Glue data catalog. If it is not, input None.
--TARGET_DB: Name of the target db where the hudi table will be located.
--TARGET_HUDI_TABLE: Name of the hudi table.
--datalake-formats: Please input the word hudi.
