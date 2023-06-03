from pyspark.sql.types import StringType
import sys
import os
import json

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import concat, col, lit, to_timestamp, dense_rank, desc
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType,BooleanType,DateType

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import boto3
from botocore.exceptions import ClientError

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RAWZONE_BUCKET', 'CURATED_BUCKET', 'FULL_LOAD_FOLDER', 'CDC_FOLDER', 'SOURCE_DB', 'SOURCE_TABLE', 'TARGET_DB', 'TARGET_HUDI_TABLE', 'PRIMARY_KEY', 'PRECOMBINE_KEY', 'PARTITION_KEY'])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet', 'false').getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

logger.info('Initialization.')
glueClient = boto3.client('glue')

logger.info('Fetching configuration.')
region = os.environ['AWS_DEFAULT_REGION']

rawBucketName = args['RAWZONE_BUCKET']
curatedBucketName = args['CURATED_BUCKET']
fullLoadFolder = args['FULL_LOAD_FOLDER']
cdcFolder = args['CDC_FOLDER']

sourceDBName = args['SOURCE_DB']
sourceTableName = args['SOURCE_TABLE']

targetDBName = args['TARGET_DB']
targetTableName = args['TARGET_HUDI_TABLE']

primaryKey = args['PRIMARY_KEY']
precombineKey = args['PRECOMBINE_KEY']
partitionKey = args['PARTITION_KEY']
if partitionKey == "None":
    partitionKey = None

if rawBucketName == None or curatedBucketName == None or sourceDBName == None or sourceTableName == None or targetDBName == None or targetTableName == None or primaryKey == None or precombineKey == None:
    raise Exception("Please input the required job parameters")

rawS3TablePath = 's3://' + rawBucketName + fullLoadFolder
cdcRawS3TablePath = 's3://' + rawBucketName + cdcFolder
curatedS3TablePathPrefix = 's3://' + curatedBucketName + '/hudi/'

hudiStorageType = 'CoW'

dropColumnList = ['db','table_name','Op']
      
logger.info('Processing starts.')

spark.sql('CREATE DATABASE IF NOT EXISTS ' + targetDBName)

isTableExists = False
isPrimaryKey = False
isPrecombineKey = False
isPartitionKey = False

try:
    glueClient.get_table(DatabaseName=targetDBName,Name=targetTableName)
    isTableExists = True
    logger.info(targetDBName + '.' + targetTableName + ' exists.')
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityNotFoundException':
        isTableExists = False
        logger.info(targetDBName + '.' + targetTableName + ' does not exist. Table will be created.')

if primaryKey is None:
    logger.info('Primary key not found. An append only glueparquet table will be created.')
else:
    isPrimaryKey = True
    logger.info('Primary key:' + primaryKey)

if precombineKey is None:
    logger.info('Precombine key not found. This is a requirement for Hudi')
else:
    isPrecombineKey = True
    logger.info('Precombine key:' + precombineKey)
    
if partitionKey is None:
    logger.info('Partition key not found. Partitions will not be created.')
else:
    isPartitionKey = True
    logger.info('Partition key:' + partitionKey)
    
# Reads the raw zone table and writes to HUDI table
try:
    inputDyf = glueContext.create_dynamic_frame_from_options(connection_type = 's3', connection_options = {'paths': [rawS3TablePath], 'groupFiles': 'none', 'recurse':True}, format = 'csv', format_options={'withHeader':True}, transformation_ctx = "inputDyfull")
    
    inputDf = inputDyf.toDF()
    if inputDf.count() > 0:
        # Ensure timestamp is in HUDI timestamp format
        inputDf = inputDyf.toDF().withColumn(precombineKey, to_timestamp(col(precombineKey))).withColumn(primaryKey, col(primaryKey).cast(IntegerType()))

    logger.info('Total record count in raw table = ' + str(inputDyf.count()))

    targetPath = curatedS3TablePathPrefix + '/' + targetDBName + '/' + targetTableName

    morConfig = {
        'hoodie.datasource.write.table.type': 'MERGE_ON_READ', 
        'hoodie.compact.inline': 'false', 
        'hoodie.compact.inline.max.delta.commits': 20, 
        'hoodie.parquet.small.file.limit': 0
    }

    commonConfig = {
        'className' : 'org.apache.hudi', 
        'hoodie.datasource.hive_sync.use_jdbc':'false', 
        'hoodie.datasource.write.precombine.field': precombineKey, 
        'hoodie.datasource.write.recordkey.field': primaryKey, 
        'hoodie.table.name': targetTableName, 
        'hoodie.datasource.hive_sync.database': targetDBName, 
        'hoodie.datasource.hive_sync.table': targetTableName, 
        'hoodie.datasource.hive_sync.enable': 'true'
    }

    partitionDataConfig = {
        'hoodie.datasource.write.partitionpath.field': partitionKey, 
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor', 
        'hoodie.datasource.hive_sync.partition_fields': partitionKey
    }
                    
    unpartitionDataConfig = {
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor', 
        'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'
    }
    
    incrementalConfig = {
        'hoodie.upsert.shuffle.parallelism': 16, 
        'hoodie.datasource.write.operation': 'upsert', 
        'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS', 
        'hoodie.cleaner.commits.retained': 10
    }
    
    initLoadConfig = {
        'hoodie.bulkinsert.shuffle.parallelism': 10, 
        'hoodie.datasource.write.operation': 'bulk_insert'
    }
    
    deleteDataConfig = {
        'hoodie.datasource.write.payload.class': 'org.apache.hudi.common.model.EmptyHoodieRecordPayload'
    }

    if(hudiStorageType == 'MoR'):
        commonConfig = {**commonConfig, **morConfig}
        logger.info('MoR config appended to commonConfig.')
    
    combinedConf = {}

    # HUDI require us to provide a primaryKey and precombineKey
    if isPrimaryKey and isPrecombineKey:
        logger.info('Going with Hudi.')
        if(isTableExists):
            logger.info('Incremental load.')
            inputDyf = glueContext.create_dynamic_frame_from_options(connection_type = 's3', connection_options = {'paths': [cdcRawS3TablePath], 'groupFiles': 'none', 'recurse':True}, format = 'csv', format_options={'withHeader':True}, transformation_ctx = "inputDyfinc")
            inputDf = inputDyf.toDF()
            if inputDf.count() > 0:
                logger.info('There is incremental data')
                # Ensure timestamp is in HUDI timestamp format
                inputDf = inputDyf.toDF().withColumn(precombineKey, to_timestamp(col(precombineKey))).withColumn(primaryKey, col(primaryKey).cast(IntegerType()))
    
                # ensure only latest updates are kept for each record using descending timestamp order
                w = Window.partitionBy(primaryKey).orderBy(desc(precombineKey))
                inputDf = inputDf.withColumn('Rank',dense_rank().over(w))
                inputDf = inputDf.filter(inputDf.Rank == 1).drop(inputDf.Rank)
    
                outputDf = inputDf.filter("Op != 'D'").drop(*dropColumnList)
    
                if outputDf.count() > 0:
                    logger.info('Upserting data. Updated row count = ' + str(outputDf.count()))
                    if (isPartitionKey):
                        logger.info('Writing to partitioned Hudi table.')
                        outputDf = outputDf.withColumn(partitionKey,concat(lit(partitionKey+'='),col(partitionKey)))
                        combinedConf = {**commonConfig, **partitionDataConfig, **incrementalConfig}
                        outputDf.write.format('hudi').options(**combinedConf).mode('Append').save(targetPath)
                    else:
                        logger.info('Writing to unpartitioned Hudi table.')
                        combinedConf = {**commonConfig, **unpartitionDataConfig, **incrementalConfig}
                        outputDf.write.format('hudi').options(**combinedConf).mode('Append').save(targetPath)
                
                outputDf_deleted = inputDf.filter("Op = 'D'").drop(*dropColumnList)
    
                if outputDf_deleted.count() > 0:
                    logger.info('Some data got deleted.')
                    if (isPartitionKey):
                        logger.info('Deleting from partitioned Hudi table.')
                        outputDf_deleted = outputDf_deleted.withColumn(partitionKey,concat(lit(partitionKey+'='),col(partitionKey)))
                        combinedConf = {**commonConfig, **partitionDataConfig, **incrementalConfig, **deleteDataConfig}
                        outputDf_deleted.write.format('hudi').options(**combinedConf).mode('Append').save(targetPath)
                    else:
                        logger.info('Deleting from unpartitioned Hudi table.')
                        combinedConf = {**commonConfig, **unpartitionDataConfig, **incrementalConfig, **deleteDataConfig}
                        outputDf_deleted.write.format('hudi').options(**combinedConf).mode('Append').save(targetPath)
            else:
                logger.info('There is no incremental data')
        else:
            outputDf = inputDf.drop(*dropColumnList)
            if outputDf.count() > 0:
                logger.info('Inital load.')
                if (isPartitionKey):
                    logger.info('Writing to partitioned Hudi table.')
                    outputDf = outputDf.withColumn(partitionKey,concat(lit(partitionKey+'='),col(partitionKey)))
                    combinedConf = {**commonConfig, **partitionDataConfig, **initLoadConfig}
                    outputDf.write.format('hudi').options(**combinedConf).mode('Overwrite').save(targetPath)
                else:
                    logger.info('Writing to unpartitioned Hudi table.')
                    combinedConf = {**commonConfig, **unpartitionDataConfig, **initLoadConfig}
                    outputDf.write.format('hudi').options(**combinedConf).mode('Overwrite').save(targetPath)
    else:
        logger.info('A primary key and precombine key must be defined.')
except BaseException as e:
    logger.info('An error occurred while processing table ' + targetDBName + '.' + targetTableName + '. Please check the error logs...')
    print(e)
