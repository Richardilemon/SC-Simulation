from pyspark.sql import SparkSession, DataFrame
from dotenv import load_dotenv
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import from_json, col


load_dotenv()
secret_key = os.getenv('AWS_SECRET_KEY')
access_key = os.getenv('AWS_ACCESS_KEY')

def main():
    spark = SparkSession.builder \
        .appName("SmartCityStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk-core:1.12.429,"
                "com.amazonaws:aws-java-sdk-kms:1.12.429,"
                "com.amazonaws:aws-java-sdk-s3:1.12.429") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel('WARN')
    
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])
    
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])
    
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("snapshot", StringType(), True)
    ])
    
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", StringType(), True)
    ])
    
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])
    
    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .option('failOnDataLoss', 'false')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes'))
        
    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())
    
    vehicleDf = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDf = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDf = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDf = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDf = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')
    
    query1 = streamWriter(vehicleDf, 's3a://thelemon/checkpoints/vehicle_data',
                          's3a://thelemon/data/vehicle_data')
    query2 = streamWriter(gpsDf, 's3a://thelemon/checkpoints/gps_data',
                          's3a://thelemon/data/gps_data')
    query3 = streamWriter(trafficDf, 's3a://thelemon/checkpoints/traffic_data',
                          's3a://thelemon/data/traffic_data')
    query4 = streamWriter(weatherDf, 's3a://thelemon/checkpoints/weather_data',
                          's3a://thelemon/data/weather_data')
    query5 = streamWriter(emergencyDf, 's3a://thelemon/checkpoints/emergency_data',
                          's3a://thelemon/data/emergency_data')
    
    query5.awaitTermination()
    
if __name__ == "__main__":
    main()
