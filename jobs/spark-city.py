from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from config import configuration
from pyspark.sql.functions import from_json, col
from pyspark.sql.dataframe import DataFrame


def main():
    # --- SPARK SESSION CONFIGURATION (FIXED FOR S3A KERBEROS ISSUE) ---
    spark = SparkSession.builder.appName("SmartCityStreaming")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,"
                                     "org.apache.hadoop:hadoop-aws:3.3.2,"
                                     "com.amazonaws:aws-java-sdk:1.11.469")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
    .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .config("spark.hadoop.fs.s3a.delegation.token.binding", "") \
    .getOrCreate()
    # ------------------------------------------------------------------

    # Adjust the log level to minimize the console output on the executers
    spark.sparkContext.setLogLevel('WARN')

    # vehicle schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("maker", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fueltype", StringType(), True),
    ])

    # gps schema
    gpsSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('speed', DoubleType(), True),
        StructField('direction', StringType(), True),
        StructField('vehicleType', StringType(), True),
    ])

    # traffic camera schema
    trafficSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('cameraId', StringType(), True),
        StructField('location', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('snapshot', StringType(), True),
    ])

    # weather data schema
    weatherSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('location', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('temperature', IntegerType(), True),
        StructField('weatherCondition', StringType(), True),
        StructField('precipitation', DoubleType(), True),
        StructField('windspeed', DoubleType(), True),
        StructField('humidity', IntegerType(), True),
        StructField('airQualityIndex', DoubleType(), True),
    ])

    # Emergency schema
    EmergencySchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('incidentId', StringType(), True),
        StructField('incident', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('status', StringType(), True),
        StructField("description", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream.format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)') # Corrected 'values' to 'value'
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')
            )

    def streamWriter(input_df: DataFrame, checkpointFolder, output):
        # Added return to make sure the StreamingQuery is returned
        return (input_df.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())

    # Read streams
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', EmergencySchema).alias('emergency')

    # Start writing streams to S3
    query1 = streamWriter(vehicleDF, 's3a://aditya-spark-streaming-data/checkpoints/vehicle_data', 's3a://aditya-spark-streaming-data/data/vehicle_data')
    query2 = streamWriter(gpsDF, 's3a://aditya-spark-streaming-data/checkpoints/gps_data', 's3a://aditya-spark-streaming-data/data/gps_data')
    query3 = streamWriter(trafficDF, 's3a://aditya-spark-streaming-data/checkpoints/traffic_data', 's3a://aditya-spark-streaming-data/data/traffic_data')
    query4 = streamWriter(weatherDF, 's3a://aditya-spark-streaming-data/checkpoints/weather_data', 's3a://aditya-spark-streaming-data/data/weather_data')
    query5 = streamWriter(emergencyDF, 's3a://aditya-spark-streaming-data/checkpoints/emergency_data', 's3a://aditya-spark-streaming-data/data/emergency_data')

    # Wait for the streams to terminate
    # It's better to wait for all queries to finish if they are all running
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()