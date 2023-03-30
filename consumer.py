from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import split, size
from pyspark.sql.types import (
    StructType,
    StringType,
    StructField,
    ArrayType
)
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 consumer.py


KAFKA_TOPIC = 'new-testing'
KAFKA_SERVER = 'localhost:9092'
spark = SparkSession.builder.appName('Twitter-Kafka-Spark-GCS').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_SERVER) \
    .option('subscribe', KAFKA_TOPIC) \
    .option('startingOffsets', 'earliest') \
    .load()

MessageSchema = StructType([
    StructField('edit_history_tweet_ids', ArrayType(StringType()), False),
    StructField('id', StringType(), False),
    StructField('text', StringType(), False)
])

df = df.selectExpr('CAST(value AS STRING)')
df = df.withColumn('message', from_json(df.value, MessageSchema)) \
    .select('message.id','message.text')

# Split text column into words and count them
df = df.withColumn('word_count', size(split(df.text, ' ')))

# Below code outputs the console
# df.writeStream.outputMode('append').format('console').start().awaitTermination()

# Below code saves the output to a parquet file on Google Cloud Storage
df.writeStream.outputMode('append').format('parquet') \
    .option("path", "gs://salem_2023/twitter/tweets") \
        .option("checkpointLocation", "gs://salem_2023/twitter/tweets/tmp") \
            .start().awaitTermination()