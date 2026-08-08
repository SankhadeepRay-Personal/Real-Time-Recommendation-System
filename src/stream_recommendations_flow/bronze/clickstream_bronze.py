from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, to_timestamp, expr
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, TimestampType, FloatType, DoubleType, LongType


@dp.table(
    name = "streamrec.bronze.clickstream_bronze_raw",
    comment = "Raw clickstream data from the website",
)
def clickstream_bronze_raw() -> DataFrame:

    username = "KSIR2NKBB3KPYG3V"
    password = "cfltZxaJlyoC7IW4NU3bN5Skg5WBY6y9KqGwiW4LFys/lcWVkhIZDcwRU9o7SRhQ"
    kafka_bootstrap_servers = "pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092"
    kafka_topic = "clickstream_avro"
    schema_registry_url = "https://psrc-777rw.asia-south2.gcp.confluent.cloud"
    schema_registry_subject = "clickstream_avro-value"

    kafka_config = {
    'kafka.bootstrap.servers': kafka_bootstrap_servers,
    'subscribe': kafka_topic,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    "failOnDataLoss" : "false",
    "kafka.ssl.endpoint.identification.algorithm" :  "https",
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" password="{password}";',
    "startingOffsets" :"earliest"
    }
    
    raw_stream = spark.readStream \
    .format("kafka")  \
    .options(**kafka_config) \
    .load()

    schema_registry_options = {
        "confluent.schema.registry.basic.auth.credentials.source": "USER_INFO",
        "confluent.schema.registry.basic.auth.user.info": "7FQQ2OLILWN5DO7T:cfltD2kScMUaIVA13dJ6DPL15aLN2xwmm1iGAXN/UmD9HRlvpEpxmxXzKnjEXMxQ",
    }

    transformed_df = raw_stream.select(
        from_avro(
            data=col("value"),
            subject=schema_registry_subject,
            schemaRegistryAddress=schema_registry_url,
            options=schema_registry_options
        ).alias("data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("bronze_ingestion_timestamp")
    ).select(
        col("data.*"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        col("bronze_ingestion_timestamp")
    ).withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
    
    return transformed_df