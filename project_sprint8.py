import os

from dotenv import load_dotenv

load_dotenv()

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

TOPIC_IN = os.getenv("TOPIC_IN")
TOPIC_OUT = os.getenv("TOPIC_OUT")


spark_jars_packages = ",".join(
    [
        os.getenv("SPARK_SQL_KAFKA_JAR"),
        os.getenv("POSTRESQL_JAR"),
    ]
)

kafka_security_options = {
    "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "kafka.security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
    "kafka.sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
    "kafka.sasl.jaas.config": os.getenv("KAFKA_SASL_JAAS_CONFIG"),
}

postgres_read_options = {
    "url": os.getenv("POSTGRES_READ_URL"),
    "driver": os.getenv("POSTGRES_DRIVER"),
    "user": os.getenv("POSTGRES_READ_USER"),
    "password": os.getenv("POSTGRES_READ_PASSWORD"),
    "dbtable": "subscribers_restaurants",
}

postgres_write_options = {
    "url": os.getenv("POSTGRES_WRITE_URL"),
    "driver": os.getenv("POSTGRES_DRIVER"),
    "user": os.getenv("POSTGRES_WRITE_USER"),
    "password": os.getenv("POSTGRES_WRITE_PASSWORD"),
    "dbtable": "subscribers_feedback",
}


def foreach_batch_function(df, epoch_id):

    df.withColumn("feedback", F.lit(None).cast(StringType())).write.mode(
        "append"
    ).format("jdbc").options(**postgres_write_options).save()

    df.select(
        F.to_json(
            F.struct(
                "restaurant_id",
                "adv_campaign_id",
                "adv_campaign_content",
                "adv_campaign_owner",
                "adv_campaign_owner_contact",
                "adv_campaign_datetime_start",
                "adv_campaign_datetime_end",
                "datetime_created",
                "client_id",
                "trigger_datetime_created",
            )
        ).alias("value")
    ).write.mode("append").format("kafka").options(**kafka_security_options).option(
        "topic", TOPIC_OUT
    ).option(
        "truncate", False
    )


spark = (
    SparkSession.builder.master("local[*]")
    .appName("RestaurantSubscribeStreamingService")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.jars.packages", spark_jars_packages)
    .getOrCreate()
)


restaurant_read_stream_df = (
    spark.readStream.format("kafka")
    .options(**kafka_security_options)
    .option("subscribe", TOPIC_IN)
    .load()
)


incomming_message_schema = StructType(
    [
        StructField("restaurant_id", StringType()),
        StructField("adv_campaign_id", StringType()),
        StructField("adv_campaign_content", StringType()),
        StructField("adv_campaign_owner", StringType()),
        StructField("adv_campaign_owner_contact", StringType()),
        StructField("adv_campaign_datetime_start", LongType()),
        StructField("adv_campaign_datetime_end", LongType()),
        StructField("datetime_created", LongType()),
    ]
)


current_timestamp_utc = int(round(datetime.utcnow().timestamp()))


filtered_read_stream_df = (
    restaurant_read_stream_df.withColumn("value", F.col("value").cast(StringType()))
    .withColumn("event", F.from_json(F.col("value"), incomming_message_schema))
    .selectExpr("event.*")
    .withWatermark("adv_campaign_datetime_end", "10 minutes")
    .groupBy(F.window("adv_campaign_datetime_end", "10 minutes"), "restaurant_id")
    .where(
        f"adv_campaign_datetime_start >= {current_timestamp_utc} and adv_campaign_datetime_end <= {current_timestamp_utc}"
    )
)


subscribers_restaurant_df = (
    spark.read.format("jdbc").options(**postgres_read_options).load()
)


result_df = filtered_read_stream_df.join(
    subscribers_restaurant_df, on="restaurant_id", how="inner"
).withColumn("trigger_datetime_created", F.lit(f"{current_timestamp_utc}"))


result_df.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
