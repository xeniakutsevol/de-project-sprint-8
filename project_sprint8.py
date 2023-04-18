import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

TOPIC_IN = "student.topic.cohort8.xeniakutsevol"
TOPIC_OUT = "student.topic.cohort8.xeniakutsevol.out"


spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

kafka_security_options = {
    "kafka.bootstrap.servers": "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
}

postgres_read_options = {
    "url": "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de",
    "driver": "org.postgresql.Driver",
    "user": "student",
    "password": "de-student",
    "dbtable": "subscribers_restaurants",
}

postgres_write_options = {
    "url": "jdbc:postgresql://localhost:5432/de",
    "driver": "org.postgresql.Driver",
    "user": "jovyan",
    "password": "jovyan",
    "dbtable": "subscribers_feedback",
}


def foreach_batch_function(df, epoch_id):

    df.persist()

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

    df.unpersist()


spark = (
    SparkSession.builder.appName("RestaurantSubscribeStreamingService")
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
