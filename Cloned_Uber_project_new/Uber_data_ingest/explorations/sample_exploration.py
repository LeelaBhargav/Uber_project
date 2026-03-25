# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

EH_NAMESPACE = "ubereventleel"
EH_NAME = "ubertopic"


#EH_CONN_STR  = spark.conf.get("connection_string")
# Kafka Consumer configuration
EH_CONN_STR = ".."

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : 10000,
  "kafka.session.timeout.ms" : 10000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : "true",
  "startingOffsets"          : "earliest"
}


df = spark.readStream.format("kafka")\
                  .options(**KAFKA_OPTIONS)\
                  .load()

display(df,checkpointLocation = "/Volumes/uber/bronze/my_volume/volume_1/")
