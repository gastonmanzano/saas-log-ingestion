import os
from kafka import KafkaConsumer
from app.config.settings import KAFKA_HOST, PROCESSED_DATA_DIR, KAFKA_TOPIC_NAME
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_timestamp
from datetime import datetime


class SaasConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC_NAME, 
            bootstrap_servers = KAFKA_HOST, 
            auto_offset_reset = 'earliest', 
            value_deserializer = lambda v: v.decode('utf-8'),
            enable_auto_commit = True
        )
        self.spark = SparkSession.builder.master('local[*]').appName("DataTransformation").getOrCreate()


    def run(self):
        for msg in self.consumer:
            filepath = msg.value

            try:
                df = self.spark.read.option('multiLine', True).json(filepath)
                df = df.withColumn("country", col("address.country"))
                df = df.withColumn("full_datetime_str", concat_ws(" ", col("logDate"), col("logInTime.currentTime")))
                df = df.withColumn("timestamp", to_timestamp("full_datetime_str", "yyyy-MM-dd HH:mm:ss"))
                df = df.drop("address", "logDate", "logInTime")
                filename = f"processed_{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}.json"
                output_path = os.path.join(PROCESSED_DATA_DIR, filename)

                df.coalesce(1).write.json(output_path)
                print(f"[INFO] Procesado guardado en {output_path}")
                df.show()
            except FileNotFoundError:
                print(f'[ERROR] Cannot found {filepath}')

            