from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, LongType, BooleanType
from pyspark import pipelines as dp

# task1 : buat schema berdassarkan struktur json yang di stream dari kafka
# task2 : buat delta tabel bronze dan silver , gold nyusul nanti di terminal operations 
# task3 : normalisasi tabel silver

# Schema crane events
crane_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("crane_id", StringType(), True),
    StructField("terminal_id", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("spreader_height_m", DoubleType(), True),
    StructField("trolley_position_m", DoubleType(), True),
    StructField("hoist_speed_mps", DoubleType(), True),
    StructField("cycle_time_sec", LongType(), True),
    StructField("load_cell_kg", LongType(), True),
    StructField("spreader_locked", BooleanType(), True),
    StructField("gps_lat", DoubleType(), True),
    StructField("gps_lng", DoubleType(), True),
    StructField("anomali_type", StringType(), True),
    StructField("ocr", StructType([
        StructField("container_no", StringType(), True),
        StructField("confidence_score", DoubleType(), True),
        StructField("status", StringType(), True)
    ]), True),
    StructField("container_ref", StructType([
        StructField("container_id", StringType(), True),
        StructField("container_number", StringType(), True),
        StructField("container_type", StringType(), True),
        StructField("container_type_name", StringType(), True),
        StructField("container_size", StringType(), True),
        StructField("container_category", StringType(), True),
        StructField("seal_number", StringType(), True)
    ]), True)
])

@dp.table(
    name="crane_events_bronze",
    comment="Raw crane events from Kafka",
    temporary=False
)
def crane_bronze():
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "pkc-921jm.us-east-2.aws.confluent.cloud:9092")
            .option("subscribe", "crane_events")
            .option("startingOffsets", "earliest")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("failOnDataLoss", "false")
            .option("kafka.ssl.endpoint.identification.algorithm", "https")
            .option("kafka.sasl.jaas.config", 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="" password="";')
            .load()
    )

@dp.table(
    name="crane_events_silver",
    comment="Parsed crane events",
    temporary=False
)
def crane_silver():
    return (
        spark.readStream.table("crane_events_bronze")
            .selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_timestamp", "topic")
            .select(
                from_json(col("json"), crane_schema).alias("data"),
                col("kafka_timestamp"),
                col("topic")
            )
            .select(
                to_timestamp(col("data.timestamp")).alias("crane_timestamp"),
                col("data.event_id"),
                col("data.crane_id"),
                col("data.terminal_id"),
                col("data.operation").alias("crane_operation"),
                col("data.spreader_height_m"),
                col("data.trolley_position_m"),
                col("data.hoist_speed_mps"),
                col("data.cycle_time_sec"),
                col("data.load_cell_kg"),
                col("data.spreader_locked"),
                col("data.gps_lat"),
                col("data.gps_lng"),
                col("data.anomali_type"),
                col("data.ocr.container_no").alias("ocr_container_no"),
                col("data.ocr.confidence_score").alias("ocr_confidence_score"),
                col("data.ocr.status").alias("ocr_status"),
                col("data.container_ref.container_id").alias("container_id"),
                col("data.container_ref.container_number").alias("container_number"),
                col("data.container_ref.container_type").alias("container_type"),
                col("data.container_ref.container_type_name").alias("container_type_name"),
                col("data.container_ref.container_size").alias("container_size"),
                col("data.container_ref.container_category").alias("container_category"),
                col("data.container_ref.seal_number").alias("seal_number"),
                
                col("kafka_timestamp"),
                col("topic")
            )
    )
