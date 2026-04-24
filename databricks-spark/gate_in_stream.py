from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark import pipelines as dp

# schema gate in and gate out sama , 
# tapi file nya disini saya pisah biar scalable , modular,
# readable dan maintainable

# schema gate_in stream
gate_in_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("gate_id", StringType(), True),
    StructField("terminal_id", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("truck", StructType([
        StructField("plate_no",StringType(), True),
        StructField("rfid_tag",StringType(), True),
        StructField("rfid_read_success",BooleanType(), True),
        StructField("driver_id",StringType(), True),
        StructField("do_number",StringType(), True)
    ]), True),
    StructField("ocr", StructType([
        StructField("container_no", StringType(), True),
        StructField("confidence_score", DoubleType(), True),
        StructField("status", StringType(), True)
    ]), True),
    StructField("weight",StructType([
        StructField("gross_weight_kg",IntegerType(), True),
        StructField("tare_weight_kg",IntegerType(), True),
        StructField("nett_weight_kg",IntegerType(), True)
    ]), True),
    StructField("container_ref", StructType([
        StructField("container_id",StringType(), True),
        StructField("container_number",StringType(), True),
        StructField("container_type",StringType(), True),
        StructField("container_type_name",StringType(), True),
        StructField("container_size",StringType(), True),
        StructField("container_category",StringType(), True),
        StructField("seal_number",StringType(), True)
    ]), True),
    StructField("status", StringType(), True),
    StructField("anomali_type", StringType(), True)
])

# buat delta table bronze utk gate in events
@dp.table(
    name="gate_in_events_bronze",
    comment="Raw gate in container events from Kafka",
    temporary=False
)
def gate_in_bronze():
    return(
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers","pkc-921jm.us-east-2.aws.confluent.cloud:9092")
            .option("subscribe","gate_in_events")
            .option("startingOffsets","earliest")
            .option("kafka.security.protocol","SASL_SSL")
            .option("kafka.sasl.mechanism","PLAIN")
            .option("failOnDataLoss","false")
            .option("kafka.sasl.jaas.config",'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="" password="";')
            .load()
    )


# buat delta table untuk silver gate in events
@dp.table(
    name="gate_in_events_silver",
    comment="Transformed gate in container events with flattened structure for dwell time and status tracking",
    temporary=False
)
def gate_in_silver():
    return(
        spark.readStream
            .table("gate_in_events_bronze")
            .selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_timestamp", "topic")
            .select(
                from_json(col("json"), gate_in_schema).alias("data"),
                col("kafka_timestamp"),
                col("topic")
            )
            .select(
                col("data.event_id"),
                to_timestamp(col("data.timestamp")).alias("gate_in_timestamp"),
                col("data.gate_id"),
                col("data.terminal_id"),
                col("data.direction"),
                col("data.truck.plate_no").alias("truck_plate_no"),
                col("data.truck.rfid_tag").alias("truck_rfid_tag"),
                col("data.truck.rfid_read_success").alias("truck_rfid_success"),
                col("data.truck.driver_id").alias("driver_id"),
                col("data.truck.do_number").alias("do_number"),
                col("data.ocr.container_no").alias("ocr_container_no"),
                col("data.ocr.confidence_score").alias("ocr_confidence_score"),
                col("data.ocr.status").alias("ocr_status"),
                col("data.weight.gross_weight_kg").alias("gross_weight_kg"),
                col("data.weight.tare_weight_kg").alias("tare_weight_kg"),
                col("data.weight.nett_weight_kg").alias("nett_weight_kg"),
                col("data.container_ref.container_id"),
                col("data.container_ref.container_number"),
                col("data.container_ref.container_type"),
                col("data.container_ref.container_type_name"),
                col("data.container_ref.container_size"),
                col("data.container_ref.container_category"),
                col("data.container_ref.seal_number"),
                col("data.status").alias("gate_status"),
                col("data.anomali_type"),
                col("kafka_timestamp"),
                col("topic"),
                
                current_timestamp().alias("silver_timestamp")
            )
    )
