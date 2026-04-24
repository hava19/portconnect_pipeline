from pyspark.sql.functions import col, datediff, hour, minute, round as spark_round, concat_ws, lit, current_timestamp , countDistinct, approx_count_distinct, window
from pyspark.sql.functions import to_date, count, sum as spark_sum, avg, max as spark_max, min as spark_min
from pyspark.sql.functions import when, coalesce, unix_timestamp, expr
from pyspark import pipelines as dp


# buat delta table export dan import , ambil data dari silver table , dan ambil insight dwell time kontainer
@dp.table(
    name="gold_container_export_flow_streaming",
    comment="Complete container export flow tracking with yard dwell time calculation",
    cluster_by=["terminal_id","container_number"]
)
@dp.expect("valid_container_number", "container_number IS NOT NULL")
@dp.expect("valid_dwell_time", "yard_dwell_time_hours >= 0 OR yard_dwell_time_hours IS NULL")
@dp.expect("valid_operation", "crane_operation IN ('LOAD', 'DISCHARGE')")
def gold_container_export_flow_streaming():
   
    gate_in = spark.readStream.table("gate_in_events_silver") \
        .withWatermark("gate_in_timestamp", "2 hours")
    
    crane = spark.readStream.table("crane_events_silver") \
        .withWatermark("crane_timestamp", "2 hours")
    
    flow = gate_in.alias("gin").join(
        crane.alias("crn"),
        (col("gin.container_id") == col("crn.container_id")) &
        (col("crn.crane_timestamp") >= col("gin.gate_in_timestamp")) &
        (col("crn.crane_timestamp") <= col("gin.gate_in_timestamp") + expr("INTERVAL 24 HOURS")),
        "left"
    )
    
    result = flow.select(
        col("gin.container_number"),
        col("gin.container_id"),
        col("gin.container_size"),
        col("gin.container_type"),
        col("gin.seal_number"),
        
        col("gin.terminal_id"),
        
        col("gin.event_id").alias("gate_in_event_id"),
        col("gin.gate_in_timestamp"),
        col("gin.gate_id").alias("gate_in_gate_id"),
        col("gin.truck_plate_no").alias("gate_in_truck"),
        col("gin.do_number"),
        col("gin.gross_weight_kg").alias("gate_in_gross_weight"),
        col("gin.nett_weight_kg").alias("gate_in_nett_weight"),
        col("gin.gate_status").alias("gate_in_status"),
        
        col("crn.event_id").alias("crane_event_id"),
        col("crn.crane_timestamp"),
        col("crn.crane_id"),
        col("crn.crane_operation"),
        col("crn.cycle_time_sec"),
        col("crn.load_cell_kg").alias("crane_load_kg"),
        col("crn.anomali_type").alias("crane_anomaly"),
        
        spark_round(
            (unix_timestamp(col("crn.crane_timestamp")) - 
             unix_timestamp(col("gin.gate_in_timestamp"))) / 3600, 
            2
        ).alias("yard_dwell_time_hours"),
        
        when(col("crn.crane_timestamp").isNotNull(), lit("LOADED_TO_VESSEL"))
        .when(col("gin.gate_in_timestamp").isNotNull(), lit("IN_YARD"))
        .otherwise(lit("UNKNOWN"))
        .alias("container_status"),
        
        current_timestamp().alias("gold_processed_at")
    )
    
    result = result.withColumn(
        "dwell_time_category",
        when(col("yard_dwell_time_hours") < 24, lit("NORMAL"))
        .when(col("yard_dwell_time_hours") < 48, lit("DELAYED"))
        .when(col("yard_dwell_time_hours") < 72, lit("LONG_DELAY"))
        .otherwise(lit("CRITICAL"))
    )
    
    return result


@dp.table(
    name="gold_container_import_flow_streaming",
    comment="Complete container import flow tracking with yard dwell time calculation",
    cluster_by=["terminal_id","container_number"]
)
@dp.expect("valid_container_number", "container_number IS NOT NULL")
@dp.expect("valid_dwell_time", "yard_dwell_time_hours >= 0 OR yard_dwell_time_hours IS NULL")
@dp.expect("valid_operation", "crane_operation IN ('LOAD', 'DISCHARGE')")
def gold_container_import_flow_streaming():
   
    gate_out = spark.readStream.table("gate_out_events_silver") \
        .withWatermark("gate_out_timestamp", "2 hours")
    
    crane = spark.readStream.table("crane_events_silver") \
        .withWatermark("crane_timestamp", "2 hours")
    
    flow = gate_out.alias("gout").join(
        crane.alias("crn"),
        (col("gout.container_id") == col("crn.container_id")) &
        (col("gout.gate_out_timestamp") >= col("crn.crane_timestamp")) &
        (col("gout.gate_out_timestamp") <= col("crn.crane_timestamp") + expr("INTERVAL 24 HOURS")),
        "left"
    )
    
    result = flow.select(
        col("gout.container_number"),
        col("gout.container_id"),
        col("gout.container_size"),
        col("gout.container_type"),
        col("gout.seal_number"),
        col("gout.terminal_id"),
        col("gout.event_id").alias("gate_out_event_id"),
        col("gout.gate_out_timestamp"),
        col("gout.gate_id").alias("gate_out_gate_id"),
        col("gout.truck_plate_no").alias("gate_out_truck"),
        col("gout.do_number"),
        col("gout.gross_weight_kg").alias("gate_out_gross_weight"),
        col("gout.nett_weight_kg").alias("gate_out_nett_weight"),
        col("gout.gate_status").alias("gate_out_status"),
        col("crn.event_id").alias("crane_event_id"),
        col("crn.crane_timestamp"),
        col("crn.crane_id"),
        col("crn.crane_operation"),
        col("crn.cycle_time_sec"),
        col("crn.load_cell_kg").alias("crane_load_kg"),
        col("crn.anomali_type").alias("crane_anomaly"),

        spark_round(
            (unix_timestamp(col("gout.gate_out_timestamp"))
               -  unix_timestamp(col("crn.crane_timestamp")) 
             ) / 3600, 
            2
        ).alias("yard_dwell_time_hours"),
        
        when(col("crn.crane_timestamp").isNotNull(), lit("DISCHARGE_FROM_VESSEL"))
        .when(col("gout.gate_out_timestamp").isNotNull(), lit("IN_YARD"))
        .otherwise(lit("UNKNOWN"))
        .alias("container_status"),
        
        current_timestamp().alias("gold_processed_at")
    )
    
    result = result.withColumn(
        "dwell_time_category",
        when(col("yard_dwell_time_hours") < 24, lit("NORMAL"))
        .when(col("yard_dwell_time_hours") < 48, lit("DELAYED"))
        .when(col("yard_dwell_time_hours") < 72, lit("LONG_DELAY"))
        .otherwise(lit("CRITICAL"))
    )
    
    return result


@dp.table(
    name="gold_crane_performance",
    comment="Crane productivity and efficiency metrics per crane per hour",
    cluster_by=["crane_id", "operation_date"]
)
@dp.expect("valid_crane_id", "crane_id IS NOT NULL")
@dp.expect("valid_total_moves", "total_moves > 0")
def gold_crane_performance():
 
    crane = spark.readStream.table("crane_events_silver")
 
    return crane.groupBy(
        "crane_id",
        "terminal_id",
        to_date("crane_timestamp").alias("operation_date"),
        hour("crane_timestamp").alias("operation_hour"),
        "crane_operation"
    ).agg(
        count("*").alias("total_moves"),
 
        # Cycle time stats (detik)
        spark_round(avg("cycle_time_sec"), 2).alias("avg_cycle_time_sec"),
        spark_min("cycle_time_sec").alias("min_cycle_time_sec"),
        spark_max("cycle_time_sec").alias("max_cycle_time_sec"),
 
        # Konversi ke moves per hour (produktivitas)
        spark_round(
            count("*") / 1.0 * (3600 / spark_max("cycle_time_sec")), 2
        ).alias("estimated_moves_per_hour"),
 
        # Load weight stats
        spark_round(avg("load_cell_kg"), 2).alias("avg_load_kg"),
        spark_max("load_cell_kg").alias("max_load_kg"),
 
        # Anomaly tracking
        spark_sum(
            when(col("anomali_type").isNotNull(), 1).otherwise(0)
        ).alias("anomaly_count"),
 
        spark_round(
            spark_sum(when(col("anomali_type").isNotNull(), 1).otherwise(0)) /
            count("*") * 100, 2
        ).alias("anomaly_rate_pct"),
 
        current_timestamp().alias("gold_processed_at")
    ).withColumn(
        # Klasifikasi performa crane
        "performance_rating",
        when(col("anomaly_rate_pct") > 10, lit("POOR"))
        .when(col("anomaly_rate_pct") > 5, lit("FAIR"))
        .when(col("avg_cycle_time_sec") > 120, lit("SLOW"))
        .otherwise(lit("GOOD"))
    )

@dp.table(
    name="gold_anomaly_summary",
    comment="Anomaly tracking per crane per day for maintenance and safety monitoring",
    cluster_by=["crane_id", "anomali_type"]
)
@dp.expect("valid_anomaly_type", "anomali_type IS NOT NULL")
def gold_anomaly_summary():
 
    crane = spark.readStream.table("crane_events_silver")
 
    return crane.filter(
        col("anomali_type").isNotNull()
    ).groupBy(
        "crane_id",
        "terminal_id",
        "anomali_type",
        to_date("crane_timestamp").alias("date")
    ).agg(
        count("*").alias("total_anomalies"),
 
        # Rata-rata cycle time saat anomali terjadi
        spark_round(avg("cycle_time_sec"), 2).alias("avg_cycle_time_when_anomaly"),
 
        # Load saat anomali
        spark_round(avg("load_cell_kg"), 2).alias("avg_load_when_anomaly"),
        spark_max("load_cell_kg").alias("max_load_when_anomaly"),
 
        spark_max("crane_timestamp").alias("last_anomaly_time"),
        spark_min("crane_timestamp").alias("first_anomaly_time"),
 
        current_timestamp().alias("gold_processed_at")
    ).withColumn(
        # Severity berdasarkan frekuensi anomali
        "anomaly_severity",
        when(col("total_anomalies") >= 10, lit("HIGH"))
        .when(col("total_anomalies") >= 5, lit("MEDIUM"))
        .otherwise(lit("LOW"))
    )

@dp.materialized_view(
    name="gold_terminal_daily_summary",
    comment="Daily terminal KPI summary combining export, import, and crane operations",
    cluster_by=["terminal_id", "date"]
)
@dp.expect("valid_terminal", "terminal_id IS NOT NULL")
@dp.expect("valid_date", "date IS NOT NULL")
def gold_terminal_daily_summary():
 
    export_flow = spark.read.table("gold_container_export_flow_streaming")
    import_flow = spark.read.table("gold_container_import_flow_streaming")
    crane = spark.read.table("crane_events_silver")
 
    # Summary export per hari
    export_summary = export_flow.groupBy(
        "terminal_id",
        to_date("gate_in_timestamp").alias("date")
    ).agg(
        count("*").alias("total_export_containers"),
        spark_round(avg("yard_dwell_time_hours"), 2).alias("avg_export_dwell_hours"),
        spark_max("yard_dwell_time_hours").alias("max_export_dwell_hours"),
        spark_sum(when(col("dwell_time_category") == "NORMAL", 1).otherwise(0)).alias("export_normal_count"),
        spark_sum(when(col("dwell_time_category") == "DELAYED", 1).otherwise(0)).alias("export_delayed_count"),
        spark_sum(when(col("dwell_time_category") == "LONG_DELAY", 1).otherwise(0)).alias("export_long_delay_count"),
        spark_sum(when(col("dwell_time_category") == "CRITICAL", 1).otherwise(0)).alias("export_critical_count"),
    )
 
    # Summary import per hari
    import_summary = import_flow.groupBy(
        "terminal_id",
        to_date("gate_out_timestamp").alias("date")
    ).agg(
        count("*").alias("total_import_containers"),
        spark_round(avg("yard_dwell_time_hours"), 2).alias("avg_import_dwell_hours"),
        spark_max("yard_dwell_time_hours").alias("max_import_dwell_hours"),
        spark_sum(when(col("dwell_time_category") == "NORMAL", 1).otherwise(0)).alias("import_normal_count"),
        spark_sum(when(col("dwell_time_category") == "DELAYED", 1).otherwise(0)).alias("import_delayed_count"),
        spark_sum(when(col("dwell_time_category") == "LONG_DELAY", 1).otherwise(0)).alias("import_long_delay_count"),
        spark_sum(when(col("dwell_time_category") == "CRITICAL", 1).otherwise(0)).alias("import_critical_count"),
    )
 
    # Summary crane per hari
    crane_summary = crane.groupBy(
        "terminal_id",
        to_date("crane_timestamp").alias("date")
    ).agg(
        count("*").alias("total_crane_moves"),
        countDistinct("crane_id").alias("active_cranes"),
        spark_round(avg("cycle_time_sec"), 2).alias("avg_cycle_time_sec"),
        spark_sum(when(col("anomali_type").isNotNull(), 1).otherwise(0)).alias("total_anomalies"),
    )
 
    # Join semua summary
    result = export_summary.join(import_summary, ["terminal_id", "date"], "full") \
                           .join(crane_summary, ["terminal_id", "date"], "left")
 
    # Total container throughput
    result = result.withColumn(
        "total_throughput",
        coalesce(col("total_export_containers"), lit(0)) +
        coalesce(col("total_import_containers"), lit(0))
    ).withColumn(
        "gold_processed_at", current_timestamp()
    )
 
    return result


@dp.table(
    name="gold_gate_hourly_traffic",
    comment="Hourly gate traffic volume for queue management and peak hour analysis",
    cluster_by=["terminal_id", "gate_id"]
)
@dp.expect("valid_gate", "gate_id IS NOT NULL")
def gold_gate_hourly_traffic():
 
    gate_in = spark.readStream.table("gate_in_events_silver") \
        .withWatermark("gate_in_timestamp", "1 hour")
    
    gate_out = spark.readStream.table("gate_out_events_silver") \
        .withWatermark("gate_out_timestamp", "1 hour")
 
    # Traffic masuk per gate per jam menggunakan window
    traffic_in = gate_in.groupBy(
        "terminal_id",
        "gate_id",
        window("gate_in_timestamp", "1 hour")
    ).agg(
        count("*").alias("vehicles_in"),
        approx_count_distinct("truck_plate_no").alias("unique_trucks_in")
    )
 
    # Traffic keluar per gate per jam menggunakan window
    traffic_out = gate_out.groupBy(
        "terminal_id",
        "gate_id",
        window("gate_out_timestamp", "1 hour")
    ).agg(
        count("*").alias("vehicles_out"),
        approx_count_distinct("truck_plate_no").alias("unique_trucks_out")
    )
 
    result = traffic_in.alias("tin").join(
        traffic_out.alias("tout"), 
        (col("tin.terminal_id") == col("tout.terminal_id")) & 
        (col("tin.gate_id") == col("tout.gate_id")) & 
        (col("tin.window") == col("tout.window")),
        "full"
    ).select(
        coalesce(col("tin.terminal_id"), col("tout.terminal_id")).alias("terminal_id"),
        coalesce(col("tin.gate_id"), col("tout.gate_id")).alias("gate_id"),
        coalesce(col("tin.window"), col("tout.window")).alias("window"),
        col("tin.vehicles_in"),
        col("tin.unique_trucks_in"),
        col("tout.vehicles_out"),
        col("tout.unique_trucks_out")
    ).withColumn(
        "date",
        to_date(col("window.start"))
    ).withColumn(
        "hour",
        hour(col("window.start"))
    ).withColumn(
        "total_traffic",
        coalesce(col("vehicles_in"), lit(0)) +
        coalesce(col("vehicles_out"), lit(0))
    ).withColumn(
        "peak_category",
        when(col("total_traffic") >= 50, lit("PEAK"))
        .when(col("total_traffic") >= 20, lit("BUSY"))
        .otherwise(lit("NORMAL"))
    ).withColumn(
        "gold_processed_at", current_timestamp()
    ).select(
        "terminal_id",
        "gate_id", 
        "date",
        "hour",
        "window",
        "vehicles_in",
        "unique_trucks_in",
        "vehicles_out",
        "unique_trucks_out",
        "total_traffic",
        "peak_category",
        "gold_processed_at"
    )
 
    return result




