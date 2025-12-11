---
tags:
  - spark
---
# 🧪 Spark Structured Streaming Exercise

### **Aggregating High Temperature Events from Kafka**

This exercise guides you through:

1. **Reading streaming data from Kafka**
    
2. **Filtering events where `temperature > 30`**
    
3. **Aggregating average temperature per sensor** using **1-minute tumbling windows**
    
4. **Writing results** to:
    
    - console
        
    - Delta Lake
        
    - optional dashboard (e.g., push to a memory table or write to Delta for BI tools)
        

---

## 📥 1. Read Streaming Data from Kafka

**Assuming Kafka messages contain JSON**:

```json
{
  "sensorId": "sensor-1",
  "temperature": 32.4,
  "timestamp": "2025-12-11T20:10:00Z"
}
```

### Spark Code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("TemperatureStream").getOrCreate()

# Kafka stream
raw_stream = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor-events")
    .option("startingOffsets", "latest")
    .load()
)

# JSON schema
schema = StructType([
    StructField("sensorId", StringType()),
    StructField("temperature", DoubleType()),
    StructField("timestamp", TimestampType())
])

# Parse JSON
stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)
```

---

## 🔥 2. Filter Temperature > 30°C

```python
hot_stream = stream.filter(col("temperature") > 30)
```

---

## 📊 3. Aggregate: Average Temperature per Sensor (1-Minute Window)

Uses **tumbling window** and **watermark** to handle late events.

```python
from pyspark.sql.functions import window, avg

agg = (
    hot_stream
    .withWatermark("timestamp", "2 minutes")
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("sensorId")
    )
    .agg(avg("temperature").alias("avg_temp"))
)
```

---

## 📤 4. Output Options

### **A) Write to Console**

```python
query_console = (
    agg
    .writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .start()
)
```

---

### **B) Write to Delta Lake**

```python
query_delta = (
    agg
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/delta/checkpoints/sensor_avg_temp")
    .start("/mnt/delta/sensor_avg_temp")
)
```

---

### **C) Write to Memory Table (for dashboards / SQL)**

This is helpful for Databricks SQL, JDBC readers, BI tools, or local debugging.

```python
query_memory = (
    agg
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("sensor_temp_view")
    .start()
)

# Now you can query it:
spark.sql("SELECT * FROM sensor_temp_view").show()
```

---

# ✅ Full Pipeline Summary

|Step|Description|
|---|---|
|1|Read Kafka topic `"sensor-events"`|
|2|Parse JSON into structured columns|
|3|Filter `temperature > 30`|
|4|Windowed aggregation: `avg(temperature)` per sensor per 1-min window|
|5|Output: console, Delta, dashboard|
