---
tags:
  - spark
---
# 🚀 Apache Spark Aggregations — Examples Cheat Sheet

## 1. `reduceByKey()` — Distributed Aggregation (RDD)

`reduceByKey()` combines values **locally on each partition first**, then shuffles reduced results.
👉 **Much more efficient** than `groupByKey()`.

### Example: Sum values by key

```python
from pyspark import SparkContext
sc = SparkContext()

rdd = sc.parallelize([
    ("apple", 3),
    ("banana", 2),
    ("apple", 4)
])

result = rdd.reduceByKey(lambda x, y: x + y)
print(result.collect())
# [('apple', 7), ('banana', 2)]
```

---

## 2. `groupByKey()` — Group Values by Key (RDD)

⚠️ **Less efficient** because it **shuffles all values** across the cluster before reducing.

### Example:

```python
rdd = sc.parallelize([
    ("apple", 3),
    ("banana", 2),
    ("apple", 4)
])

grouped = rdd.groupByKey()
result = grouped.mapValues(lambda vals: sum(vals))
print(result.collect())
# [('apple', 7), ('banana', 2)]
```

---

## 3. Aggregation Functions: `count()`, `sum()`, `avg()` (DataFrame API)

The DataFrame API is optimized and preferred for most workloads.

### Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([
    ("apple", 3),
    ("apple", 4),
    ("banana", 2)
], ["fruit", "value"])

df.groupBy("fruit").agg(
    count("*").alias("cnt"),
    sum("value").alias("total"),
    avg("value").alias("avg_val")
).show()
```

**Output:**

```
+------+---+-----+-------+
| fruit|cnt|total|avg_val|
+------+---+-----+-------+
| apple|  2|    7|    3.5|
|banana|  1|    2|    2.0|
+------+---+-----+-------+
```

---

## 4. Dataset-style Aggregation with `reduce()` (DataFrame)

You can also reduce an entire DataFrame.

### Example: Sum a column across the whole dataset

```python
from pyspark.sql.functions import col

total = df.select(col("value")).rdd.reduce(lambda x, y: x + y)
print(total)
# 9
```

---

## 5. 🔁 Stateful Aggregation (Structured Streaming)

Stateful aggregations keep **state across microbatches** and are essential for:

* Rolling counts
* Running totals
* Session windows
* Deduplication

### Example: **Running word count with state**

Uses **update mode** and `groupByKey` in streaming (strongly optimized in Spark).

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.getOrCreate()

lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Split into words
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# Stateful aggregation: running count
wordCounts = words.groupBy("word").count()

query = wordCounts.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()
```

### Example: Stateful Aggregation with Watermark (handling late data)

```python
from pyspark.sql.functions import window

events = spark.readStream.format("kafka") \
    .option("subscribe", "events") \
    .load()

parsed = events.selectExpr(
    "CAST(timestamp AS TIMESTAMP) AS ts",
    "CAST(value AS STRING) AS event"
)

aggregated = parsed \
    .withWatermark("ts", "10 minutes") \
    .groupBy(
        window("ts", "5 minutes"),
        "event"
    ) \
    .count()
```

---

## Summary Table

| Concept                     | API       | Use Case                     | Notes                                               |
| --------------------------- | --------- | ---------------------------- | --------------------------------------------------- |
| `reduceByKey()`             | RDD       | Efficient distributed reduce | Local combine before shuffle                        |
| `groupByKey()`              | RDD       | Group values for custom ops  | Heavy shuffle — avoid when possible                 |
| `count()`, `sum()`, `avg()` | DataFrame | Standard aggregation         | Catalyst optimized                                  |
| Stateful Aggregation        | Streaming | Running totals, sessions     | Stores state; requires watermark for unbounded data |
