---
tags:
  - spark
---
# SparkContext

This is the *core* of Spark. It handles:

* connecting to the cluster (Standalone/YARN/Mesos/K8s),
* task scheduling,
* creating RDDs,
* managing executors.

Without SparkContext, Spark cannot run at all.

**Example:**

```python
sc = SparkContext(appName="MyApp")
rdd = sc.parallelize([1, 2, 3])
```

## SparkSession

Starting with Spark 2.0, SparkSession became the main interface.

SparkSession:

* unifies SQLContext, HiveContext, and SparkContext,
* provides DataFrame and Dataset APIs,
* is used for SQL, Hive tables, Delta Lake, and reading/writing data,
* contains a SparkContext internally.

**Example:**

```python
spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.read.json("data.json")
```

Accessing SparkContext from SparkSession:

```python
sc = spark.sparkContext
```

# 🧠 Why SparkSession was introduced

Before Spark 2.0:

* RDDs → SparkContext
* DataFrames → SQLContext
* Hive tables → HiveContext

This was messy. SparkSession unified all of these:

✔ a single entry point
✔ unified configuration
✔ easier to use in Python/JVM
✔ consistent API

# 🆚 Main differences

| Feature                          | SparkContext   | SparkSession                |
| -------------------------------- | -------------- | --------------------------- |
| Entry point                      | Yes, low-level | Yes, unified and high-level |
| RDD API                          | ✔              | ✔ via `spark.sparkContext`  |
| DataFrame/SQL/Dataset            | ✘              | ✔                           |
| Read/write data (DataSource API) | Limited        | ✔                           |
| Hive support                     | ✘              | ✔                           |
| Internally used by SparkSession  | ✘              | ✔                           |

# 📝 Summary

**SparkSession is the modern, unified way to work with Spark.**
It is a high-level API that:

* offers DataFrames, SQL, and Datasets,
* handles reading/writing data,
* gives access to Hive,
* internally wraps SparkContext.

**SparkContext is the engine. SparkSession is the steering wheel.**
