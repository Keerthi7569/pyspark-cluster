
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = (
    SparkSession.builder
    .appName("SparkPartitionBenchmark")
    .getOrCreate()
)

sc = spark.sparkContext

print("Spark Master:", sc.master)

# Generate 1 million records in a distributed way
data = spark.range(0, 1_000_000).select(
    (F.col("id") % 100).alias("department"),
    (F.col("id") % 50000).alias("salary")
)

# Test different partition counts
for partitions in [1, 2, 4]:
    test_df = data.repartition(partitions)

    start = time.perf_counter()

    result = test_df.groupBy("department").agg(
        F.sum("salary").alias("total_salary")
    ).count()

    elapsed = time.perf_counter() - start

    print(
        f"Partitions: {partitions} | "
        f"Groups: {result} | "
        f"Time: {elapsed:.2f} seconds"
    )

print("BENCHMARK COMPLETED")

spark.stop()

