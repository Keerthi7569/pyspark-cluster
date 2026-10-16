
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("EmployeeDataClusterJob")
    .getOrCreate()
)

print("Spark version:", spark.version)
print("Master:", spark.sparkContext.master)

# Create 100,000 employee records
data = [
    (
        i,
        f"Employee_{i}",
        ["Sales", "HR", "IT"][i % 3],
        None if i % 100 == 0 else 30000 + (i % 30001)
    )
    for i in range(1, 100001)
]

columns = ["id", "name", "department", "salary"]
df = spark.createDataFrame(data, columns)

# Clean and transform
clean_df = (
    df.fillna({"salary": 0})
      .dropDuplicates(["id"])
      .withColumn("bonus", F.round(F.col("salary") * 0.10, 2))
)

# Force execution and verify results
print("Total employees:", clean_df.count())
print("Missing salaries:", clean_df.filter(F.col("salary").isNull()).count())
print("Duplicate IDs:", clean_df.groupBy("id").count()
      .filter(F.col("count") > 1).count())

# Department aggregation
clean_df.groupBy("department").agg(
    F.count("*").alias("employee_count"),
    F.round(F.avg("salary"), 2).alias("average_salary"),
    F.sum("bonus").alias("total_bonus")
).show()

# RDD operation
department_totals = (
    clean_df.select("department", "salary")
    .rdd
    .map(lambda row: (row["department"], row["salary"]))
    .reduceByKey(lambda a, b: a + b)
)


print("RDD salary totals:", sorted(department_totals.collect()))


# Check the data before writing
print("Rows before Parquet write:", clean_df.count())
clean_df.printSchema()
clean_df.show(5, truncate=False)

# Write Parquet
output_path = "/shared-output/employee_parquet"

clean_df.write.mode("overwrite").parquet(output_path)

# Inspect output files
import os
saved_files = os.listdir(output_path)
print("Saved files:", saved_files)

# Confirm data files exist before reading
parquet_files = [
    name for name in saved_files
    if name.endswith(".parquet")
]

if parquet_files:
    parquet_df = spark.read.parquet(output_path)
    print("Parquet records:", parquet_df.count())
    print("Parquet columns:", parquet_df.columns)
else:
    print("ERROR: No Parquet data files were created")
