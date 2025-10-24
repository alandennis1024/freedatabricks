# Imports
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, lit
import random
# Create Spark Session
spark = SparkSession.builder \
    .appName("DeltaLakeLiquidClustering") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
# Define schema for the table
schema = ["id", "category", "value"]
# Generate random data
num_records = 10000
data = []
for i in range(num_records):
    data.append((i, f"category_{random.randint(1, 10)}", random.uniform(0, 100)))
df = spark.createDataFrame(data, schema)
# Define the Table Name
table_name = "default.my_delta_lake_liquid_clustered_table"
cluster_by_column = "value,category"

print("Creating Delta Table with Liquid Clustering...")

DeltaTable.createOrReplace(spark) \
    .tableName(table_name) \
    .addColumn("id", dataType = "INT") \
    .addColumn("category", dataType = "STRING") \
    .addColumn("value", dataType = "DOUBLE") \
    .clusterBy("value") \
    .execute()
deltaTable = DeltaTable.forName(spark, table_name)

deltaTable.alias("target").merge(source=df.alias("source"),
    condition="target.id = source.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

deltaTable = DeltaTable.forName(spark, table_name)
print("Data in the Delta Table:")
deltaTable.toDF().show(10)
print("Querying the Delta Table for category_1:")
deltaTable.toDF().filter("category = 'category_1'").show(10)
print("Querying the Delta Table for value < 20:")
deltaTable.toDF().filter("value < 20").show(10)
# sql = f"DROP TABLE IF EXISTS {table_name}"
# print(f"Executing SQL: {sql}")
# spark.sql(sql)
# print("Dropped the table.")