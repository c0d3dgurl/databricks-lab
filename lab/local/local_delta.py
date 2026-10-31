"""Spark and Delta Lake on your own laptop, no cloud.

    python3.12 -m venv .venv && . .venv/bin/activate   # PySpark 3.5 needs Python 3.10 to 3.12
    pip install "pyspark==3.5.*" "delta-spark==3.2.*"    # and Java 17
    python local_delta.py
"""
import os
import sys

# Spark starts worker processes with plain `python3`; make them use this same Python
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("mart233-local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

path = "/tmp/mart233_customers"
spark.createDataFrame(
    [(1, "Ama Mensah", "Accra", "18-24"), (2, "Kofi Boateng", "Kumasi", "45+")],
    "customer_id LONG, name STRING, city STRING, age_band STRING",
).write.format("delta").mode("overwrite").save(path)

spark.createDataFrame(
    [(3, "Esi Owusu", "Takoradi", "25-34")],
    "customer_id LONG, name STRING, city STRING, age_band STRING",
).write.format("delta").mode("append").save(path)

spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select("version", "operation").show()
spark.read.format("delta").option("versionAsOf", 0).load(path).show()

# The transaction log is plain JSON: ls /tmp/mart233_customers/_delta_log
