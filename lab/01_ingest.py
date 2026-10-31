# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Ingest: Mart 233, a made-up online shop in Ghana
# MAGIC Generates customer profiles, products and orders, writes a raw **bronze** orders table,
# MAGIC then cleaned **silver** tables. Mart 233 and every number here are invented for the lab.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"   # default catalog on Free Edition
SCHEMA = "shop"
N_CUSTOMERS = 2_000
N_ORDERS = 50_000

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")


def pick(values, seed):
    """A random element of `values` per row."""
    return F.element_at(F.array(*[F.lit(v) for v in values]), (F.rand(seed) * len(values)).cast("int") + 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer profiles
# MAGIC Each profile gets a hidden favourite category, driven mostly by age band,
# MAGIC so the analysis later has a real preference to find.

# COMMAND ----------

FIRST_NAMES = ["Ama", "Kofi", "Akosua", "Kwame", "Esi", "Yaw", "Abena", "Kwesi",
               "Efua", "Kojo", "Adwoa", "Fiifi", "Akua", "Kwabena", "Afia", "Kwaku"]
LAST_NAMES = ["Mensah", "Owusu", "Asante", "Boateng", "Addo", "Osei",
              "Agyeman", "Quaye", "Tetteh", "Darko", "Ofori", "Appiah"]
CITIES = ["Accra", "Kumasi", "Takoradi", "Tamale", "Cape Coast", "Ho"]
AGE_BANDS = ["18-24", "25-34", "35-44", "45+"]
TWO_YEARS = 2 * 365 * 24 * 3600


def fav_category(age, customer_id):
    """The hidden favourite category, mostly set by age band. Built from a hash of the
    customer id, so it gives the same answer every time it is worked out."""
    r = (F.abs(F.hash(customer_id)) % 100) / 100
    return (
        F.when(age == "18-24", F.when(r < 0.6, "Phones").otherwise("Fashion"))
        .when(age == "25-34", F.when(r < 0.5, "Fashion").otherwise("Electronics"))
        .when(age == "35-44", F.when(r < 0.5, "Home").otherwise("Groceries"))
        .otherwise(F.when(r < 0.5, "Groceries").otherwise("Beauty"))
    )


customers = (
    spark.range(N_CUSTOMERS)
    .withColumnRenamed("id", "customer_id")
    .withColumn("name", F.concat_ws(" ", pick(FIRST_NAMES, 1), pick(LAST_NAMES, 2)))
    .withColumn("city", pick(CITIES, 3))
    .withColumn("age_band", pick(AGE_BANDS, 5))
    .withColumn(
        "joined_at",
        (F.unix_timestamp(F.current_timestamp()) - (F.rand(6) * TWO_YEARS).cast("long")).cast("timestamp"),
    )
)

# Two fixed demo accounts the shop app signs in as; everyone else is random
DEMO_CUSTOMERS = {1: ("Ama Mensah", "Accra", "18-24"), 2: ("Kofi Boateng", "Kumasi", "45+")}
for cid, (name, city, age_band) in DEMO_CUSTOMERS.items():
    is_demo = F.col("customer_id") == cid
    customers = (
        customers.withColumn("name", F.when(is_demo, name).otherwise(F.col("name")))
        .withColumn("city", F.when(is_demo, city).otherwise(F.col("city")))
        .withColumn("age_band", F.when(is_demo, age_band).otherwise(F.col("age_band")))
    )

# The favourite category is never stored: the analysis has to find it
customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("customers")
display(spark.table("customers").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

products = spark.createDataFrame(
    [
        (1, "Phones", 0, "Budget smartphone", 1800.0),
        (2, "Phones", 1, "Mid-range smartphone", 3200.0),
        (3, "Phones", 2, "Phone case and screen guard", 80.0),
        (4, "Fashion", 0, "Kente print shirt", 250.0),
        (5, "Fashion", 1, "Ladies sandals", 180.0),
        (6, "Fashion", 2, "Jeans", 220.0),
        (7, "Electronics", 0, "Bluetooth speaker", 450.0),
        (8, "Electronics", 1, "Wireless earbuds", 300.0),
        (9, "Electronics", 2, "Power bank 20,000mAh", 260.0),
        (10, "Home", 0, "Standing fan", 600.0),
        (11, "Home", 1, "Rice cooker", 520.0),
        (12, "Home", 2, "Bedsheet set", 300.0),
        (13, "Groceries", 0, "Jasmine rice 5kg", 160.0),
        (14, "Groceries", 1, "Cooking oil 3L", 120.0),
        (15, "Groceries", 2, "Chocolate malt drink 1kg", 110.0),
        (16, "Beauty", 0, "Shea butter body cream", 70.0),
        (17, "Beauty", 1, "Hair extensions", 350.0),
        (18, "Beauty", 2, "Perfume", 280.0),
    ],
    "product_id INT, category STRING, product_idx INT, product_name STRING, price_ghs DOUBLE",
)
products.drop("product_idx").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: raw orders, as they landed

# COMMAND ----------

CATEGORIES = ["Phones", "Fashion", "Electronics", "Home", "Groceries", "Beauty"]
NINETY_DAYS = 90 * 24 * 3600
# Each city leans towards one category as well
CITY_CATEGORY = {"Accra": "Electronics", "Kumasi": "Fashion", "Takoradi": "Phones",
                 "Tamale": "Groceries", "Cape Coast": "Beauty", "Ho": "Home"}
city_category = F.create_map(*[F.lit(x) for kv in CITY_CATEGORY.items() for x in kv])

orders = (
    spark.range(N_ORDERS)
    .withColumnRenamed("id", "order_id")
    .withColumn("customer_id", (F.rand(7) * N_CUSTOMERS).cast("long"))
    .join(F.broadcast(spark.table("customers").select("customer_id", "age_band", "city")), "customer_id")
    .withColumn("fav_category", fav_category(F.col("age_band"), F.col("customer_id")))
    # 60% of a shopper's orders come from their favourite category, 15% from their city's, the rest at random
    .withColumn("r", F.rand(8))
    .withColumn(
        "category",
        F.when(F.col("r") < 0.6, F.col("fav_category"))
        .when(F.col("r") < 0.75, city_category[F.col("city")])
        .otherwise(pick(CATEGORIES, 9)),
    )
    .withColumn("product_idx", (F.rand(10) * 3).cast("int"))
    .join(F.broadcast(products), ["category", "product_idx"])
    .withColumn("quantity", (F.rand(11) * 3 + 1).cast("int"))
    .withColumn("payment", pick(["Mobile money", "Mobile money", "Card", "Cash on delivery"], 12))
    .withColumn("device", F.when(F.rand(13) < 0.8, "mobile").otherwise("desktop"))
    .withColumn(
        "ordered_at",
        (F.unix_timestamp(F.current_timestamp()) - (F.rand(14) * NINETY_DAYS).cast("long")).cast("timestamp"),
    )
    # 40% of orders get a rating; shoppers rate their favourite category higher
    .withColumn(
        "rating",
        F.when(
            F.rand(15) < 0.4,
            F.when(F.col("category") == F.col("fav_category"), 4 + (F.rand(16) * 2).cast("int"))
            .otherwise(2 + (F.rand(16) * 3).cast("int")),
        ),
    )
    .select("order_id", "customer_id", "product_id", "quantity", "price_ghs",
            "payment", "device", "ordered_at", "rating")
)

# Add some dirt so the silver step has work to do: duplicates and zero quantities
dirty = orders.sample(0.01, seed=17).unionByName(
    orders.sample(0.005, seed=18).withColumn("quantity", F.lit(0))
)
bronze = orders.unionByName(dirty)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real orders from the shop app
# MAGIC The Mart 233 app saves every purchase to Lakebase (Postgres). That database is registered in
# MAGIC Unity Catalog as `mart233_app`, so Spark can read it like any other table, with no copy.
# MAGIC Buy something in the app, run this notebook again, and the gold tables change.

# COMMAND ----------

APP_ORDERS = "mart233_app.shop.order_items"

if spark.catalog.tableExists(APP_ORDERS):
    app_orders = spark.table(APP_ORDERS).select(
        (F.col("id") + 10_000_000).cast("long").alias("order_id"),  # keeps app ids clear of generated ones
        F.col("customer_id").cast("long"),
        F.col("product_id").cast("int"),
        F.col("quantity").cast("int"),
        F.col("price_ghs").cast("double"),
        "payment",
        F.lit("app").alias("device"),
        F.col("created_at").cast("timestamp").alias("ordered_at"),
        F.lit(None).cast("int").alias("rating"),
    )
    print(app_orders.count(), "order lines from the shop app")
    bronze = bronze.unionByName(app_orders)
else:
    print(f"{APP_ORDERS} not found: skipping app orders (see the app section of the README)")

bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_orders")
display(spark.table("bronze_orders").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: typed, cleaned, deduplicated, joined to profiles and products

# COMMAND ----------

silver = (
    spark.table("bronze_orders")
    .filter("quantity > 0")
    .withColumn("revenue_ghs", F.col("quantity") * F.col("price_ghs"))
    .withColumn("order_date", F.to_date("ordered_at"))
    .dropDuplicates(["order_id"])
    .join(spark.table("customers").select("customer_id", "name", "city", "age_band"), "customer_id")
    .join(spark.table("products").select("product_id", "product_name", "category"), "product_id")
)

silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_orders")
print(spark.table("bronze_orders").count(), "bronze rows ->", spark.table("silver_orders").count(), "silver rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta keeps every version

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY silver_orders"))

# COMMAND ----------

# Time travel: read the first version of the table
display(spark.sql("SELECT count(*) AS rows_in_v0 FROM silver_orders VERSION AS OF 0"))
