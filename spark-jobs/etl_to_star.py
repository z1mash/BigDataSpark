from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, quarter, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/lab2"
PG_PROPS = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("ETL_to_Star") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Reading mock_data from PostgreSQL...")
df = spark.read.jdbc(PG_URL, "mock_data", properties=PG_PROPS)
df.cache()
print(f"Total rows: {df.count()}")

dim_customer = df.select(
    col("sale_customer_id").alias("id"),
    col("customer_first_name").alias("first_name"),
    col("customer_last_name").alias("last_name"),
    col("customer_age").alias("age"),
    col("customer_email").alias("email"),
    col("customer_country").alias("country"),
    col("customer_postal_code").alias("postal_code")
).dropDuplicates(["id"])

dim_seller = df.select(
    col("sale_seller_id").alias("id"),
    col("seller_first_name").alias("first_name"),
    col("seller_last_name").alias("last_name"),
    col("seller_email").alias("email"),
    col("seller_country").alias("country"),
    col("seller_postal_code").alias("postal_code")
).dropDuplicates(["id"])

dim_product = df.select(
    col("sale_product_id").alias("id"),
    col("product_name").alias("name"),
    col("product_category").alias("category"),
    col("product_price").alias("price"),
    col("product_weight").alias("weight"),
    col("product_color").alias("color"),
    col("product_size").alias("size"),
    col("product_brand").alias("brand"),
    col("product_material").alias("material"),
    col("product_description").alias("description"),
    col("product_rating").alias("rating"),
    col("product_reviews").alias("reviews"),
    col("product_release_date").alias("release_date"),
    col("product_expiry_date").alias("expiry_date")
).dropDuplicates(["id"])

dim_store_raw = df.select(
    col("store_name"),
    col("store_location").alias("location"),
    col("store_city").alias("city"),
    col("store_state").alias("state"),
    col("store_country").alias("country"),
    col("store_phone").alias("phone"),
    col("store_email").alias("email")
).dropDuplicates(["store_name"])

w_store = Window.orderBy("store_name")
dim_store = dim_store_raw.withColumn("id", row_number().over(w_store))

dim_supplier_raw = df.select(
    col("supplier_name"),
    col("supplier_contact").alias("contact"),
    col("supplier_email").alias("email"),
    col("supplier_phone").alias("phone"),
    col("supplier_address").alias("address"),
    col("supplier_city").alias("city"),
    col("supplier_country").alias("country")
).dropDuplicates(["supplier_name"])

w_supplier = Window.orderBy("supplier_name")
dim_supplier = dim_supplier_raw.withColumn("id", row_number().over(w_supplier))

dim_date_raw = df.select(
    to_date(col("sale_date"), "M/d/yyyy").alias("date")
).dropDuplicates(["date"])

w_date = Window.orderBy("date")
dim_date = dim_date_raw \
    .withColumn("id", row_number().over(w_date)) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("month", month("date")) \
    .withColumn("year", year("date")) \
    .withColumn("quarter", quarter("date"))

df_parsed = df.withColumn("parsed_date", to_date(col("sale_date"), "M/d/yyyy"))

df_with_store = df_parsed.join(
    dim_store.select(col("id").alias("store_id"), col("store_name")),
    on="store_name"
)

df_with_supplier = df_with_store.join(
    dim_supplier.select(col("id").alias("supplier_id"), col("supplier_name")),
    on="supplier_name"
)

df_with_date = df_with_supplier.join(
    dim_date.select(col("id").alias("date_id"), col("date")),
    df_with_supplier["parsed_date"] == dim_date["date"]
)

fact_sales = df_with_date.select(
    col("sale_customer_id").alias("customer_id"),
    col("sale_seller_id").alias("seller_id"),
    col("sale_product_id").alias("product_id"),
    col("store_id"),
    col("supplier_id"),
    col("date_id"),
    col("sale_quantity").alias("quantity"),
    col("sale_total_price").alias("total_price")
).withColumn("id", monotonically_increasing_id())

tables = {
    "dim_customer": dim_customer,
    "dim_seller": dim_seller,
    "dim_product": dim_product,
    "dim_store": dim_store,
    "dim_supplier": dim_supplier,
    "dim_date": dim_date,
    "fact_sales": fact_sales,
}

for name, data in tables.items():
    print(f"Writing {name} ({data.count()} rows)...")
    data.write.jdbc(PG_URL, name, mode="overwrite", properties=PG_PROPS)

print("ETL to Star schema completed!")
spark.stop()
