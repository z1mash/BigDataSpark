from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, desc, rank
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/lab2"
PG_PROPS = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

CH_URL = "jdbc:clickhouse://clickhouse:8123/default?compress=0&ssl=false"
CH_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": ""
}

spark = SparkSession.builder \
    .appName("ETL_to_ClickHouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Reading star schema from PostgreSQL...")
fact = spark.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)
dim_product = spark.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)
dim_customer = spark.read.jdbc(PG_URL, "dim_customer", properties=PG_PROPS)
dim_store = spark.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS)
dim_supplier = spark.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)
dim_date = spark.read.jdbc(PG_URL, "dim_date", properties=PG_PROPS)

fact.cache()

# -------------------------------------------------------
# 1. dm_product_sales — Витрина продаж по продуктам
# - Топ-10 самых продаваемых продуктов (rank <= 10)
# - Общая выручка по категориям продуктов
# - Средний рейтинг и количество отзывов для каждого продукта
# -------------------------------------------------------
sales_per_product = fact.groupBy("product_id").agg(
    _sum("quantity").alias("total_quantity"),
    _sum("total_price").alias("total_revenue")
)

product_agg = dim_product \
    .join(sales_per_product, dim_product.id == sales_per_product.product_id, "left") \
    .select(
        dim_product.id.alias("product_id"),
        col("name"),
        col("category"),
        col("rating"),
        col("reviews"),
        col("total_quantity"),
        col("total_revenue")
    )

# Топ-10 по количеству продаж
w = Window.orderBy(desc("total_quantity"))
dm_product_sales = product_agg \
    .withColumn("sales_rank", rank().over(w)) \
    .withColumn("is_top10", (col("sales_rank") <= 10).cast("integer")) \
    .withColumn("category_revenue",
        _sum("total_revenue").over(Window.partitionBy("category"))
    ) \
    .orderBy(desc("total_quantity"))

# -------------------------------------------------------
# 2. dm_customer_sales — Витрина продаж по клиентам
# - Топ-10 клиентов с наибольшей общей суммой покупок (rank <= 10)
# - Распределение клиентов по странам
# - Средний чек для каждого клиента
# -------------------------------------------------------
customer_agg = fact \
    .join(dim_customer, fact.customer_id == dim_customer.id) \
    .groupBy(
        dim_customer.id.alias("customer_id"),
        col("first_name"),
        col("last_name"),
        col("country")
    ).agg(
        _sum("total_price").alias("total_spent"),
        avg("total_price").alias("avg_check"),
        count("*").alias("purchase_count")
    )

w_cust = Window.orderBy(desc("total_spent"))
dm_customer_sales = customer_agg \
    .withColumn("spending_rank", rank().over(w_cust)) \
    .withColumn("is_top10", (col("spending_rank") <= 10).cast("integer")) \
    .withColumn("customers_in_country",
        count("customer_id").over(Window.partitionBy("country"))
    ) \
    .orderBy(desc("total_spent"))

# -------------------------------------------------------
# 3. dm_time_sales — Витрина продаж по времени
# - Месячные и годовые тренды продаж
# - Сравнение выручки за разные периоды (prev_month_revenue)
# - Средний размер заказа по месяцам
# -------------------------------------------------------
time_agg = fact \
    .join(dim_date, fact.date_id == dim_date.id) \
    .groupBy("year", "month", "quarter") \
    .agg(
        _sum("total_price").alias("monthly_revenue"),
        avg("quantity").alias("avg_order_size"),
        count("*").alias("order_count")
    )

w_time = Window.orderBy("year", "month")
from pyspark.sql.functions import lag, coalesce, lit
dm_time_sales = time_agg \
    .withColumn("yearly_revenue",
        _sum("monthly_revenue").over(Window.partitionBy("year"))
    ) \
    .withColumn("prev_month_revenue",
        coalesce(lag("monthly_revenue", 1).over(w_time), lit(0.0))
    ) \
    .orderBy("year", "month")

# -------------------------------------------------------
# 4. dm_store_sales — Витрина продаж по магазинам
# - Топ-5 магазинов с наибольшей выручкой (rank <= 5)
# - Распределение продаж по городам и странам
# - Средний чек для каждого магазина
# -------------------------------------------------------
store_agg = fact \
    .join(dim_store, fact.store_id == dim_store.id) \
    .groupBy(
        dim_store.id.alias("store_id"),
        col("store_name"),
        col("city"),
        col("country")
    ).agg(
        _sum("total_price").alias("total_revenue"),
        avg("total_price").alias("avg_check"),
        count("*").alias("order_count")
    )

w_store = Window.orderBy(desc("total_revenue"))
dm_store_sales = store_agg \
    .withColumn("revenue_rank", rank().over(w_store)) \
    .withColumn("is_top5", (col("revenue_rank") <= 5).cast("integer")) \
    .withColumn("city_revenue",
        _sum("total_revenue").over(Window.partitionBy("city"))
    ) \
    .withColumn("country_revenue",
        _sum("total_revenue").over(Window.partitionBy("country"))
    ) \
    .orderBy(desc("total_revenue"))

# -------------------------------------------------------
# 5. dm_supplier_sales — Витрина продаж по поставщикам
# - Топ-5 поставщиков с наибольшей выручкой (rank <= 5)
# - Средняя цена товаров от каждого поставщика
# - Распределение продаж по странам поставщиков
# -------------------------------------------------------
supplier_agg = fact \
    .join(dim_supplier, fact.supplier_id == dim_supplier.id) \
    .join(dim_product, fact.product_id == dim_product.id) \
    .groupBy(
        dim_supplier.id.alias("supplier_id"),
        col("supplier_name"),
        dim_supplier.country.alias("supplier_country")
    ).agg(
        _sum("total_price").alias("total_revenue"),
        avg(dim_product.price).alias("avg_product_price"),
        count("*").alias("order_count")
    )

w_sup = Window.orderBy(desc("total_revenue"))
dm_supplier_sales = supplier_agg \
    .withColumn("revenue_rank", rank().over(w_sup)) \
    .withColumn("is_top5", (col("revenue_rank") <= 5).cast("integer")) \
    .withColumn("country_revenue",
        _sum("total_revenue").over(Window.partitionBy("supplier_country"))
    ) \
    .orderBy(desc("total_revenue"))

# -------------------------------------------------------
# 6. dm_product_quality — Витрина качества продукции
# - Продукты с наивысшим и наименьшим рейтингом (rank)
# - Корреляция между рейтингом и объемом продаж (rating + total_sold)
# - Продукты с наибольшим количеством отзывов (reviews_rank)
# -------------------------------------------------------
sales_vol = fact.groupBy("product_id").agg(
    _sum("quantity").alias("total_sold")
)

w_rating_asc = Window.orderBy("rating")
w_rating_desc = Window.orderBy(desc("rating"))
w_reviews = Window.orderBy(desc("reviews"))

dm_product_quality = dim_product \
    .join(sales_vol, dim_product.id == sales_vol.product_id, "left") \
    .select(
        dim_product.id.alias("product_id"),
        col("name"),
        col("category"),
        col("rating"),
        col("reviews"),
        col("total_sold")
    ) \
    .withColumn("rating_desc_rank", rank().over(w_rating_desc)) \
    .withColumn("rating_asc_rank", rank().over(w_rating_asc)) \
    .withColumn("reviews_rank", rank().over(w_reviews)) \
    .orderBy(desc("rating"))

# -------------------------------------------------------
# Запись 6 витрин в ClickHouse
# -------------------------------------------------------
reports = {
    "dm_product_sales":   dm_product_sales,
    "dm_customer_sales":  dm_customer_sales,
    "dm_time_sales":      dm_time_sales,
    "dm_store_sales":     dm_store_sales,
    "dm_supplier_sales":  dm_supplier_sales,
    "dm_product_quality": dm_product_quality,
}

for name, df in reports.items():
    print(f"Writing {name}...")
    df.write.jdbc(CH_URL, name, mode="overwrite", properties=CH_PROPS)

print("ETL to ClickHouse completed! 6 datamarts written.")
spark.stop()
