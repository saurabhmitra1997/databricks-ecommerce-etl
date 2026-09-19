from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date, sum, count


@dp.table(name="bronze_orders_incremental")
def bronze_orders_incremental():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load("/Volumes/dev/data/pyvolume/incremental_orders/")
    )


@dp.table(name="silver_orders_incremental")
def silver_orders_incremental():
    return (
        spark.read.table("bronze_orders_incremental")
        .select(
            col("order_id").cast("int"),
            col("customer_id").cast("int"),
            col("amount").cast("double"),
            to_date(col("order_date")).alias("order_date"),
            col("status")
        )
        .filter(col("customer_id").isNotNull())
        .filter(col("amount") > 0)
        .dropDuplicates(["order_id"])
    )


@dp.table(name="gold_customer_sales_incremental")
def gold_customer_sales_incremental():
    return (
        spark.read.table("silver_orders_incremental")
        .filter(col("status") == "Delivered")
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("amount").alias("total_sales")
        )
    )
