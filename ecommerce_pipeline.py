from pyspark import pipelines as dp
from pyspark.sql.functions import col,to_date,sum,count

#
# bronze layer
#
@dp.materialized_view(name="bronze_customers")
def bronze_customers():
    return (
       spark.read.option("header","true")
                         .option("inferSchema","true")
                         .csv("/Volumes/dev/data/pyvolume/customer.csv")
       )

@dp.materialized_view(name="bronze_order")
def bronze_order():
    return(
        spark.read.option("header","true")
                         .option("inferSchema","true")
                         .csv("/Volumes/dev/data/pyvolume/order.csv")
    )
#
# silver layer
#
@dp.table(name="silver_customers")
def silver_customers():
    return (
       spark.read.table("bronze_customers").
       select(
           col("customer_id").cast("int"),
           col("name").alias("customer_name"),
           col("city")).
       dropDuplicates(["customer_id"])
    )

@dp.table(name="silver_order")
def silver_orders():
    return (
        spark.read.table("bronze_order").
        select(
            col("order_id").cast("int"),
            col("customer_id").cast("int"),
            col("amount").cast("double"),
        to_date(col("order_date")).alias("order_date"),
        col("status")).
        filter(col("customer_id").isNotNull()).
        filter(col("amount")>0).
        dropDuplicates(["order_id"])
    )
#
# gold layer
#
@dp.table(name="gold_customer_sales")
def gold_customer_sales():
    orders=spark.read.table("silver_order")
    customers=spark.read.table("silver_customers")
    return (
        orders.filter(col("status")=="Delivered").
        join(customers,"customer_id","inner").
        groupBy("customer_id","customer_name","city").
        agg(count("order_id").alias("total_orders"),
            sum("amount").alias("total_sales"))
    )