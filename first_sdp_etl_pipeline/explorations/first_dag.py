from pyspark import pipelines as dp
from pyspark.sql.functions import *

#materialised view
"""
A materialized view is a declarative pipeline object. It includes a query that defines it, a flow to update it, and the cached results for fast access. A materialized view:

Tracks changes in upstream data.
On trigger, incrementally processes the changed data and applies the necessary transformations.
Maintains the output table, in sync with the source data, based on a specified refresh interval.
Materialized views are a good choice for many transformations:

You apply reasoning over cached results instead of rows. In fact, you simply write a query.
They are always correct. All required data is processed, even if it arrives late or out of order.
They are often incremental. Databricks will try to choose the appropriate strategy that minimizes the cost of updating a materialized view.
"""
# DBTITLE 1,Create a materialized view
# Create a materialized view
@dp.materialized_view(name="source_sales_mv")
def src_sales():
    df=spark.read.table("sdp_catalog.source.sales")
    df=df.withColumn("sale_date",to_date(col("date")))
    df=df.withColumn("sale_year",year(col("date")))
    df=df.withColumn("sale_month", month(col("date")))
    return df

@dp.materialized_view(name="silver_sales_mv")
# @dp.temporary_view(name="silver_sales_mv")
def sales_mv():
    df=spark.read.table("source_sales_mv")
    df=df.withColumn("sale_day", dayofmonth(col("date")))
    df=df.withColumn("revenue",col("revenue")*2)
    return df    

@dp.materialized_view(name="gold_mv")   
def gold_mv():
    df=spark.read.table("silver_sales_mv")
    df=df.groupBy("sale_year","sale_month","sale_day").agg(sum(col("revenue")).alias("revenue"))
    return df