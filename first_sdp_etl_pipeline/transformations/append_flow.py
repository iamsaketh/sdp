from pyspark import pipelines as dp
from pyspark.sql.functions import *

dp.create_streaming_table("total_sales")

@dp.append_flow(target="total_sales")
def sales_north():
    df=spark.readStream.table("sdp_catalog.source.sales_north")
    return df

@dp.append_flow(target="total_sales")
def sales_south():
    df=spark.readStream.table("sdp_catalog.source.sales_south")
    return df
        