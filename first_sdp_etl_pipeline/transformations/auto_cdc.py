from pyspark import pipelines as dp
from pyspark.sql.functions import *

dp.create_streaming_table("products_scd2")
dp.create_streaming_table("products_scd1")

@dp.temporary_view(name="products_src")
def products_src():
    df=spark.readStream.table("sdp_catalog.source.products")
    return df

# SCT Type-2
dp.create_auto_cdc_flow(
  target = "products_scd2",
  source = "products_src",
  keys = ["product_id"],
  sequence_by = col("updated_at"),
  except_column_list = ["updated_at"],
  stored_as_scd_type = "2"
)

# SCT Type-1
dp.create_auto_cdc_flow(
  target = "products_scd1",
  source = "products_src",
  keys = ["product_id"],
  sequence_by = col("updated_at"),
  except_column_list = ["updated_at"],
  stored_as_scd_type = "1"
)
