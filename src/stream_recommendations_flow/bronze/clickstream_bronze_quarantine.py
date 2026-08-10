from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


@dp.table(
    name = "streamrec.bronze.clickstream_bronze_quarantine",
    comment = "Quarantine table for clickstream data",
)
def clickstream_bronze_quarantine() -> DataFrame:

    base_filter = col("userId").isNull() | col("productId").isNull()
    quantity_condition = (
        col("eventType").isin("add_to_cart", "purchase") &
        (col("quantity").isNull() | (col("quantity") <= 0))
    )
    review_condition = (
        (col("eventType") == "review") &
        (col("rating").isNull() | (col("rating") < 1) | (col("rating") > 5))
    )
    search_condition = (
        (col("eventType") == "search") &
        (col("searchQuery").isNull() | (col("searchQuery") == ""))
    )
    
    raw_df = spark.readStream.table("streamrec.bronze.clickstream_bronze_raw")

    return raw_df.filter(
        base_filter | quantity_condition | review_condition | search_condition
    )
    

