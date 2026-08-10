from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


@dp.table(
    name = "streamrec.bronze.clickstream_bronze",
    comment = "Raw clickstream data from the website",
)
def clickstream_bronze() -> DataFrame:

    base_filter = col("userId").isNotNull() & col("productId").isNotNull()
    quantity_condition = (
        ~col("eventType").isin("add_to_cart", "purchase") |
        (col("quantity").isNotNull() & (col("quantity") > 0))
    )
    review_condition = (
        (col("eventType") != "review") |
        (col("rating").isNotNull() & (col("rating") >= 1) & (col("rating") <= 5))
    )
    search_condition = (
        (col("eventType") != "search") |
        (col("searchQuery").isNotNull() & (col("searchQuery") != ""))
    )
    
    raw_df = spark.readStream.table("streamrec.bronze.clickstream_bronze_raw")

    return raw_df.filter(base_filter & quantity_condition & review_condition & search_condition)
    

