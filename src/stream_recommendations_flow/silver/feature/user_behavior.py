from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, max as _max, 
    when, collect_list, expr, desc,round
)
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

@dp.materialized_view(
    name="streamrec.feature.user_behavior",
    comment="User behavior features aggregated from enriched events"
)
def user_behavior() -> DataFrame:
    
    # Read enriched events as stream
    enriched_df = spark.read.table("streamrec.silver.enriched_events")
    
    # Aggregate user behavior metrics
    user_agg = enriched_df.groupBy("customer_id").agg(
        # Total views - count view events
        count(when(col("eventType") == "view", 1)).alias("total_views"),
        
        # Purchase count - count purchase events
        count(when(col("eventType") == "purchase", 1)).alias("purchase_count"),
        
        # Average price viewed - avg of finalPrice
        avg("finalPrice").alias("avg_price_viewed"),
        
        # Last event time - maximum timestamp
        _max("event_timestamp").alias("last_event_time"),
        
        # Collect categories and brands for frequency calculation
        collect_list("category").alias("categories_list"),
        collect_list("brand").alias("brands_list")
    )
    

   

    # Explode categories and brands for frequency calculation
    categories_exploded = user_agg.select("customer_id", "categories_list") \
        .withColumn("category", expr("explode(categories_list)"))
    brands_exploded = user_agg.select("customer_id", "brands_list") \
        .withColumn("brand", expr("explode(brands_list)"))

    # Calculate category frequency and get favorite category
    category_freq = categories_exploded.groupBy("customer_id", "category") \
        .agg(count("*").alias("category_count"))
    category_window = Window.partitionBy("customer_id").orderBy(desc("category_count"))
    favorite_category_df = category_freq.withColumn("rn", row_number().over(category_window)) \
        .filter(col("rn") == 1) \
        .select("customer_id", col("category").alias("favorite_category"))

    # Calculate brand frequency and get favorite brand
    brand_freq = brands_exploded.groupBy("customer_id", "brand") \
        .agg(count("*").alias("brand_count"))
    brand_window = Window.partitionBy("customer_id").orderBy(desc("brand_count"))
    favorite_brand_df = brand_freq.withColumn("rn", row_number().over(brand_window)) \
        .filter(col("rn") == 1) \
        .select("customer_id", col("brand").alias("favorite_brand"))

    # Join favorite category and brand back to user_agg
    result = user_agg \
        .join(favorite_category_df, "customer_id", "left") \
        .join(favorite_brand_df, "customer_id", "left") \
        .select(
            "customer_id",
            "total_views",
            "purchase_count",
            round(col("avg_price_viewed"), 2).cast(DecimalType(10, 2)).alias("avg_price_viewed"),
            "last_event_time",
            "favorite_category",
            "favorite_brand"
        )
    
    return result
