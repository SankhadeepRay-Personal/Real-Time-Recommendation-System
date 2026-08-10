from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="streamrec.feature.product_popularity_stream",
    comment="Product popularity metrics with sliding window aggregations for real-time recommendations",
    cluster_by=["productId", "category"]
)
def product_popularity_stream() -> DataFrame:
    """
    Aggregates product engagement metrics over a sliding window:
    - View count (eventType = 'view')
    - Purchase count (eventType = 'purchase')
    - Add to cart count (eventType = 'add_to_cart')
    - Trending products based on recent activity
    - Top categories based on engagement
    """
    
    events = spark.readStream.table("streamrec.silver.enriched_events")
    
    # Add watermark to bound state for windowed aggregations
    # 10 minutes watermark means late data up to 10 minutes is still processed
    events_with_watermark = events.withWatermark("event_timestamp", "10 minutes")
    
    # Aggregate metrics using a sliding window
    # 1-hour window sliding every 5 minutes
    product_metrics = (
        events_with_watermark
        .groupBy(
            F.col("productId"),
            F.col("productName"),
            F.col("category"),
            F.col("subCategory"),
            F.col("brand"),
            F.window("event_timestamp", "1 hour", "5 minutes")
        )
        .agg(
            # Count different event types
            F.sum(F.when(F.col("eventType") == "view", 1).otherwise(0)).alias("view_count"),
            F.sum(F.when(F.col("eventType") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            F.sum(F.when(F.col("eventType") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
            
            # Total engagement score (weighted: view=1, add_to_cart=3, purchase=5)
            F.sum(
                F.when(F.col("eventType") == "view", 1)
                .when(F.col("eventType") == "add_to_cart", 3)
                .when(F.col("eventType") == "purchase", 5)
                .otherwise(0)
            ).alias("engagement_score"),
            
            # Total events for this product in the window
            F.count("*").alias("total_events"),
            
            # Average rating for products that have ratings
            F.avg(F.when(F.col("rating").isNotNull(), F.col("rating"))).alias("avg_rating"),
            
            # Total revenue in the window
            F.sum(F.when(F.col("eventType") == "purchase", F.col("totalAmount")).otherwise(0)).alias("total_revenue")
        )
        .select(
            F.col("productId"),
            F.col("productName"),
            F.col("category"),
            F.col("subCategory"),
            F.col("brand"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("view_count"),
            F.col("purchase_count"),
            F.col("add_to_cart_count"),
            F.col("engagement_score"),
            F.col("total_events"),
            F.col("avg_rating"),
            F.col("total_revenue"),
            # Calculate conversion rates
            (F.col("add_to_cart_count") / F.greatest(F.col("view_count"), F.lit(1))).alias("cart_conversion_rate"),
            (F.col("purchase_count") / F.greatest(F.col("add_to_cart_count"), F.lit(1))).alias("purchase_conversion_rate"),
            F.current_timestamp().alias("updated_at")
        )
    )
    
    return product_metrics