from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    name="streamrec.feature.product_popularity",
    comment="User-product interaction counts by event type for ML model training",
    cluster_by=["productId"]
)
def product_popularity() -> DataFrame:
    """
    Simple user-product event counts:
    - Which user interacted with which product
    - How many views, purchases, and add_to_cart events per user-product pair
    """
    
    # Read enriched events as batch for stable ML training features
    events = spark.read.table("streamrec.silver.enriched_events")
    
    # Group by product, count each event type
    product_aggregates = (
        events
        .groupBy(
            "productId",
            "productName",
            "category",
            "subCategory",
            "brand"
        )
        .agg(
            # Count each event type for this product
            F.sum(F.when(F.col("eventType") == "view", 1).otherwise(0)).alias("view_count"),
            F.sum(F.when(F.col("eventType") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            F.sum(F.when(F.col("eventType") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
            
            # Total interactions for this product
            F.count("*").alias("total_interactions")
        )
    )
    
    # Calculate category-level metrics
    category_metrics = (
        product_aggregates
        .groupBy("category")
        .agg(
            F.sum("total_interactions").alias("category_total_interactions"),
            F.sum("view_count").alias("category_view_count"),
            F.sum("purchase_count").alias("category_purchase_count")
        )
    )
    
    # Join category metrics back to products
    products_with_category = product_aggregates.join(
        category_metrics,
        "category",
        "left"
    )
    
    # Define windows for ranking
    trending_window = Window.orderBy(F.desc("total_interactions"))
    category_window = Window.partitionBy("category").orderBy(F.desc("total_interactions"))
    top_category_window = Window.orderBy(F.desc("category_total_interactions"))
    
    # Add rankings and flags
    result = (
        products_with_category
        .withColumn("trending_rank", F.row_number().over(trending_window))
        .withColumn("category_rank", F.row_number().over(category_window))
        .withColumn("category_popularity_rank", F.dense_rank().over(top_category_window))
        .withColumn("is_trending_product", F.when(F.col("trending_rank") <= 20, True).otherwise(False))
        .withColumn("is_top_category", F.when(F.col("category_popularity_rank") <= 5, True).otherwise(False))
        .select(
            F.col("productId"),
            F.col("productName"),
            F.col("category"),
            F.col("subCategory"),
            F.col("brand"),
            F.col("view_count"),
            F.col("purchase_count"),
            F.col("add_to_cart_count"),
            F.col("total_interactions"),
            F.col("trending_rank"),
            F.col("is_trending_product"),
            F.col("category_rank"),
            F.col("is_top_category"),
            F.col("category_total_interactions"),
            F.current_timestamp().alias("updated_at")
        )
    )
    
    return result