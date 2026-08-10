from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, to_timestamp, expr,concat_ws
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, TimestampType, FloatType, DoubleType, LongType


@dp.table(
    name = "streamrec.silver.enriched_events",
    comment = "Enriched clickstream data from the website"
)
def enriched_events() -> DataFrame:

    bronze_df = spark.readStream.table("streamrec.bronze.clickstream_bronze")
    customers_df = spark.read.table("streamrec.bronze.customer_dim")
    products_df = spark.read.table("streamrec.bronze.product_dim")
    
    transformed_df = bronze_df.join(customers_df, bronze_df.userId == customers_df.customerId, "left") .join(products_df, bronze_df.productId == products_df.productId, "left")

    return transformed_df.select(
        bronze_df.eventId,
        bronze_df.sessionId,
        bronze_df.userId.alias("customer_id"),
        bronze_df.productId,
        bronze_df.eventType,
        bronze_df.deviceType,
        bronze_df.eventSequence,
        bronze_df.searchQuery,
        bronze_df.quantity,
        bronze_df.rating,
        bronze_df.totalAmount,
        bronze_df.paymentMode,
        bronze_df.event_timestamp,
        concat_ws(" ",customers_df.firstName, customers_df.lastName).alias("full_name"),
        customers_df.email,
        customers_df.phone,
        customers_df.gender,
        customers_df.age,
        customers_df.city,
        customers_df.state,
        customers_df.country,
        customers_df.registrationDate,
        customers_df.customerSegment,
        customers_df.preferredCategory,
        customers_df.preferredDevice,
        customers_df.loyaltyPoints,	
        customers_df.isActive.alias("customer_active"),
        products_df.productName,
        products_df.category,
        products_df.subCategory,
        products_df.brand,
        products_df.basePrice,
        products_df.discountPercent,
        products_df.finalPrice,
        products_df.stockQuantity,
        products_df.isActive.alias("product_active")

    )
        