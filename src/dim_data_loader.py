# Databricks notebook source
df = spark.read.format("csv").option("header", "true").option("inferSchema",True).load("/Volumes/streamrec/bronze/source/customers.csv")
df.write.format("delta").mode("overwrite").saveAsTable("streamrec.bronze.customer_dim")

# COMMAND ----------

df_products = spark.read.format("csv").option("header", "true").option("inferSchema", True).load("/Volumes/streamrec/bronze/source/products.csv")
df_products = df_products.withColumn("discountPercent", df_products["discountPercent"].cast("double"))
df_products.write.format("delta").mode("overwrite").saveAsTable("streamrec.bronze.product_dim")