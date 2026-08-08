# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS StreamRec

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS StreamRec.feature;
# MAGIC CREATE SCHEMA IF NOT EXISTS StreamRec.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS StreamRec.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS StreamRec.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS StreamRec.bronze.customer_dim (
# MAGIC   customerId INT,
# MAGIC   firstName STRING,
# MAGIC   lastName STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   age INT,
# MAGIC   gender STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   country STRING,
# MAGIC   registrationDate DATE,
# MAGIC   customerSegment STRING,
# MAGIC   preferredCategory STRING,
# MAGIC   preferredDevice STRING,
# MAGIC   loyaltyPoints INT,
# MAGIC   isActive INT
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS StreamRec.bronze.product_dim (
# MAGIC   productId INT,
# MAGIC   productName STRING,
# MAGIC   category STRING,
# MAGIC   subCategory STRING,
# MAGIC   brand STRING,
# MAGIC   basePrice DOUBLE,
# MAGIC   discountPercent DOUBLE,
# MAGIC   finalPrice DOUBLE,
# MAGIC   rating DOUBLE,
# MAGIC   numReviews INT,
# MAGIC   stockQuantity INT,
# MAGIC   isActive INT
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS StreamRec.bronze.source