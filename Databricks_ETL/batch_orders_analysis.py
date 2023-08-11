# Databricks notebook source
# Set authentication type to SAS for the specified ADLS Gen2 account
spark.conf.set("fs.azure.account.auth.type.dlsassign.dfs.core.windows.net", "SAS")

# Specify the SAS token provider type for accessing the ADLS account
spark.conf.set("fs.azure.sas.token.provider.type.dlsassign.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

# Set the fixed SAS token value for authentication and authorization
spark.conf.set("fs.azure.sas.fixed.token.dlsassign.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-08-31T03:26:51Z&st=2023-08-07T19:26:51Z&spr=https&sig=wU3ruQRItsr%2BNfdVcUerP4mqT2WHtA5QYOrPWZEGPwA%3D")

# COMMAND ----------

from pyspark.sql.functions import from_json, col, expr, window
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, IntegerType

# COMMAND ----------

# Read the customer batch and order stream data
customer_df = spark.sql(f"SELECT * FROM cleansed.customers")
order_stream_df = spark.read.json('abfs://demo-data@dlsassign.dfs.core.windows.net/orders.json')


# COMMAND ----------

display(order_stream_df)

# COMMAND ----------



# COMMAND ----------

# Explode the order_lines array to individual rows
exploded_df = order_stream_df.selectExpr("order_id", "customer_id as order_customer_id", "explode(order_lines) as order_line", "timestamp")


# COMMAND ----------

display(exploded_df)

# COMMAND ----------

# Calculate the change in the last 24 hours compared to the past 30 days average
industry_change_df = exploded_df.withColumn("order_date", expr("to_timestamp(timestamp)"))
display(industry_change_df)

# COMMAND ----------

display(industry_change_df)

# COMMAND ----------

industry_change_df = industry_change_df.join(customer_df, col("order_customer_id") == col("customer_id"))


# COMMAND ----------

industry_change_df = industry_change_df.selectExpr("order_id", "order_date", "order_customer_id", "order_line.product_id", "order_line.price", "specialized_industries")


# COMMAND ----------

industry_change_df = industry_change_df.withColumn("past_30_days_avg_price",
                                                   expr("avg(price) OVER (PARTITION BY specialized_industries, product_id ORDER BY order_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING)"))


# COMMAND ----------

industry_change_df = industry_change_df.withColumn("change_in_24_hours",
                                                   expr("(price - past_30_days_avg_price) / past_30_days_avg_price"))


# COMMAND ----------

# Deduplicate based on specific columns
deduplicated_df = industry_change_df.dropDuplicates(["order_id", "order_customer_id", "product_id", "order_date"])


# COMMAND ----------

# Find the top 3 industries with the biggest change
top_3_industries = deduplicated_df.groupBy("specialized_industries").agg(expr("avg(change_in_24_hours)").alias("avg_change_in_24_hours")) \
    .orderBy(col("avg_change_in_24_hours").desc()).limit(3)


# COMMAND ----------

display(top_3_industries)

# COMMAND ----------

# Start the query to display the results
query = top_3_industries.writeStream.outputMode("complete").format("console").start()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.orders

# COMMAND ----------

