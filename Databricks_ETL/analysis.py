# Databricks notebook source
# Set authentication type to SAS for the specified ADLS Gen2 account
spark.conf.set("fs.azure.account.auth.type.dlsassign.dfs.core.windows.net", "SAS")

# Specify the SAS token provider type for accessing the ADLS account
spark.conf.set("fs.azure.sas.token.provider.type.dlsassign.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

# Set the fixed SAS token value for authentication and authorization
spark.conf.set("fs.azure.sas.fixed.token.dlsassign.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-08-31T03:26:51Z&st=2023-08-07T19:26:51Z&spr=https&sig=wU3ruQRItsr%2BNfdVcUerP4mqT2WHtA5QYOrPWZEGPwA%3D")

# COMMAND ----------

#Customer source data and landed in demo-data container and existing data which is present in the raw container. First it will check whether new data is available or not if yes from soruce to raw layer then it will append only new rows in the raw layer
source_data = 'abfs://demo-data@dlsassign.dfs.core.windows.net/Customers.csv'
existing_data = 'abfs://raw@dlsassign.dfs.core.windows.net/customers.parquet'

# COMMAND ----------

# DBTITLE 1,Run data transfer script from source to raw layer container
# MAGIC %run "/Users/a11081990@outlook.com/brenntag/ContainerDataTransfer"

# COMMAND ----------

# Execute function to load data from source to raw if new data landed
data_transfer(source_data,existing_data)

# COMMAND ----------

# DBTITLE 1,Execute Customer data cleansing script
# MAGIC %run "/Users/a11081990@outlook.com/brenntag/CustomerDataPrep"

# COMMAND ----------

# DBTITLE 1,Execute live streaming orders data and write upcoming data into curated.live_orders table
# MAGIC %run "/Users/a11081990@outlook.com/brenntag/streaming_eventhub_to_hive"

# COMMAND ----------

# load live stream orders data coming from event hub to curated.live_orders table
check_point_path = f"abfs://meta-data@dlsassign.dfs.core.windows.net/curated/orders/_checkpoint"
df_stream = process_streaming_logic()
df_stream.writeStream.option("checkpointLocation", check_point_path).toTable("curated.live_orders")

# COMMAND ----------

import json
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

customer_df = spark.sql(f"SELECT * FROM cleansed.customers")
order_stream_df = spark.sql(f"SELECT * FROM curated.live_orders")

# COMMAND ----------

# Calculate the change in the last 24 hours compared to the past 30 days average
industry_change_df = order_stream_df.withColumn("order_date", expr("to_timestamp(timestamp)"))
combine_industry_change_df = industry_change_df.join(customer_df, col("order_customer_id") == col("customer_id"))

# COMMAND ----------

display(combine_industry_change_df)

# COMMAND ----------

display(industry_change_df)

# COMMAND ----------

features_df = combine_industry_change_df.selectExpr("order_id", "order_date", "order_customer_id", "order_line.product_id", "order_line.price", "specialized_industries")
total_rows_before = features_df.count()
print(f"Total rows before droping duplicates is {total_rows_before}")
# Deduplicate based on specific columns
deduplicated_df = features_df.dropDuplicates(["order_customer_id", "product_id", "order_date"])
total_rows_after = deduplicated_df.count()
print(f"Total rows after dropingduplicates is {total_rows_after}")
# display(industry_change_df3)


# COMMAND ----------

rolling_avg_df = deduplicated_df.withColumn("past_30_days_avg_price",
                                           expr("avg(price) OVER (PARTITION BY specialized_industries, product_id ORDER BY order_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING)"))


# COMMAND ----------

display(deduplicated_df)

# COMMAND ----------

rolling_avg_df = deduplicated_df.withColumn("past_30_days_avg_price",
                                                   expr("avg(price) OVER (PARTITION BY order_id,order_date,order_customer_id, specialized_industries ORDER BY order_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING)"))


# COMMAND ----------

display(rolling_avg_df)

# COMMAND ----------

price_change_df = rolling_avg_df.withColumn("change_in_24_hours",
                                                   expr("(price - past_30_days_avg_price) / past_30_days_avg_price"))


# COMMAND ----------

# Find the top 3 industries with the biggest change
top_3_industries = price_change_df.groupBy("specialized_industries").agg(expr("avg(change_in_24_hours)").alias("avg_change_in_24_hours")) \
    .orderBy(col("avg_change_in_24_hours").desc()).limit(3)

# COMMAND ----------

display(top_3_industries)

# COMMAND ----------

