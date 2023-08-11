# Databricks notebook source
# Set authentication type to SAS for the specified ADLS Gen2 account
spark.conf.set("fs.azure.account.auth.type.dlsassign.dfs.core.windows.net", "SAS")

# Specify the SAS token provider type for accessing the ADLS account
spark.conf.set("fs.azure.sas.token.provider.type.dlsassign.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

# Set the fixed SAS token value for authentication and authorization
spark.conf.set("fs.azure.sas.fixed.token.dlsassign.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-08-31T03:26:51Z&st=2023-08-07T19:26:51Z&spr=https&sig=wU3ruQRItsr%2BNfdVcUerP4mqT2WHtA5QYOrPWZEGPwA%3D")

# COMMAND ----------

import json
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType

connectionString = "Endpoint=sb://brenntag1.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=PZUjh4doT1l6fkcFmUUSa+qLl7S292nty+AEhAq3U+M=;EntityPath=brenntag_eh"
ehConf = {}
starting_event_position = {
    "offset": "-1",
    "seqNo": -1,
    "enqueuedTime": None,
    "isInclusive": True,
}
ehConf["eventhubs.connectionString"] = spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
ehConf["eventhubs.consumerGroup"] = "$Default"
ehConf["startingPosition"] = json.dumps(starting_event_position)
df_eventhub = spark.readStream.format("eventhubs").options(**ehConf).load()


def process_streaming_logic():
    json_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_lines", ArrayType(StructType([
            StructField("product_id", StringType(), True),
            StructField("volume", IntegerType(), True),
            StructField("price", FloatType(), True)
        ])), True),
        StructField("amount", FloatType(), True),
        StructField("timestamp", StringType(), True)
    ])

    json_streaming_df = df_eventhub.withColumn("body", from_json(df_eventhub.body.cast("string"), json_schema))
    parsed_df = json_streaming_df.select("body.*")
    industry_change_df = parsed_df.withColumn("timestamp", expr("to_timestamp(timestamp)"))
    exploded_df = industry_change_df.selectExpr(
        "order_id", "customer_id as order_customer_id", "explode(order_lines) as order_line", "timestamp"
    )
    return exploded_df