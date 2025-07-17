from pyspark.sql import functions as F
from pyspark.sql.window import Window


df = spark.table("workspace.default.orders_main")


df_cleaned = (
    df.dropna(subset=[
        "order_id", "product_id", "user_id", "product_name",
        "order_dow", "order_hour_of_day", "days_since_prior_order",
        "department", "aisle", "department_id"
    ])
    .withColumn("days_since_prior_order", F.col("days_since_prior_order").cast("double"))
    .withColumn("order_hour_of_day", F.col("order_hour_of_day").cast("int"))
    .withColumn("order_dow", F.col("order_dow").cast("int"))
    .withColumn("reordered", F.col("reordered").cast("int"))
    .withColumn("department_id", F.col("department_id").cast("int"))
)


df_with_fraud_score = df_cleaned.withColumn(
    "fraud_score",
    0.3 * F.col("order_hour_of_day") +
    0.2 * F.col("order_dow") +
    0.1 * F.col("days_since_prior_order") +
    0.4 * F.col("reordered")
)


df_enriched = (
    df_with_fraud_score
    .withColumn("reorder_flag", F.when(F.col("reordered") == 1, 1).otherwise(0))
    .withColumn("time_sensitive", F.when(F.col("order_hour_of_day").between(18, 23), 1).otherwise(0))
    .withColumn("odd_hour_flag", F.when((F.col("order_hour_of_day") < 6) | (F.col("order_hour_of_day") > 22), 1).otherwise(0))
    .withColumn("is_fraud", F.when(F.col("fraud_score") > 10, 1).otherwise(0))
    .withColumn("order_month", F.lit("2024-01"))  # Static for demo — replace with real timestamp logic
    .withColumn("order_year", F.lit(2024))
    .withColumn("region_group", F.when(F.col("order_dow") <= 2, "North")
                                  .when(F.col("order_dow") <= 4, "West")
                                  .otherwise("South"))
    .withColumn("hour_category", F.when(F.col("order_hour_of_day") < 12, "Morning")
                                  .when(F.col("order_hour_of_day") < 17, "Afternoon")
                                  .otherwise("Evening"))
    .withColumn("is_weekend", F.when(F.col("order_dow").isin([0, 6]), 1).otherwise(0))
    .withColumn("revenue", F.lit(1.0))  # Replace with actual price * quantity if available
    .select(
        "order_id", "user_id", "product_id", "product_name", "department", "department_id", "aisle",
        "reordered", "order_number", "order_hour_of_day", "order_dow", "days_since_prior_order",
        "reorder_flag", "time_sensitive", "odd_hour_flag", "fraud_score", "is_fraud",
        "order_month", "order_year", "region_group", "hour_category", "is_weekend", "revenue"
    )
)

df_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.fraud_scores_enriched")


reorder_stats = df_enriched.groupBy("department_id").agg(
    F.count("*").alias("total_orders"),
    F.sum("reordered").alias("total_reorders")
)

# Show stats for verification
reorder_stats.orderBy(F.desc("total_orders")).show()
# Save as a managed Delta table in your Databricks catalog
df_enriched.write.mode("overwrite").saveAsTable("retailx.fraud_orders")


