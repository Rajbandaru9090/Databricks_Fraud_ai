# Load all CSVs from DBFS or mounted path
aisles = spark.read.csv("/mnt/data/aisles.csv", header=True, inferSchema=True)
departments = spark.read.csv("/mnt/data/departments.csv", header=True, inferSchema=True)
products = spark.read.csv("/mnt/data/products.csv", header=True, inferSchema=True)
orders = spark.read.csv("/mnt/data/orders.csv", header=True, inferSchema=True)
order_products_prior = spark.read.csv("/mnt/data/order_products__prior.csv", header=True, inferSchema=True)
order_products_train = spark.read.csv("/mnt/data/order_products__train.csv", header=True, inferSchema=True)

# Combine prior and train datasets
order_products_all = order_products_prior.union(order_products_train)

# Join order_products with products to get product details
df = order_products_all.join(products, on="product_id", how="left") \
                       .join(aisles, on="aisle_id", how="left") \
                       .join(departments, on="department_id", how="left") \
                       .join(orders, on="order_id", how="left")

# Optional: Filter if needed (e.g., only completed orders, non-null values)
df_clean = df.dropna(subset=["product_name", "order_id", "department", "aisle"])

# Save as a Delta Table or Temp View for analysis
df_clean.createOrReplaceTempView("retailx_orders_clean")
