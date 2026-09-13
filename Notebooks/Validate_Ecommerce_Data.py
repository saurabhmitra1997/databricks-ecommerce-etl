# Read the Gold table
df = spark.table("workspace.default.gold_customer_sales")

# Display the final data
display(df)

# Validation 1: Gold table should not be empty
assert df.count() > 0, "Gold table is empty"

# Validation 2: Sales should not be negative
assert df.filter("total_sales < 0").count() == 0, "Negative sales found"

# Validation 3: Order count should be positive
assert df.filter("total_orders <= 0").count() == 0, "Invalid order count found"

print("Data quality checks passed successfully!")
