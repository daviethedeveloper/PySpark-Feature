# PySpark-Feature
Use Data bricks to filter out data and create a feature


## Income Spend Map

This feature, called "Income Spend Map", provides insights into the average spend per customer based on different income ranges. 


Here's how it was created:
```
1. Unmap BUCKETED_CUSTOMER_INCOMES: Extract income data from `poi_spend_visits` by exploding the `BUCKETED_CUSTOMER_INCOMES` column to separate income ranges and their corresponding values.

2. Unmap MEAN_SPEND_PER_CUSTOMER_BY_INCOME: Similarly, extract spend data from `poi_spend_visits` by exploding the `MEAN_SPEND_PER_CUSTOMER_BY_INCOME` column to obtain spend values for each income range.

3. Aggregate Data: Join the unmapped income and spend data based on "TRACTCE" and "income" columns. Then, group the data by "TRACTCE" and pivot the "income" column to create separate columns for each income range. Aggregate the spend values using the first value encountered for each income range.

4. Define Income Ranges: Define the income ranges to be considered, such as "<25k", "25-45k", "45-60k", etc.

5. Create Map Column: Using `create_map()` function, create a map where each income range is paired with its corresponding spend value. This map will serve as the "Income Spend Map" feature.

6. Join with feature_table: Join the newly created income spend map with the existing `feature_table` based on the "TRACTCE" column, ensuring that each region gets its income spend map.

7. Handle Missing Values: Fill any missing values resulting from the join operation with zeros to maintain data integrity.
```

```pyspark
from pyspark.sql import functions as F

unmapped_incomes = poi_spend_visits.select(
    F.col("TRACTCE"),
    F.explode("BUCKETED_CUSTOMER_INCOMES").alias("income", "income_value")
)


unmapped_spend = poi_spend_visits.select(
    F.col("TRACTCE"),
    F.explode("MEAN_SPEND_PER_CUSTOMER_BY_INCOME").alias("income", "spend_value")
)


agg_income_spend = unmapped_incomes.join(
    unmapped_spend,
    ["TRACTCE", "income"]
).groupBy("TRACTCE").pivot("income").agg(
    F.first("spend_value").alias("mean_spend_per_customer_by_income")
)


income_ranges = ["<25k", "25-45k", "45-60k", "60-75k", "75-100k", "100-150k", ">150k"]


income_spend_map = F.create_map(*sum([[F.lit(income), F.col(income)] for income in income_ranges], []))


agg_income_spend = agg_income_spend.withColumn("income_spend_map", income_spend_map).select("TRACTCE", "income_spend_map")

feature_table = feature_table.join(
    agg_income_spend,
    on="TRACTCE",
    how="left"
)

feature_table = feature_table.na.fill(0)
```
