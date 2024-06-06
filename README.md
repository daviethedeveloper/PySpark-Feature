# PySpark-Feature
Use Data bricks to filter out data and create a feature



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
