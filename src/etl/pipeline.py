"""
pipeline.py – Monthly KPI ETL
Usage: spark-submit pipeline.py 2025-07
"""
from pyspark.sql import SparkSession, functions as F, Window
import sys, logging

# ───────────────────────────── Spark Session
spark = (SparkSession.builder
         .appName("gcs_kpi_pipeline")
         .enableHiveSupport()
         .getOrCreate())
log = logging.getLogger("kpi")

# ───────────────────────────── Load tables
tx_raw     = spark.table("transactions_raw")
cust_raw   = spark.table("customers_raw")
camp_raw   = spark.table("campaigns_raw")

# ───────────────────────────── Clean & persist raw ➜ fact/dim
# ▸ 1. Transactions – add month partition
tx = (tx_raw
      .withColumn("txn_ym", F.date_format("txn_ts", "yyyy-MM"))
      .withColumn("segment",
                  F.when(F.col("amount_usd") < 500, "SME")
                   .when(F.col("amount_usd") < 5000, "MidCorp")
                   .otherwise("Large")))

(tx
 .repartition("txn_ym")
 .write.mode("append")
 .insertInto("transactions_fact"))

# ▸ 2. Customers – mark active/closed
cust = (cust_raw
        .withColumn("is_active", F.col("attrition_flag") == "Existing Customer"))
cust.write.mode("overwrite").saveAsTable("customer_dim")

# ▸ 3. Campaigns
camp_raw.write.mode("overwrite").saveAsTable("campaign_fact")

# ───────────────────────────── KPIs (for one month)
run_month = sys.argv[1]          # '2025-07'

# Spend by segment × treatment
spend = (spark.table("transactions_fact")
         .filter(F.col("txn_ym") == run_month)
         .join(spark.table("campaign_fact"), "campaign_id", "left")
         .groupBy("segment", "treatment")
         .agg(F.sum("amount_usd").alias("spend_usd")))

# Activation = first transaction in month
first_txn = (spark.table("transactions_fact")
             .groupBy("customer_id")
             .agg(F.min("txn_ts").alias("first_tx")))
activated = (first_txn
             .filter(F.date_format("first_tx", "yyyy-MM") == run_month)
             .join(cust.select("customer_id", "segment"), "customer_id")
             .groupBy("segment")
             .agg(F.count("*").alias("activated_accts")))

# Total accts per segment
totals = (cust.groupBy("segment")
               .agg(F.count("*").alias("total_accts")))

# Churn = no transaction in last 90 days
w = Window.partitionBy("customer_id").orderBy(F.col("txn_ts").desc())
last_tx = (spark.table("transactions_fact")
           .withColumn("rn", F.row_number().over(w))
           .filter("rn = 1")
           .withColumn("inactive_days", F.datediff(F.current_date(), "txn_ts")))
churned = (last_tx.filter("inactive_days > 90")
                  .join(cust.select("customer_id", "segment"), "customer_id")
                  .groupBy("segment")
                  .agg(F.count("*").alias("churned_accts")))

# Combine
kpi = (spend
       .join(activated, "segment", "left")
       .join(totals, "segment", "left")
       .join(churned, "segment", "left")
       .withColumn("activation_rate", F.col("activated_accts")/F.col("total_accts"))
       .withColumn("churn_rate",      F.col("churned_accts")/F.col("total_accts"))
       .withColumn("roi_pct",
                   (F.col("spend_usd") - F.col("budget_usd"))/F.col("budget_usd")*100)
       .withColumn("month_ym", F.lit(run_month)))

kpi.write.mode("overwrite")\
    .insertInto("kpi_agg_monthly", partition={"month_ym": run_month})

log.info("ETL complete for month %s", run_month)
