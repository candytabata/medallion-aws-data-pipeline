import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window


args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SILVER_BUCKET",
    "GOLD_BUCKET",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Read the Silver partition 

today = datetime.now(timezone.utc)
silver_path = (
    f"s3://{args['SILVER_BUCKET']}/transactions/"
    f"year={today.year}/month={today.month:02d}/day={today.day:02d}/"
)

df = spark.read.parquet(silver_path)
df = df.withColumn("amount", F.col("amount").cast(DoubleType()))
df = df.withColumn("running_balance", F.col("running_balance").cast(DoubleType()))

# Only settled transactions represent confirmed economic activity
settled = df.filter(F.col("status") == "SETTLED")

run_date = today.strftime("%Y-%m-%d")
gold_base = f"s3://{args['GOLD_BUCKET']}/aggregations"


# Spend by age band and category 

age_band_col = (
    F.when(F.col("age") < 25,  "18-24")
     .when(F.col("age") < 35,  "25-34")
     .when(F.col("age") < 45,  "35-44")
     .when(F.col("age") < 55,  "45-54")
     .when(F.col("age") < 65,  "55-64")
     .otherwise("65+")
)

spend_by_age_band = (
    settled
    .filter(F.col("transaction_type") == "DEBIT")
    .withColumn("age_band", age_band_col)
    .groupBy("age_band", "transaction_category")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(F.sum("amount"), 2).alias("total_spend"),
        F.round(F.avg("amount"), 2).alias("avg_spend"),
        F.round(F.max("amount"), 2).alias("max_spend"),
    )
    .orderBy("age_band", "transaction_category")
    .withColumn("run_date", F.lit(run_date))
).cache()

(
    spend_by_age_band.write
    .mode("overwrite")
    .parquet(f"{gold_base}/spend_by_age_band/run_date={run_date}/")
)
print(f"spend_by_age_band: {spend_by_age_band.count()} rows written")
spend_by_age_band.unpersist()


# Client financial activity 

client_activity = (
    settled
    .groupBy("account_number")
    .agg(
        F.count("*").alias("total_transactions"),
        F.count(F.when(F.col("transaction_type") == "DEBIT",  1)).alias("debit_count"),
        F.count(F.when(F.col("transaction_type") == "CREDIT", 1)).alias("credit_count"),
        F.round(F.sum(F.when(F.col("transaction_type") == "DEBIT",  F.col("amount")).otherwise(0)), 2).alias("total_debited"),
        F.round(F.sum(F.when(F.col("transaction_type") == "CREDIT", F.col("amount")).otherwise(0)), 2).alias("total_credited"),
        F.round(
            F.sum(F.when(F.col("transaction_type") == "CREDIT", F.col("amount")).otherwise(0)) -
            F.sum(F.when(F.col("transaction_type") == "DEBIT",  F.col("amount")).otherwise(0)),
            2
        ).alias("net_movement"),
        F.round(F.avg("amount"), 2).alias("avg_transaction_amount"),
        F.round(F.max("running_balance"), 2).alias("closing_balance"),
        F.count(F.when(F.col("reversal_flag") == True, 1)).alias("reversal_count"),
        F.countDistinct("transaction_category").alias("distinct_categories_used"),
        F.countDistinct("channel").alias("distinct_channels_used"),
    )
    .withColumn("run_date", F.lit(run_date))
).cache()

(
    client_activity.write
    .mode("overwrite")
    .parquet(f"{gold_base}/client_financial_activity/run_date={run_date}/")
)
print(f"client_financial_activity: {client_activity.count()} rows written")
client_activity.unpersist()


# Channel adoption 
# Aggregates all transactions (not just settled) by channel to show
# where customers are transacting, which categories are used per channel,
# and the failure/reversal rates per channel

channel_adoption = (
    df
    .groupBy("channel", "transaction_category")
    .agg(
        F.count("*").alias("total_transactions"),
        F.count(F.when(F.col("status") == "SETTLED",  1)).alias("settled_count"),
        F.count(F.when(F.col("status") == "FAILED",   1)).alias("failed_count"),
        F.count(F.when(F.col("status") == "REVERSED", 1)).alias("reversed_count"),
        F.count(F.when(F.col("status") == "PENDING",  1)).alias("pending_count"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.countDistinct("account_number").alias("unique_accounts"),
    )
    .withColumn(
        "failure_rate",
        F.round(F.col("failed_count") / F.col("total_transactions"), 4)
    )
    .orderBy("channel", "transaction_category")
    .withColumn("run_date", F.lit(run_date))
).cache()

(
    channel_adoption.write
    .mode("overwrite")
    .parquet(f"{gold_base}/channel_adoption/run_date={run_date}/")
)
print(f"channel_adoption: {channel_adoption.count()} rows written")
channel_adoption.unpersist()

# Provincial transaction flow
# ATM transactions carry atm_province; non-ATM rows have no province field.
# This aggregation is therefore scoped to ATM transactions only.

provincial_flow = (
    settled
    .filter(F.col("transaction_category") == "ATM")
    .groupBy("atm_province", "transaction_type")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.countDistinct("account_number").alias("unique_accounts"),
    )
    .orderBy("atm_province", "transaction_type")
    .withColumn("run_date", F.lit(run_date))
).cache()

(
    provincial_flow.write
    .mode("overwrite")
    .parquet(f"{gold_base}/provincial_flow/run_date={run_date}/")
)
print(f"provincial_flow: {provincial_flow.count()} rows written")
provincial_flow.unpersist()


# Reversal and failure rates 
# Shows the count and amount of transactions by category and status

reversal_rates = (
    df
    .groupBy("transaction_category", "status")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
    )
    .withColumn(
        "rate",
        F.round(
            F.col("transaction_count") /
            F.sum("transaction_count").over(Window.partitionBy("transaction_category")),
            4
        )
    )
    .orderBy("transaction_category", "status")
    .withColumn("run_date", F.lit(run_date))
).cache()

(
    reversal_rates.write
    .mode("overwrite")
    .parquet(f"{gold_base}/reversal_rates/run_date={run_date}/")
)
print(f"reversal_rates: {reversal_rates.count()} rows written")
reversal_rates.unpersist()


# Time-of-day spending patterns 
# Aggregates DEBIT transactions by hour of day and category to show 
# when customers are spending and how this varies by product type. 

time_of_day = (
    settled
    .filter(F.col("transaction_type") == "DEBIT")
    .withColumn("hour_of_day", F.hour(F.col("transaction_datetime").cast("timestamp")))
    .groupBy("hour_of_day", "transaction_category")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(F.sum("amount"), 2).alias("total_spend"),
        F.round(F.avg("amount"), 2).alias("avg_spend"),
    )
    .orderBy("hour_of_day", "transaction_category")
    .withColumn("run_date", F.lit(run_date))
).cache()

(
    time_of_day.write
    .mode("overwrite")
    .parquet(f"{gold_base}/time_of_day/run_date={run_date}/")
)
print(f"time_of_day: {time_of_day.count()} rows written")
time_of_day.unpersist()


# Currency diversification by age band 
# Shows the distribution of transaction volume and amount by currency and age band, 

currency_by_age = (
    settled
    .filter(F.col("transaction_category") == "SWIFT")
    .withColumn("age_band", age_band_col)
    .groupBy("age_band", "currency")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.countDistinct("account_number").alias("unique_accounts"),
    )
    .orderBy("age_band", "currency")
    .withColumn("run_date", F.lit(run_date))
).cache()

(
    currency_by_age.write
    .mode("overwrite")
    .parquet(f"{gold_base}/currency_by_age/run_date={run_date}/")
)
print(f"currency_by_age: {currency_by_age.count()} rows written")
currency_by_age.unpersist()


# MCC spend concentration 
# Aggregates settled transactions by merchant category code (MCC) to show where customers
# are spending and the concentration of spend across different types of merchants.  

mcc_spend = (
    settled
    .filter(F.col("merchant_category_code").isNotNull())
    .groupBy("merchant_category_code")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(F.sum("amount"), 2).alias("total_spend"),
        F.round(F.avg("amount"), 2).alias("avg_spend"),
        F.round(F.max("amount"), 2).alias("max_spend"),
        F.countDistinct("account_number").alias("unique_accounts"),
    )
    .orderBy(F.col("total_spend").desc())
    .withColumn("run_date", F.lit(run_date))
)

(
    mcc_spend.write
    .mode("overwrite")
    .parquet(f"{gold_base}/mcc_spend/run_date={run_date}/")
)
print(f"mcc_spend: {mcc_spend.count()} rows written")

job.commit()
