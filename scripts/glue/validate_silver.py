import sys
import json
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# ── Job initialisation ────────────────────────────────────────────────────────

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


# ── Read today's Silver partition ─────────────────────────────────────────────

today = datetime.now(timezone.utc)
silver_path = (
    f"s3://{args['SILVER_BUCKET']}/transactions/"
    f"year={today.year}/month={today.month:02d}/day={today.day:02d}/"
)

df = spark.read.parquet(silver_path)

# Cast numeric fields — may arrive as strings depending on Spark inference
df = df.withColumn("amount", F.col("amount").cast(DoubleType()))
df = df.withColumn("running_balance", F.col("running_balance").cast(DoubleType()))

total_rows = df.count()


# ── Check registry ────────────────────────────────────────────────────────────

results = []

def record(category: str, check: str, passed: bool, detail: str = None):
    results.append({
        "category": category,
        "check": check,
        "passed": passed,
        "detail": detail,
    })


# ── 1. COMPLETENESS ───────────────────────────────────────────────────────────
# Fields that must never be null on any row.
# Nullable by design: beneficiary_account_number (ATM=null), reversal_reference,
# merchant_name/category/city (non-POS=null), atm_* (non-ATM=null).

REQUIRED_NON_NULL = [
    "transaction_id",
    "transaction_datetime",
    "transaction_type",
    "transaction_category",
    "channel",
    "status",
    "amount",
    "currency",
    "running_balance",
    "reversal_flag",
    "account_number",
    "age",
    "gender",
]

for col in REQUIRED_NON_NULL:
    null_count = df.filter(F.col(col).isNull()).count()
    record("Completeness", f"{col} is never null", null_count == 0,
           f"{null_count} null rows" if null_count > 0 else None)


# ── 2. ALLOWED VALUE SETS ─────────────────────────────────────────────────────

ENUM_CHECKS = {
    "transaction_type":     ["DEBIT", "CREDIT"],
    "transaction_category": ["ATM", "POS", "EFT", "SWIFT", "DebiCheck", "RTC"],
    "channel":              ["MOBILE", "INTERNET", "BRANCH", "ATM"],
    "status":               ["SETTLED", "PENDING", "REVERSED", "FAILED"],
    "currency":             ["ZAR", "USD", "EUR", "GBP", "CHF", "JPY", "AUD"],
    "gender":               ["M", "F"],
}

for col, allowed in ENUM_CHECKS.items():
    bad = df.filter(~F.col(col).isin(allowed)).count()
    record("Allowed Values", f"{col} within allowed set", bad == 0,
           f"{bad} unexpected values" if bad > 0 else None)


# ── 3. FORMAT / REGEX ─────────────────────────────────────────────────────────

# transaction_id: TXN-YYYYMMDD-<12 hex chars uppercase>
bad_txn_id = df.filter(
    ~F.col("transaction_id").rlike(r"^TXN-\d{8}-[0-9A-F]{12}$")
).count()
record("Format", "transaction_id matches TXN-YYYYMMDD-<HEX12>", bad_txn_id == 0,
       f"{bad_txn_id} malformed IDs" if bad_txn_id > 0 else None)

# account_number: pseudonymised SHA-256 hex (64 lowercase hex chars)
bad_account = df.filter(
    ~F.col("account_number").rlike(r"^[0-9a-f]{64}$")
).count()
record("Format", "account_number is SHA-256 hex", bad_account == 0,
       f"{bad_account} malformed values" if bad_account > 0 else None)

# beneficiary_account_number: SHA-256 hex when not null
bad_ben = df.filter(
    F.col("beneficiary_account_number").isNotNull() &
    ~F.col("beneficiary_account_number").rlike(r"^[0-9a-f]{64}$")
).count()
record("Format", "beneficiary_account_number is SHA-256 hex when not null", bad_ben == 0,
       f"{bad_ben} malformed values" if bad_ben > 0 else None)

# age: integer, must be >= 18 (generator minimum_age=18)
bad_age = df.filter(F.col("age") < 18).count()
record("Format", "age >= 18", bad_age == 0,
       f"{bad_age} underage rows" if bad_age > 0 else None)


# ── 4. BUSINESS RULES ─────────────────────────────────────────────────────────

# Expected column set — partition columns (year/month/day) are encoded in the
# S3 path and are not embedded in the Parquet schema.
EXPECTED_COLUMNS = {
    "transaction_id", "transaction_datetime", "transaction_type",
    "transaction_category", "channel", "status", "amount", "currency",
    "running_balance", "reversal_flag", "reversal_reference",
    "account_number", "beneficiary_account_number",
    "age", "gender",
    "merchant_name", "merchant_category_code", "merchant_city",
    "atm_terminal_id", "atm_province", "atm_city",
}

actual_columns = set(df.columns)
extra   = sorted(actual_columns - EXPECTED_COLUMNS)
missing = sorted(EXPECTED_COLUMNS - actual_columns)
columns_match = not extra and not missing
col_detail = None
if extra:   col_detail = (col_detail or "") + f"extra: {extra} "
if missing: col_detail = (col_detail or "") + f"missing: {missing}"
record("Business Rules", "column set matches expected schema", columns_match, col_detail)

# transaction_id uniqueness
distinct_count = df.select("transaction_id").distinct().count()
dupe_count = total_rows - distinct_count
record("Business Rules", "transaction_id is unique", dupe_count == 0,
       f"{dupe_count} duplicates" if dupe_count > 0 else None)

# No negative amounts
neg_amount = df.filter(F.col("amount") < 0).count()
record("Business Rules", "amount >= 0", neg_amount == 0,
       f"{neg_amount} negative rows" if neg_amount > 0 else None)

# No overdrafts
neg_balance = df.filter(F.col("running_balance") < 0).count()
record("Business Rules", "running_balance >= 0", neg_balance == 0,
       f"{neg_balance} negative rows" if neg_balance > 0 else None)

# Partition must not be empty
record("Business Rules", "partition has at least 1 row", total_rows > 0,
       f"{total_rows} rows")

# ATM transactions must always be DEBIT
atm_credit = df.filter(
    (F.col("transaction_category") == "ATM") & (F.col("transaction_type") == "CREDIT")
).count()
record("Business Rules", "ATM transactions are always DEBIT", atm_credit == 0,
       f"{atm_credit} ATM CREDIT rows" if atm_credit > 0 else None)

# Non-SWIFT transactions must use ZAR
non_swift_non_zar = df.filter(
    (F.col("transaction_category") != "SWIFT") & (F.col("currency") != "ZAR")
).count()
record("Business Rules", "non-SWIFT transactions use ZAR", non_swift_non_zar == 0,
       f"{non_swift_non_zar} violations" if non_swift_non_zar > 0 else None)


# ── 5. CONDITIONAL NULLABILITY ────────────────────────────────────────────────

# Merchant fields: populated for POS, null for EFT/SWIFT/DebiCheck/RTC
for col in ["merchant_name", "merchant_category_code", "merchant_city"]:
    missing_pos = df.filter(
        (F.col("transaction_category") == "POS") & F.col(col).isNull()
    ).count()
    record("Conditional Nullability", f"{col} populated for POS", missing_pos == 0,
           f"{missing_pos} POS rows missing {col}" if missing_pos > 0 else None)

    unexpected_non_merchant = df.filter(
        F.col("transaction_category").isin(["EFT", "SWIFT", "DebiCheck", "RTC"]) &
        F.col(col).isNotNull()
    ).count()
    record("Conditional Nullability", f"{col} null for EFT/SWIFT/DebiCheck/RTC",
           unexpected_non_merchant == 0,
           f"{unexpected_non_merchant} unexpected rows" if unexpected_non_merchant > 0 else None)

# Beneficiary fields: null for ATM, populated for all other categories
atm_with_ben = df.filter(
    (F.col("transaction_category") == "ATM") &
    F.col("beneficiary_account_number").isNotNull()
).count()
record("Conditional Nullability", "beneficiary_account_number null for ATM",
       atm_with_ben == 0,
       f"{atm_with_ben} unexpected rows" if atm_with_ben > 0 else None)

non_atm_missing_ben = df.filter(
    (F.col("transaction_category") != "ATM") &
    F.col("beneficiary_account_number").isNull()
).count()
record("Conditional Nullability", "beneficiary_account_number populated for non-ATM",
       non_atm_missing_ben == 0,
       f"{non_atm_missing_ben} missing rows" if non_atm_missing_ben > 0 else None)

# reversal_flag must be False for PENDING and FAILED
for status_val in ["PENDING", "FAILED"]:
    bad_reversal = df.filter(
        (F.col("status") == status_val) & (F.col("reversal_flag") == True)
    ).count()
    record("Conditional Nullability", f"reversal_flag is False when status={status_val}",
           bad_reversal == 0,
           f"{bad_reversal} violations" if bad_reversal > 0 else None)

# reversal_reference: populated when reversal_flag=True, null otherwise
missing_ref = df.filter(
    (F.col("reversal_flag") == True) & F.col("reversal_reference").isNull()
).count()
record("Conditional Nullability", "reversal_reference populated when reversal_flag=True",
       missing_ref == 0,
       f"{missing_ref} rows missing reference" if missing_ref > 0 else None)

unexpected_ref = df.filter(
    (F.col("reversal_flag") == False) & F.col("reversal_reference").isNotNull()
).count()
record("Conditional Nullability", "reversal_reference null when reversal_flag=False",
       unexpected_ref == 0,
       f"{unexpected_ref} rows with unexpected reference" if unexpected_ref > 0 else None)

# ATM terminal fields: populated for ATM, null for non-ATM
for col in ["atm_terminal_id", "atm_province", "atm_city"]:
    missing_atm = df.filter(
        (F.col("transaction_category") == "ATM") & F.col(col).isNull()
    ).count()
    record("Conditional Nullability", f"{col} populated for ATM", missing_atm == 0,
           f"{missing_atm} ATM rows missing {col}" if missing_atm > 0 else None)

    unexpected_atm = df.filter(
        (F.col("transaction_category") != "ATM") & F.col(col).isNotNull()
    ).count()
    record("Conditional Nullability", f"{col} null for non-ATM", unexpected_atm == 0,
           f"{unexpected_atm} unexpected rows" if unexpected_atm > 0 else None)


# ── Summary ───────────────────────────────────────────────────────────────────

total_checks = len(results)
passed_checks = sum(1 for r in results if r["passed"])
failed_checks = total_checks - passed_checks
overall = "PASS" if failed_checks == 0 else "FAIL"

print("=" * 60)
print(f"  Silver Validation  {today.strftime('%Y-%m-%d')}")
print(f"  Rows checked : {total_rows:,}")
print(f"  PASSED       : {passed_checks}/{total_checks}")
print(f"  FAILED       : {failed_checks}/{total_checks}")
print(f"  Overall      : {overall}")
print("=" * 60)

if failed_checks > 0:
    print("\nFailed checks:")
    for r in results:
        if not r["passed"]:
            print(f"  [{r['category']}] {r['check']} — {r['detail']}")

# ── Write JSON report to S3 ───────────────────────────────────────────────────

report = {
    "run_date": today.strftime("%Y-%m-%d"),
    "silver_path": silver_path,
    "total_rows": total_rows,
    "checks_passed": passed_checks,
    "checks_failed": failed_checks,
    "overall": overall,
    "results": results,
}

s3 = boto3.client("s3")
report_key = (
    f"validation-reports/year={today.year}/month={today.month:02d}"
    f"/day={today.day:02d}/silver_validation.json"
)
s3.put_object(
    Bucket=args["GOLD_BUCKET"],
    Key=report_key,
    Body=json.dumps(report, indent=2),
    ContentType="application/json",
)
print(f"\nReport written to s3://{args['GOLD_BUCKET']}/{report_key}")

job.commit()

if failed_checks > 0:
    raise Exception(f"Silver validation failed: {failed_checks}/{total_checks} checks did not pass.")
