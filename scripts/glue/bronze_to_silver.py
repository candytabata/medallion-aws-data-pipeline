import sys
import json
import hashlib
import boto3
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


#  Job initialisation 

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BRONZE_BUCKET",
    "SILVER_BUCKET",
    "SALT_SECRET_ARN",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


#  Fetch salt from Secrets Manager 
def get_salt(secret_arn: str) -> str:
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_arn)
    secret = response.get("SecretString") or response.get("SecretBinary").decode()
    # Secrets Manager may return a JSON object or a plain string
    try:
        return json.loads(secret).get("password", secret)
    except (json.JSONDecodeError, AttributeError):
        return secret

salt = get_salt(args["SALT_SECRET_ARN"])

#  Build today's Bronze path 
today = datetime.now(timezone.utc)
bronze_prefix = f"year={today.year}/month={today.month:02d}/day={today.day:02d}/"
bronze_path = f"s3://{args['BRONZE_BUCKET']}/{bronze_prefix}"
silver_path = f"s3://{args['SILVER_BUCKET']}/"

#  Read raw NDJSON from Bronze 
raw_df = spark.read.json(bronze_path)

#  Drop high-risk PII fields 
PII_DROP = [
    "cellphone_number",
    "email_address",
    "physical_address",
    "ip_address",
    "device_fingerprint",
    "gps_latitude",
    "gps_longitude",
    "beneficiary_name",
    "account_holder_name",
]
df = raw_df.drop(*PII_DROP)

#  Pseudonymise fields with keyed SHA-256 
PSEUDONYMISE_FIELDS = [
    "account_number",
    "beneficiary_account_number",
]

#  Hash helper 
def hash_pii(value: str, salt: str) -> str:
    if value is None:
        return None
    return hashlib.sha256(f"{salt}{value}".encode()).hexdigest()

hash_udf = F.udf(lambda v: hash_pii(v, salt), StringType())

for field in PSEUDONYMISE_FIELDS:
    df = df.withColumn(field, hash_udf(F.col(field)))

#  Derive age + gender from SA ID number

def parse_sa_id(id_number: str):
    """
    SA ID: YYMMDD SSSS C A Z
    First 6 digits = DOB, digit 7-10 = gender (>= 5000 = male)
    Returns (age, gender) or (None, None) if invalid.
    """
    if id_number is None or len(str(id_number)) != 13:
        return (None, None)
    try:
        yy = int(id_number[0:2])
        mm = int(id_number[2:4])
        dd = int(id_number[4:6])
        gender_digit = int(id_number[6:10])

        today = datetime.now(timezone.utc)
        year = (1900 + yy) if yy >= 25 else (2000 + yy)
        dob = datetime(year, mm, dd, tzinfo=timezone.utc)
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

        gender = "M" if gender_digit >= 5000 else "F"
        return (age, gender)
    except (ValueError, TypeError):
        return (None, None)

parse_sa_id_udf = F.udf(
    parse_sa_id,
    "struct<age:int,gender:string>"
)

df = df.withColumn("_id_parsed", parse_sa_id_udf(F.col("id_number")))
df = df.withColumn("age", F.col("_id_parsed.age"))
df = df.withColumn("gender", F.col("_id_parsed.gender"))
df = df.drop("_id_parsed", "id_number")


# Province is available via atm_province for ATM transactions
df = df.drop("branch_code")

# ── Write to Silver as Parquet with hive partitions ──────────────────────────
# Add partition columns before writing
df = df.withColumn("year",  F.lit(str(today.year)))
df = df.withColumn("month", F.lit(f"{today.month:02d}"))
df = df.withColumn("day",   F.lit(f"{today.day:02d}"))

# Write to Silver
silver_path = f"s3://{args['SILVER_BUCKET']}/transactions/"

(
    df.write
    .mode("append")
    .partitionBy("year", "month", "day")
    .parquet(silver_path)
)

# Commit job bookmark 

job.commit()
