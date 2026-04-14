import random
import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from faker import Faker

fake = Faker(["zu_ZA", "en"])

SAST = timezone(timedelta(hours=2))


# Valid transaction categories per channel and their relative weights.
# SWIFT is an international interbank transfer: not available on MOBILE;
# rare on INTERNET (some banks allow it but it is uncommon); predominant in BRANCH.
CHANNEL_CATEGORY_WEIGHTS = {
    "MOBILE":   {"POS": 38, "EFT": 32, "DebiCheck": 20, "RTC": 10},
    "INTERNET": {"POS": 22, "EFT": 38, "SWIFT":  3, "DebiCheck": 22, "RTC": 15},
    "ATM":      {"ATM": 100},
    "BRANCH":   {"POS": 15, "EFT": 30, "SWIFT": 30, "DebiCheck":  5, "RTC":  5, "ATM": 15},
}

# Currencies used for SWIFT outbound payments (ZAR = inbound/rand-denominated)
SWIFT_CURRENCIES = ["USD", "EUR", "GBP", "CHF", "JPY", "AUD", "ZAR"]
SWIFT_CURRENCY_WEIGHTS = [30, 30, 20, 5, 5, 5, 5]

# ATM locations: (province_code, province_name, city) with relative weights
ATM_LOCATIONS = [
    ("GP", "Gauteng",       "Johannesburg",  30),
    ("GP", "Gauteng",       "Pretoria",      15),
    ("WC", "Western Cape",  "Cape Town",     20),
    ("KZN","KwaZulu-Natal", "Durban",        12),
    ("EC", "Eastern Cape",  "Gqeberha",       5),
    ("EC", "Eastern Cape",  "East London",    3),
    ("FS", "Free State",    "Bloemfontein",   4),
    ("NW", "North West",    "Rustenburg",     3),
    ("LP", "Limpopo",       "Polokwane",      3),
    ("MP", "Mpumalanga",    "Nelspruit",      3),
    ("NC", "Northern Cape", "Kimberley",      2),
]
_ATM_WEIGHTS = [loc[3] for loc in ATM_LOCATIONS]

# Amount ranges (min, max) in ZAR per channel
CHANNEL_AMOUNT_RANGE = {
    "MOBILE":   (5.00,      25_000.00),
    "INTERNET": (5.00,     500_000.00),
    "ATM":      (100.00,     5_000.00),
    "BRANCH":   (100.00,   500_000.00),
}


def generate_transaction(opening_balance: Decimal | None = None) -> dict:
    channel = random.choices(
        ["MOBILE", "INTERNET", "BRANCH", "ATM"],
        weights=[35, 25, 10, 15],
    )[0]

    category_pool = CHANNEL_CATEGORY_WEIGHTS[channel]
    transaction_category = random.choices(
        list(category_pool.keys()), weights=list(category_pool.values())
    )[0]

    if transaction_category == "ATM":
        transaction_type = "DEBIT"
    else:
        transaction_type = random.choices(["DEBIT", "CREDIT"], weights=[70, 30])[0]

    status = random.choices(
        ["SETTLED", "PENDING", "REVERSED", "FAILED"],
        weights=[70, 15, 10, 5],
    )[0]

    # reversal_flag may only be True for SETTLED or REVERSED; never for PENDING/FAILED
    if status == "REVERSED":
        reversal_flag = True
    elif status == "SETTLED":
        reversal_flag = random.random() < 0.10  # ~10% of settled txns are marked for reversal
    else:
        reversal_flag = False

    if opening_balance is None:
        opening_balance = Decimal(str(round(random.uniform(1000.00, 150000.00), 2)))

    lo, hi = CHANNEL_AMOUNT_RANGE[channel]
    amount = float(str(round(random.uniform(lo, hi), 2)))

    # Cap debits at the available balance — no overdrafts in this model
    if transaction_type == "DEBIT":
        amount = min(amount, opening_balance)
        running_balance = float(opening_balance) - amount
    else:
        running_balance = float(opening_balance) + amount

    currency = (
        random.choices(SWIFT_CURRENCIES, weights=SWIFT_CURRENCY_WEIGHTS)[0]
        if transaction_category == "SWIFT"
        else "ZAR"
    )

    now = datetime.now(SAST)
    transaction_dt = fake.date_time_between(
        start_date="-30d", end_date="now", tzinfo=SAST
    )

    txn_id = f"TXN-{now.strftime('%Y%m%d')}-{uuid.uuid4().hex[:12].upper()}"

    if reversal_flag:
        original_dt = now - timedelta(days=random.randint(1, 30))
        reversal_reference = f"TXN-{original_dt.strftime('%Y%m%d')}-{uuid.uuid4().hex[:12].upper()}"
    else:
        reversal_reference = None

    account_number = str(random.randint(1000000000, 9999999999))

    # South African ID: YYMMDD GSSS C A Z (13 digits)
    dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
    gender_digit = random.randint(5000, 9999)  # 5000–9999 = male, 0000–4999 = female
    sequence = random.randint(0, 9)
    citizenship = 0  # SA citizen
    id_number = f"{dob.strftime('%y%m%d')}{gender_digit:04d}{sequence}{citizenship}8"

    cellphone_number = f"+2782{random.randint(1000000, 9999999)}"
    email_address = fake.email()

    # Physical address using faker
    physical_address = fake.address().replace("\n", ", ")

    branch_codes = ["632005", "051001", "198765", "250655", "678910", "470010", "430000"]
    branch_code = random.choice(branch_codes)

    # Digital channels get IP, fingerprint, GPS; ATM/BRANCH do not
    is_digital = channel in ("MOBILE", "INTERNET")
    if is_digital:
        ip_address = f"41.{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}"
        device_fingerprint = uuid.uuid4().hex
        lat = float(round(random.uniform(-34.5, -22.0), 4))
        lon = float(round(random.uniform(16.5, 32.9), 4))
        gps_latitude = lat
        gps_longitude = lon
    else:
        ip_address = None
        device_fingerprint = None
        gps_latitude = None
        gps_longitude = None

    # Merchant fields — null for EFT/SWIFT/DebiCheck/RTC
    merchant_categories = {
        "POS": [
            # Grocery & supermarkets (MCC 5411)
            ("Pick n Pay Claremont", "5411", "Cape Town"),
            ("Woolworths Food Gardens", "5411", "Pretoria"),
            ("Checkers Fourways", "5411", "Johannesburg"),
            ("Shoprite Bellville", "5411", "Cape Town"),
            ("Spar Umhlanga", "5411", "Durban"),
            ("Makro Woodmead", "5411", "Johannesburg"),
            ("Food Lover's Market Tyger Valley", "5411", "Cape Town"),
            # Pharmacy & health (MCC 5912)
            ("Life Healthcare Rondebosch", "5912", "Cape Town"),
            ("Dis-Chem Menlyn", "5912", "Pretoria"),
            ("Clicks Eastgate", "5912", "Johannesburg"),
            ("Medirite Pharmacy Sandton", "5912", "Johannesburg"),
            # Fuel & automotive (MCC 5541)
            ("Engen Garage Sandton", "5541", "Johannesburg"),
            ("Shell Garage Rosebank", "5541", "Johannesburg"),
            ("BP Garage Century City", "5541", "Cape Town"),
            ("Sasol Garsfontein", "5541", "Pretoria"),
            ("TotalEnergies Berea", "5541", "Durban"),
            # Restaurants & fast food (MCC 5812)
            ("Nando's Melrose Arch", "5812", "Johannesburg"),
            ("Mugg & Bean Pavilion", "5812", "Durban"),
            ("Wimpy Greenacres", "5812", "Gqeberha"),
            ("Tiger's Milk De Waterkant", "5812", "Cape Town"),
            ("Ocean Basket Canal Walk", "5812", "Cape Town"),
            # Fast food (MCC 5814)
            ("KFC Hatfield Plaza", "5814", "Pretoria"),
            ("McDonald's Tyger Valley", "5814", "Cape Town"),
            ("Steers Gateway", "5814", "Durban"),
            ("Burger King Sandton City", "5814", "Johannesburg"),
            # Clothing & apparel (MCC 5651 / 5621 / 5699)
            ("Edgars Eastgate", "5651", "Johannesburg"),
            ("Foschini V&A Waterfront", "5651", "Cape Town"),
            ("Truworths Mall of Africa", "5621", "Johannesburg"),
            ("Mr Price Menlyn", "5699", "Pretoria"),
            ("Woolworths Clothing Cavendish", "5699", "Cape Town"),
            # Electronics & technology (MCC 5734)
            ("Incredible Connection Cresta", "5734", "Johannesburg"),
            ("Game Stores Tyger Valley", "5734", "Cape Town"),
            ("iStore V&A Waterfront", "5734", "Cape Town"),
            # Health & fitness (MCC 7997)
            ("Virgin Active Sandton", "7997", "Johannesburg"),
            ("Planet Fitness Canal Walk", "7997", "Cape Town"),
            # Home improvement (MCC 5251)
            ("Builders Warehouse Silverton", "5251", "Pretoria"),
            ("Leroy Merlin Cresta", "5251", "Johannesburg"),
        ],
        "ATM": [(None, None, None)],
    }

    if transaction_category in ("EFT", "SWIFT", "DebiCheck", "RTC"):
        merchant_name = None
        merchant_category_code = None
        merchant_city = None
    elif transaction_category == "POS":
        m = random.choice(merchant_categories["POS"])
        merchant_name, mcc, merchant_city = m
        merchant_category_code = f"MCC {mcc}"
    else:  # ATM
        merchant_name = None
        merchant_category_code = None
        merchant_city = None

    # Beneficiary fields — null for ATM (cash withdrawal, no counterparty account)
    if transaction_category == "ATM":
        beneficiary_account_number = None
        beneficiary_name = None
    else:
        beneficiary_account_number = str(random.randint(1000000000, 9999999999))
        beneficiary_name = fake.name()

    # ATM terminal fields — null for non-ATM transactions
    if transaction_category == "ATM":
        prov_code, atm_province, atm_city, _ = random.choices(ATM_LOCATIONS, weights=_ATM_WEIGHTS)[0]
        atm_terminal_id = f"ATM-{prov_code}-{random.randint(1, 9999):05d}"
    else:
        atm_terminal_id = None
        atm_province = None
        atm_city = None

    return {
        "transaction_id": txn_id,
        "transaction_datetime": transaction_dt,
        "transaction_type": transaction_type,
        "transaction_category": transaction_category,
        "channel": channel,
        "status": status,
        "amount": amount,
        "currency": currency,
        "running_balance": running_balance,
        "reversal_flag": reversal_flag,
        "reversal_reference": reversal_reference,
        "account_number": account_number,
        "account_holder_name": fake.name(),
        "beneficiary_account_number": beneficiary_account_number,
        "beneficiary_name": beneficiary_name,
        "id_number": id_number,
        "cellphone_number": cellphone_number,
        "email_address": email_address,
        "physical_address": physical_address,
        "branch_code": branch_code,
        "ip_address": ip_address,
        "device_fingerprint": device_fingerprint,
        "gps_latitude": gps_latitude,
        "gps_longitude": gps_longitude,
        "merchant_name": merchant_name,
        "merchant_category_code": merchant_category_code,
        "merchant_city": merchant_city,
        "atm_terminal_id": atm_terminal_id,
        "atm_province": atm_province,
        "atm_city": atm_city,
    }


'''
if __name__ == "__main__":
    import pprint

    balance = None
    for _ in range(3):
        record = generate_transaction(opening_balance=balance)
        balance = record["running_balance"]
        pprint.pprint(record)
        print()
'''

if __name__ == "__main__":
    import json

    with open("data/bronze/transactions.ndjson", "w") as f:
        balance = None
        for _ in range(10_000):
            record = generate_transaction(opening_balance=balance)
            balance = record["running_balance"]
            # Serialize Decimal and datetime to JSON-safe types
            f.write(json.dumps(record, default=str) + "\n")