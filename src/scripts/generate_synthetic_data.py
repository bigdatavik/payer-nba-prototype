# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare Payer Synthetic Data Generation
# MAGIC Generates realistic synthetic data for the Next-Best-Action prototype.

# COMMAND ----------

# MAGIC %pip install faker holidays --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any

import numpy as np
import pandas as pd
from faker import Faker
from pyspark.sql import SparkSession

# COMMAND ----------

# Get parameters from widgets (set by notebook_task base_parameters)
dbutils.widgets.text("catalog", "humana_payer")
dbutils.widgets.text("schema", "next_best_action")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

print(f"Generating data for {CATALOG}.{SCHEMA}")
print(f"Volume path: {VOLUME_PATH}")

# COMMAND ----------

# Initialize
fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

spark = SparkSession.builder.getOrCreate()

# Data volume configuration
# NOTE: Reduced for demo - keeps AI functions fast while showing meaningful data
NUM_MEMBERS = 50  # Small but representative sample
NUM_CLAIMS_PER_MEMBER_AVG = 5
NUM_INTERACTIONS_PER_MEMBER_AVG = 3  # ~150 total interactions for AI enrichment
LOOKBACK_MONTHS = 12

today = datetime.now().date()
start_date = today - timedelta(days=LOOKBACK_MONTHS * 30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Infrastructure

# COMMAND ----------

# Use existing catalog and schema, create only if needed
try:
    spark.sql(f"USE CATALOG {CATALOG}")
    print(f"Using catalog: {CATALOG}")
except Exception as e:
    print(f"Note: Could not use catalog {CATALOG}: {e}")
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
        spark.sql(f"USE CATALOG {CATALOG}")
    except Exception as e2:
        print(f"Warning: Could not create catalog: {e2}")

try:
    spark.sql(f"USE SCHEMA {SCHEMA}")
    print(f"Using schema: {SCHEMA}")
except Exception as e:
    print(f"Note: Could not use schema {SCHEMA}: {e}")
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
        spark.sql(f"USE SCHEMA {SCHEMA}")
    except Exception as e2:
        print(f"Warning: Could not create schema: {e2}")

try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_data")
    print(f"Volume ready: {VOLUME_PATH}")
except Exception as e:
    print(f"Warning: Could not create volume: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Members Table

# COMMAND ----------

# Plan types with weights
PLAN_TYPES = [
    ("Medicare Advantage", 0.35),
    ("Commercial PPO", 0.25),
    ("Commercial HMO", 0.20),
    ("Medicaid", 0.12),
    ("Individual Exchange", 0.08),
]

MEMBER_SEGMENTS = [
    ("High Value", 0.15),
    ("Rising Risk", 0.20),
    ("Chronic Care", 0.25),
    ("Healthy", 0.30),
    ("New Member", 0.10),
]

REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
STATES = {
    "Northeast": ["NY", "NJ", "PA", "MA", "CT"],
    "Southeast": ["FL", "GA", "NC", "SC", "VA"],
    "Midwest": ["OH", "IL", "MI", "IN", "WI"],
    "Southwest": ["TX", "AZ", "NM", "OK", "CO"],
    "West": ["CA", "WA", "OR", "NV", "UT"],
}

COMMUNICATION_PREFERENCES = ["Email", "Phone", "Mail", "SMS", "Portal"]


def generate_members() -> List[Dict[str, Any]]:
    """Generate member records with demographics and plan info."""
    members = []

    for i in range(NUM_MEMBERS):
        member_id = f"MBR{str(i+1).zfill(8)}"
        region = random.choice(REGIONS)
        state = random.choice(STATES[region])

        plan_type = np.random.choice(
            [p[0] for p in PLAN_TYPES],
            p=[p[1] for p in PLAN_TYPES]
        )

        segment = np.random.choice(
            [s[0] for s in MEMBER_SEGMENTS],
            p=[s[1] for s in MEMBER_SEGMENTS]
        )

        # Enrollment date - weighted toward longer tenure
        tenure_months = int(np.random.exponential(scale=24)) + 1
        tenure_months = min(tenure_months, LOOKBACK_MONTHS + 36)
        enrollment_date = today - timedelta(days=tenure_months * 30)

        # Age distribution based on plan type
        if plan_type == "Medicare Advantage":
            age = random.randint(65, 95)
        elif plan_type == "Medicaid":
            age = random.choice(list(range(0, 18)) + list(range(18, 65)))
        else:
            age = random.randint(18, 64)

        birth_date = today - timedelta(days=age * 365 + random.randint(0, 364))

        members.append({
            "member_id": member_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "date_of_birth": birth_date,
            "gender": random.choice(["M", "F"]),
            "email": fake.email(),
            "phone": fake.phone_number()[:14],
            "address_line1": fake.street_address(),
            "city": fake.city(),
            "state": state,
            "zip_code": fake.zipcode(),
            "region": region,
            "plan_type": plan_type,
            "plan_id": f"PLN{random.randint(1000, 9999)}",
            "member_segment": segment,
            "enrollment_date": enrollment_date,
            "pcp_provider_id": f"PCP{random.randint(10000, 99999)}",
            "communication_preference": random.choice(COMMUNICATION_PREFERENCES),
            "language_preference": np.random.choice(
                ["English", "Spanish", "Chinese", "Vietnamese", "Korean"],
                p=[0.85, 0.10, 0.02, 0.02, 0.01]
            ),
            "has_caregiver": random.random() < 0.15,
            "is_dual_eligible": plan_type == "Medicare Advantage" and random.random() < 0.20,
            "created_at": datetime.now(),
        })

    return members


members_data = generate_members()
members_df = spark.createDataFrame(pd.DataFrame(members_data))
members_df.write.mode("overwrite").parquet(f"{VOLUME_PATH}/members")
print(f"Generated {len(members_data)} members")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Policies Table

# COMMAND ----------

def generate_policies(members: List[Dict]) -> List[Dict[str, Any]]:
    """Generate policy records linked to members."""
    policies = []

    for member in members:
        policy_id = f"POL{member['member_id'][3:]}"

        # Premium based on plan type and age
        base_premium = {
            "Medicare Advantage": 0,  # Medicare pays
            "Commercial PPO": 450,
            "Commercial HMO": 350,
            "Medicaid": 0,
            "Individual Exchange": 400,
        }[member["plan_type"]]

        premium = base_premium * (1 + np.random.normal(0, 0.15))
        premium = max(0, premium)

        # Deductible based on plan
        deductible = {
            "Medicare Advantage": 0,
            "Commercial PPO": random.choice([1500, 2500, 3500, 5000]),
            "Commercial HMO": random.choice([500, 1000, 1500]),
            "Medicaid": 0,
            "Individual Exchange": random.choice([2000, 4000, 6000, 8000]),
        }[member["plan_type"]]

        policies.append({
            "policy_id": policy_id,
            "member_id": member["member_id"],
            "plan_type": member["plan_type"],
            "plan_id": member["plan_id"],
            "effective_date": member["enrollment_date"],
            "termination_date": None if random.random() > 0.05 else fake.date_between(
                start_date=member["enrollment_date"], end_date=today
            ),
            "monthly_premium": round(premium, 2),
            "annual_deductible": deductible,
            "deductible_met_amount": round(random.uniform(0, deductible), 2),
            "out_of_pocket_max": deductible * 3,
            "copay_primary_care": random.choice([0, 15, 20, 25, 30]),
            "copay_specialist": random.choice([25, 40, 50, 75]),
            "copay_emergency": random.choice([100, 150, 250, 500]),
            "coinsurance_percent": random.choice([0.10, 0.20, 0.30]),
            "status": "Active" if random.random() > 0.05 else "Terminated",
            "created_at": datetime.now(),
        })

    return policies


policies_data = generate_policies(members_data)
policies_df = spark.createDataFrame(pd.DataFrame(policies_data))
policies_df.write.mode("overwrite").parquet(f"{VOLUME_PATH}/policies")
print(f"Generated {len(policies_data)} policies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Claims Summary Table

# COMMAND ----------

CLAIM_TYPES = ["Medical", "Pharmacy", "Behavioral", "Dental", "Vision"]
CLAIM_CATEGORIES = [
    "Primary Care Visit",
    "Specialist Visit",
    "Emergency Room",
    "Inpatient Admission",
    "Outpatient Procedure",
    "Pharmacy Fill",
    "Lab/Diagnostic",
    "Behavioral Health",
    "Preventive Care",
    "Chronic Care Management",
]

# Realistic ICD-10 diagnosis codes by category
ICD10_CODES = {
    "Primary Care Visit": [
        ("Z00.00", "General adult medical examination"),
        ("J06.9", "Acute upper respiratory infection"),
        ("R51.9", "Headache, unspecified"),
        ("M54.5", "Low back pain"),
        ("R10.9", "Unspecified abdominal pain"),
        ("J02.9", "Acute pharyngitis, unspecified"),
        ("R05.9", "Cough, unspecified"),
        ("N39.0", "Urinary tract infection"),
    ],
    "Specialist Visit": [
        ("M17.11", "Primary osteoarthritis, right knee"),
        ("I10", "Essential hypertension"),
        ("E11.9", "Type 2 diabetes without complications"),
        ("K21.0", "GERD with esophagitis"),
        ("G43.909", "Migraine, unspecified"),
        ("M79.3", "Panniculitis, unspecified"),
        ("L40.0", "Psoriasis vulgaris"),
        ("H52.4", "Presbyopia"),
    ],
    "Emergency Room": [
        ("S52.501A", "Fracture of lower end of radius"),
        ("I21.9", "Acute myocardial infarction, unspecified"),
        ("J18.9", "Pneumonia, unspecified"),
        ("K35.80", "Acute appendicitis, unspecified"),
        ("S06.0X0A", "Concussion without loss of consciousness"),
        ("R07.9", "Chest pain, unspecified"),
        ("T78.40XA", "Allergy, unspecified"),
        ("R55", "Syncope and collapse"),
    ],
    "Inpatient Admission": [
        ("I50.9", "Heart failure, unspecified"),
        ("J44.1", "COPD with acute exacerbation"),
        ("A41.9", "Sepsis, unspecified organism"),
        ("I63.9", "Cerebral infarction, unspecified"),
        ("N17.9", "Acute kidney failure, unspecified"),
        ("K80.00", "Calculus of gallbladder with acute cholecystitis"),
        ("J96.01", "Acute respiratory failure with hypoxia"),
        ("I26.99", "Other pulmonary embolism"),
    ],
    "Outpatient Procedure": [
        ("K40.90", "Inguinal hernia, unspecified"),
        ("H25.9", "Age-related cataract, unspecified"),
        ("M23.50", "Chronic instability of knee"),
        ("K57.30", "Diverticulosis of large intestine"),
        ("N20.0", "Calculus of kidney"),
        ("C44.91", "Basal cell carcinoma of skin"),
        ("M75.10", "Rotator cuff tear, unspecified shoulder"),
        ("K80.20", "Calculus of gallbladder without cholecystitis"),
    ],
    "Pharmacy Fill": [
        ("E11.65", "Type 2 diabetes with hyperglycemia"),
        ("I10", "Essential hypertension"),
        ("E78.5", "Hyperlipidemia, unspecified"),
        ("F32.9", "Major depressive disorder, unspecified"),
        ("J45.909", "Asthma, unspecified"),
        ("M06.9", "Rheumatoid arthritis, unspecified"),
        ("G40.909", "Epilepsy, unspecified"),
        ("F41.1", "Generalized anxiety disorder"),
    ],
    "Lab/Diagnostic": [
        ("Z13.220", "Screening for lipid disorders"),
        ("Z00.00", "General adult medical examination"),
        ("R73.09", "Other abnormal glucose"),
        ("D64.9", "Anemia, unspecified"),
        ("E03.9", "Hypothyroidism, unspecified"),
        ("Z12.31", "Screening mammogram for malignant neoplasm"),
        ("Z12.11", "Screening colonoscopy"),
        ("R94.31", "Abnormal electrocardiogram"),
    ],
    "Behavioral Health": [
        ("F33.0", "Major depressive disorder, recurrent, mild"),
        ("F41.1", "Generalized anxiety disorder"),
        ("F10.20", "Alcohol dependence, uncomplicated"),
        ("F31.9", "Bipolar disorder, unspecified"),
        ("F43.10", "Post-traumatic stress disorder"),
        ("F90.9", "ADHD, unspecified type"),
        ("F50.9", "Eating disorder, unspecified"),
        ("F11.20", "Opioid dependence, uncomplicated"),
    ],
    "Preventive Care": [
        ("Z23", "Encounter for immunization"),
        ("Z12.31", "Screening mammogram"),
        ("Z12.11", "Encounter for screening colonoscopy"),
        ("Z00.00", "Annual physical examination"),
        ("Z01.411", "Gynecological examination with Pap smear"),
        ("Z13.6", "Screening for cardiovascular disorders"),
        ("Z13.1", "Screening for diabetes mellitus"),
        ("Z13.89", "Screening for other disorders"),
    ],
    "Chronic Care Management": [
        ("E11.9", "Type 2 diabetes mellitus without complications"),
        ("I10", "Essential hypertension"),
        ("J44.9", "COPD, unspecified"),
        ("I50.9", "Heart failure, unspecified"),
        ("N18.3", "Chronic kidney disease, stage 3"),
        ("E78.5", "Hyperlipidemia, unspecified"),
        ("G20", "Parkinson's disease"),
        ("G30.9", "Alzheimer's disease, unspecified"),
    ],
}

# Realistic CPT procedure codes by category
CPT_CODES = {
    "Primary Care Visit": [
        ("99213", "Office visit, established patient, low complexity"),
        ("99214", "Office visit, established patient, moderate complexity"),
        ("99203", "Office visit, new patient, low complexity"),
        ("99204", "Office visit, new patient, moderate complexity"),
        ("99395", "Periodic preventive visit, 18-39 years"),
        ("99396", "Periodic preventive visit, 40-64 years"),
    ],
    "Specialist Visit": [
        ("99243", "Office consultation, moderate complexity"),
        ("99244", "Office consultation, high complexity"),
        ("99214", "Office visit, established patient, moderate"),
        ("99215", "Office visit, established patient, high complexity"),
        ("92014", "Ophthalmological examination"),
        ("93000", "Electrocardiogram, complete"),
    ],
    "Emergency Room": [
        ("99283", "Emergency department visit, moderate severity"),
        ("99284", "Emergency department visit, high severity"),
        ("99285", "Emergency department visit, immediate threat"),
        ("99281", "Emergency department visit, minor"),
        ("99282", "Emergency department visit, low severity"),
    ],
    "Inpatient Admission": [
        ("99223", "Initial hospital care, high complexity"),
        ("99222", "Initial hospital care, moderate complexity"),
        ("99233", "Subsequent hospital care, high complexity"),
        ("99238", "Hospital discharge day management"),
        ("99291", "Critical care, first 30-74 minutes"),
    ],
    "Outpatient Procedure": [
        ("43239", "Upper GI endoscopy with biopsy"),
        ("45380", "Colonoscopy with biopsy"),
        ("29881", "Knee arthroscopy with meniscectomy"),
        ("66984", "Cataract surgery with IOL"),
        ("47562", "Laparoscopic cholecystectomy"),
        ("27447", "Total knee replacement"),
    ],
    "Pharmacy Fill": [
        ("90471", "Immunization administration"),
        ("99211", "Office visit, minimal problem"),
        ("96372", "Therapeutic injection, SC or IM"),
        ("J3490", "Unclassified drugs"),
        ("J0585", "Injection, onabotulinumtoxinA"),
    ],
    "Lab/Diagnostic": [
        ("80053", "Comprehensive metabolic panel"),
        ("80061", "Lipid panel"),
        ("85025", "Complete blood count with differential"),
        ("84443", "TSH, thyroid stimulating hormone"),
        ("82947", "Glucose, quantitative"),
        ("71046", "Chest X-ray, 2 views"),
        ("77067", "Screening mammography, bilateral"),
        ("74150", "CT abdomen without contrast"),
    ],
    "Behavioral Health": [
        ("90837", "Psychotherapy, 53-60 minutes"),
        ("90834", "Psychotherapy, 38-52 minutes"),
        ("90832", "Psychotherapy, 16-37 minutes"),
        ("90847", "Family psychotherapy with patient"),
        ("90853", "Group psychotherapy"),
        ("90792", "Psychiatric diagnostic evaluation"),
    ],
    "Preventive Care": [
        ("99381", "Preventive visit, infant"),
        ("99395", "Preventive visit, 18-39 years"),
        ("99396", "Preventive visit, 40-64 years"),
        ("99397", "Preventive visit, 65+ years"),
        ("77067", "Screening mammography"),
        ("45378", "Screening colonoscopy"),
        ("90471", "Immunization administration"),
    ],
    "Chronic Care Management": [
        ("99490", "Chronic care management, 20 min"),
        ("99491", "Chronic care management, 30 min"),
        ("99487", "Complex chronic care management"),
        ("G0438", "Annual wellness visit, initial"),
        ("G0439", "Annual wellness visit, subsequent"),
        ("99483", "Cognitive assessment and care plan"),
    ],
}


def generate_claims(members: List[Dict]) -> List[Dict[str, Any]]:
    """Generate claims records with realistic ICD-10, CPT codes, and cost distributions."""
    claims = []

    for member in members:
        # Number of claims based on segment
        segment_multiplier = {
            "High Value": 1.5,
            "Rising Risk": 1.3,
            "Chronic Care": 1.8,
            "Healthy": 0.6,
            "New Member": 0.4,
        }[member["member_segment"]]

        num_claims = int(np.random.poisson(NUM_CLAIMS_PER_MEMBER_AVG * segment_multiplier))

        for _ in range(num_claims):
            claim_date = fake.date_between(
                start_date=max(member["enrollment_date"], start_date),
                end_date=today
            )

            claim_type = np.random.choice(
                CLAIM_TYPES,
                p=[0.45, 0.30, 0.10, 0.10, 0.05]
            )

            claim_category = np.random.choice(CLAIM_CATEGORIES)

            # Get realistic ICD-10 and CPT codes for this category
            icd_code, icd_desc = random.choice(ICD10_CODES.get(claim_category, ICD10_CODES["Primary Care Visit"]))
            cpt_code, cpt_desc = random.choice(CPT_CODES.get(claim_category, CPT_CODES["Primary Care Visit"]))

            # Cost distribution based on realistic procedure pricing
            cost_ranges = {
                "Primary Care Visit": (85, 250),
                "Specialist Visit": (150, 450),
                "Emergency Room": (800, 5500),
                "Inpatient Admission": (8000, 85000),
                "Outpatient Procedure": (1200, 25000),
                "Pharmacy Fill": (15, 1200),
                "Lab/Diagnostic": (75, 2500),
                "Behavioral Health": (120, 350),
                "Preventive Care": (0, 350),  # Often covered at 100%
                "Chronic Care Management": (80, 200),
            }

            min_cost, max_cost = cost_ranges.get(claim_category, (100, 500))
            # Use lognormal within the range for realistic distribution
            log_mean = np.log((min_cost + max_cost) / 2)
            log_std = 0.5
            billed_amount = np.clip(np.random.lognormal(log_mean, log_std), min_cost, max_cost * 1.5)

            # Allowed amount is typically 40-80% of billed for in-network
            allowed_amount = billed_amount * np.random.uniform(0.45, 0.85)

            # Paid amount depends on plan type and preventive status
            if claim_category == "Preventive Care":
                paid_pct = 1.0  # Preventive care typically covered 100%
            elif member["plan_type"] == "Medicare Advantage":
                paid_pct = np.random.uniform(0.80, 0.95)
            elif member["plan_type"] in ["Commercial HMO", "Commercial PPO"]:
                paid_pct = np.random.uniform(0.70, 0.90)
            else:
                paid_pct = np.random.uniform(0.65, 0.85)

            paid_amount = allowed_amount * paid_pct
            member_responsibility = allowed_amount - paid_amount

            # Facility type based on category
            facility_map = {
                "Primary Care Visit": ["Clinic", "Medical Office"],
                "Specialist Visit": ["Clinic", "Medical Office", "Ambulatory"],
                "Emergency Room": ["Hospital"],
                "Inpatient Admission": ["Hospital"],
                "Outpatient Procedure": ["Hospital", "Ambulatory Surgery Center"],
                "Pharmacy Fill": ["Pharmacy", "Mail Order Pharmacy"],
                "Lab/Diagnostic": ["Lab", "Diagnostic Center", "Hospital"],
                "Behavioral Health": ["Clinic", "Behavioral Health Center"],
                "Preventive Care": ["Clinic", "Medical Office"],
                "Chronic Care Management": ["Clinic", "Medical Office"],
            }
            facility_type = random.choice(facility_map.get(claim_category, ["Clinic"]))

            claims.append({
                "claim_id": f"CLM{uuid.uuid4().hex[:12].upper()}",
                "member_id": member["member_id"],
                "claim_date": claim_date,
                "service_date": claim_date - timedelta(days=random.randint(0, 14)),
                "claim_type": claim_type,
                "claim_category": claim_category,
                "diagnosis_code": icd_code,
                "diagnosis_description": icd_desc,
                "procedure_code": cpt_code,
                "procedure_description": cpt_desc,
                "provider_id": f"PRV{random.randint(10000, 99999)}",
                "provider_name": fake.company() + " Healthcare",
                "facility_type": facility_type,
                "billed_amount": round(billed_amount, 2),
                "allowed_amount": round(allowed_amount, 2),
                "paid_amount": round(paid_amount, 2),
                "member_responsibility": round(member_responsibility, 2),
                "status": np.random.choice(
                    ["Paid", "Pending", "Denied", "Adjusted"],
                    p=[0.75, 0.10, 0.10, 0.05]
                ),
                "is_preventive": claim_category == "Preventive Care",
                "is_chronic_related": member["member_segment"] == "Chronic Care" and random.random() < 0.6,
                "created_at": datetime.now(),
            })

    return claims


claims_data = generate_claims(members_data)
claims_df = spark.createDataFrame(pd.DataFrame(claims_data))
claims_df.write.mode("overwrite").parquet(f"{VOLUME_PATH}/claims")
print(f"Generated {len(claims_data)} claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Interactions Table

# COMMAND ----------

INTERACTION_TYPES = [
    "Phone Call",
    "Secure Message",
    "Chat",
    "Email",
    "Portal Visit",
    "Mobile App",
    "IVR",
    "In-Person Visit",
]

INTERACTION_REASONS = [
    "Benefits Inquiry",
    "Claims Status",
    "Provider Search",
    "Authorization Request",
    "Billing Question",
    "ID Card Request",
    "Plan Change Inquiry",
    "Complaint",
    "Enrollment Question",
    "Prescription Inquiry",
    "Appointment Scheduling",
    "Care Coordination",
    "Wellness Program",
    "Annual Enrollment",
]

RESOLUTIONS = [
    "Resolved - First Contact",
    "Resolved - Callback",
    "Escalated to Supervisor",
    "Transferred to Department",
    "Pending Follow-up",
    "Member Abandoned",
]


def generate_interactions(members: List[Dict]) -> List[Dict[str, Any]]:
    """Generate member interaction records."""
    interactions = []

    for member in members:
        # Number of interactions based on segment
        segment_multiplier = {
            "High Value": 1.2,
            "Rising Risk": 1.4,
            "Chronic Care": 1.3,
            "Healthy": 0.5,
            "New Member": 1.5,  # New members call more
        }[member["member_segment"]]

        num_interactions = int(np.random.poisson(NUM_INTERACTIONS_PER_MEMBER_AVG * segment_multiplier))

        for _ in range(num_interactions):
            interaction_date = fake.date_between(
                start_date=max(member["enrollment_date"], start_date),
                end_date=today
            )

            interaction_type = np.random.choice(
                INTERACTION_TYPES,
                p=[0.30, 0.15, 0.10, 0.10, 0.15, 0.10, 0.05, 0.05]
            )

            reason = np.random.choice(INTERACTION_REASONS)

            # Generate call notes for phone/chat interactions
            is_voice_or_chat = interaction_type in ["Phone Call", "Chat", "In-Person Visit"]

            # Satisfaction and handle time vary by resolution
            resolution = np.random.choice(
                RESOLUTIONS,
                p=[0.55, 0.15, 0.08, 0.10, 0.07, 0.05]
            )

            if resolution == "Resolved - First Contact":
                satisfaction = np.random.choice([4, 5], p=[0.3, 0.7])
                handle_time = int(np.random.exponential(scale=8)) + 2
            elif resolution in ["Escalated to Supervisor", "Complaint"]:
                satisfaction = np.random.choice([1, 2, 3], p=[0.3, 0.4, 0.3])
                handle_time = int(np.random.exponential(scale=20)) + 10
            else:
                satisfaction = np.random.choice([2, 3, 4], p=[0.2, 0.4, 0.4])
                handle_time = int(np.random.exponential(scale=12)) + 5

            # Generate realistic transcript snippets that showcase AI sentiment analysis
            transcript = None
            if is_voice_or_chat:
                # Positive sentiment transcripts
                positive_templates = [
                    f"Member was extremely pleased with how quickly we resolved their {reason.lower()} issue. They mentioned our service is 'the best they've experienced with any insurance company.' Thanked the agent profusely.",
                    f"Called about {reason.lower()}. Member was delighted to learn about their {member['plan_type']} benefits. Said they recommend us to all their friends and family. Very satisfied customer.",
                    f"Member reached out regarding {reason.lower()}. They expressed gratitude for the clear explanation and said 'This is why I stay with this plan - you always take care of me.'",
                    f"Quick resolution on {reason.lower()}. Member praised our efficiency saying 'I was dreading this call but you made it so easy. Thank you so much!'",
                    f"Member inquired about {reason.lower()}. They were thrilled with the additional benefits we explained. Mentioned considering upgrading their plan next enrollment period.",
                ]

                # Negative sentiment transcripts
                negative_templates = [
                    f"Member extremely frustrated about {reason.lower()}. They've called three times already about this issue. Demanded to speak with a supervisor. Threatened to switch plans during next enrollment.",
                    f"Very upset caller regarding {reason.lower()}. Member stated 'This is absolutely unacceptable service. I've been waiting for resolution for weeks.' Had to escalate to retention team.",
                    f"Member complained loudly about {reason.lower()}. Said 'I pay too much for this kind of treatment. Your competitors would never do this.' Agent struggled to de-escalate.",
                    f"Angry call about {reason.lower()}. Member said they feel 'completely ignored and disrespected.' Previous callbacks were never received. Considering filing formal complaint.",
                    f"Member called in distress over {reason.lower()}. Expressed extreme dissatisfaction saying 'I've never experienced such poor customer service. This is my last resort before contacting the state insurance board.'",
                ]

                # Neutral/Mixed sentiment transcripts
                neutral_templates = [
                    f"Standard inquiry about {reason.lower()}. Member acknowledged the information but seemed uncertain. Said they would call back if they have more questions.",
                    f"Member called about {reason.lower()}. Interaction was brief and factual. Member neither expressed satisfaction nor dissatisfaction with the outcome.",
                    f"Routine call regarding {reason.lower()}. Member asked several clarifying questions about their {member['plan_type']} coverage. Call ended without clear sentiment.",
                    f"Member requested information about {reason.lower()}. They seemed confused about some aspects but appreciated the explanation. Said they need to think about it.",
                    f"Discussion about {reason.lower()}. Member had mixed feelings - appreciated the help but expressed concerns about processing times. Will monitor situation.",
                ]

                # Select transcript based on satisfaction score
                if satisfaction >= 4:
                    transcript = random.choice(positive_templates)
                elif satisfaction <= 2:
                    transcript = random.choice(negative_templates)
                else:
                    transcript = random.choice(neutral_templates)

            interactions.append({
                "interaction_id": f"INT{uuid.uuid4().hex[:12].upper()}",
                "member_id": member["member_id"],
                "interaction_date": interaction_date,
                "interaction_timestamp": datetime.combine(
                    interaction_date,
                    datetime.min.time()
                ) + timedelta(hours=random.randint(8, 20), minutes=random.randint(0, 59)),
                "interaction_type": interaction_type,
                "channel": interaction_type,
                "reason": reason,
                "sub_reason": f"{reason} - Detail",
                "agent_id": f"AGT{random.randint(1000, 9999)}" if is_voice_or_chat else None,
                "handle_time_minutes": handle_time if is_voice_or_chat else None,
                "queue_time_seconds": random.randint(10, 300) if is_voice_or_chat else None,
                "resolution": resolution,
                "satisfaction_score": satisfaction,
                "transcript_notes": transcript,
                "is_complaint": reason == "Complaint" or satisfaction <= 2,
                "is_escalated": "Escalated" in resolution,
                "follow_up_required": "Pending" in resolution,
                "created_at": datetime.now(),
            })

    return interactions


interactions_data = generate_interactions(members_data)
interactions_df = spark.createDataFrame(pd.DataFrame(interactions_data))
interactions_df.write.mode("overwrite").parquet(f"{VOLUME_PATH}/interactions")
print(f"Generated {len(interactions_data)} interactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. CRM Events Table

# COMMAND ----------

CRM_EVENT_TYPES = [
    "Lead Created",
    "Campaign Sent",
    "Campaign Opened",
    "Campaign Clicked",
    "Form Submitted",
    "Webinar Registered",
    "Webinar Attended",
    "Meeting Scheduled",
    "Meeting Completed",
    "Task Created",
    "Task Completed",
]

CAMPAIGN_TYPES = [
    "Annual Enrollment",
    "Wellness Program",
    "Disease Management",
    "Preventive Care Reminder",
    "Benefit Education",
    "Plan Upgrade",
    "Retention Outreach",
    "New Member Welcome",
    "Provider Change Notice",
    "Formulary Update",
]


def generate_crm_events(members: List[Dict]) -> List[Dict[str, Any]]:
    """Generate CRM/marketing event records."""
    events = []

    for member in members:
        # Number of CRM events
        num_events = int(np.random.poisson(5))

        for _ in range(num_events):
            event_date = fake.date_between(
                start_date=max(member["enrollment_date"], start_date),
                end_date=today
            )

            event_type = np.random.choice(CRM_EVENT_TYPES)
            campaign_type = np.random.choice(CAMPAIGN_TYPES)

            # Response rates vary by segment
            response_rate = {
                "High Value": 0.4,
                "Rising Risk": 0.25,
                "Chronic Care": 0.35,
                "Healthy": 0.15,
                "New Member": 0.30,
            }[member["member_segment"]]

            responded = random.random() < response_rate

            events.append({
                "event_id": f"CRM{uuid.uuid4().hex[:12].upper()}",
                "member_id": member["member_id"],
                "event_date": event_date,
                "event_timestamp": datetime.combine(
                    event_date,
                    datetime.min.time()
                ) + timedelta(hours=random.randint(6, 22)),
                "event_type": event_type,
                "campaign_id": f"CMP{random.randint(1000, 9999)}",
                "campaign_name": f"{campaign_type} - {fake.date_this_year().strftime('%b %Y')}",
                "campaign_type": campaign_type,
                "channel": member["communication_preference"],
                "responded": responded,
                "response_date": event_date + timedelta(days=random.randint(0, 7)) if responded else None,
                "utm_source": random.choice(["email", "sms", "portal", "app", "direct"]),
                "utm_campaign": campaign_type.lower().replace(" ", "_"),
                "created_at": datetime.now(),
            })

    return events


crm_events_data = generate_crm_events(members_data)
crm_events_df = spark.createDataFrame(pd.DataFrame(crm_events_data))
crm_events_df.write.mode("overwrite").parquet(f"{VOLUME_PATH}/crm_events")
print(f"Generated {len(crm_events_data)} CRM events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Digital Events Table

# COMMAND ----------

DIGITAL_EVENT_TYPES = [
    "Page View",
    "Login",
    "Logout",
    "Search",
    "Click",
    "Form Start",
    "Form Submit",
    "Download",
    "Video Play",
    "Error",
]

DIGITAL_PAGES = [
    "Home",
    "Benefits",
    "Claims",
    "Find Provider",
    "ID Cards",
    "My Account",
    "Pharmacy",
    "Wellness",
    "Messages",
    "Forms",
]


def generate_digital_events(members: List[Dict]) -> List[Dict[str, Any]]:
    """Generate digital engagement event records."""
    events = []

    for member in members:
        # Digital engagement varies
        if member["communication_preference"] in ["Portal", "Email"]:
            num_events = int(np.random.poisson(15))
        else:
            num_events = int(np.random.poisson(5))

        for _ in range(num_events):
            event_date = fake.date_between(
                start_date=max(member["enrollment_date"], start_date),
                end_date=today
            )

            events.append({
                "event_id": f"DIG{uuid.uuid4().hex[:12].upper()}",
                "member_id": member["member_id"],
                "session_id": f"SES{uuid.uuid4().hex[:8].upper()}",
                "event_date": event_date,
                "event_timestamp": datetime.combine(
                    event_date,
                    datetime.min.time()
                ) + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59)),
                "event_type": np.random.choice(DIGITAL_EVENT_TYPES),
                "page_name": np.random.choice(DIGITAL_PAGES),
                "device_type": np.random.choice(
                    ["Desktop", "Mobile", "Tablet"],
                    p=[0.4, 0.5, 0.1]
                ),
                "platform": np.random.choice(
                    ["Web Portal", "iOS App", "Android App"],
                    p=[0.5, 0.3, 0.2]
                ),
                "browser": np.random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
                "duration_seconds": int(np.random.exponential(scale=60)),
                "is_error": random.random() < 0.02,
                "created_at": datetime.now(),
            })

    return events


digital_events_data = generate_digital_events(members_data)
digital_events_df = spark.createDataFrame(pd.DataFrame(digital_events_data))
digital_events_df.write.mode("overwrite").parquet(f"{VOLUME_PATH}/digital_events")
print(f"Generated {len(digital_events_data)} digital events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Campaign Responses Table

# COMMAND ----------

def generate_campaign_responses(members: List[Dict]) -> List[Dict[str, Any]]:
    """Generate campaign response records."""
    responses = []

    for member in members:
        # Number of campaign responses
        num_responses = int(np.random.poisson(3))

        for _ in range(num_responses):
            response_date = fake.date_between(
                start_date=max(member["enrollment_date"], start_date),
                end_date=today
            )

            campaign_type = np.random.choice(CAMPAIGN_TYPES)

            # Outcome based on segment
            positive_outcome_rate = {
                "High Value": 0.5,
                "Rising Risk": 0.3,
                "Chronic Care": 0.4,
                "Healthy": 0.2,
                "New Member": 0.35,
            }[member["member_segment"]]

            outcome = np.random.choice(
                ["Converted", "Engaged", "No Response", "Opted Out"],
                p=[positive_outcome_rate * 0.5, positive_outcome_rate * 0.5,
                   (1 - positive_outcome_rate) * 0.9, (1 - positive_outcome_rate) * 0.1]
            )

            responses.append({
                "response_id": f"RSP{uuid.uuid4().hex[:12].upper()}",
                "member_id": member["member_id"],
                "campaign_id": f"CMP{random.randint(1000, 9999)}",
                "campaign_name": f"{campaign_type} Campaign",
                "campaign_type": campaign_type,
                "send_date": response_date - timedelta(days=random.randint(1, 14)),
                "response_date": response_date,
                "channel": member["communication_preference"],
                "outcome": outcome,
                "revenue_impact": round(np.random.lognormal(5, 1), 2) if outcome == "Converted" else 0,
                "cost": round(np.random.uniform(0.5, 5), 2),
                "attribution_model": "Last Touch",
                "created_at": datetime.now(),
            })

    return responses


campaign_responses_data = generate_campaign_responses(members_data)
campaign_responses_df = spark.createDataFrame(pd.DataFrame(campaign_responses_data))
campaign_responses_df.write.mode("overwrite").parquet(f"{VOLUME_PATH}/campaign_responses")
print(f"Generated {len(campaign_responses_data)} campaign responses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Care Management Interactions Table

# COMMAND ----------

CARE_PROGRAMS = [
    "Diabetes Management",
    "Heart Health",
    "Behavioral Health",
    "Maternity Care",
    "Oncology Support",
    "Chronic Pain Management",
    "Weight Management",
    "Smoking Cessation",
    "Medication Adherence",
    "Post-Discharge Follow-up",
]


def generate_care_management(members: List[Dict]) -> List[Dict[str, Any]]:
    """Generate care management interaction records."""
    care_interactions = []

    # Only some members have care management
    care_eligible = [m for m in members if m["member_segment"] in ["Chronic Care", "Rising Risk", "High Value"]]

    for member in care_eligible:
        if random.random() > 0.4:  # 40% of eligible members have care management
            continue

        program = np.random.choice(CARE_PROGRAMS)
        enrollment_date = fake.date_between(
            start_date=max(member["enrollment_date"], start_date),
            end_date=today - timedelta(days=30)
        )

        # Generate touchpoints
        num_touchpoints = int(np.random.poisson(6))

        for i in range(num_touchpoints):
            touchpoint_date = enrollment_date + timedelta(days=i * random.randint(7, 21))
            if touchpoint_date > today:
                break

            care_interactions.append({
                "care_interaction_id": f"CARE{uuid.uuid4().hex[:12].upper()}",
                "member_id": member["member_id"],
                "care_manager_id": f"CM{random.randint(100, 999)}",
                "program_name": program,
                "program_enrollment_date": enrollment_date,
                "interaction_date": touchpoint_date,
                "interaction_type": np.random.choice([
                    "Initial Assessment",
                    "Follow-up Call",
                    "Care Plan Review",
                    "Goal Setting",
                    "Barrier Discussion",
                    "Resource Referral",
                    "Medication Review",
                ]),
                "duration_minutes": int(np.random.normal(25, 10)),
                "goals_discussed": random.randint(1, 4),
                "barriers_identified": random.randint(0, 3),
                "referrals_made": random.randint(0, 2),
                "member_engagement_score": random.randint(1, 10),
                "notes": f"Care management touchpoint for {program}. Member {'highly engaged' if random.random() > 0.5 else 'needs additional support'}.",
                "next_touchpoint_date": touchpoint_date + timedelta(days=random.randint(7, 21)),
                "created_at": datetime.now(),
            })

    return care_interactions


care_management_data = generate_care_management(members_data)
care_management_df = spark.createDataFrame(pd.DataFrame(care_management_data))
care_management_df.write.mode("overwrite").parquet(f"{VOLUME_PATH}/care_management")
print(f"Generated {len(care_management_data)} care management interactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("DATA GENERATION COMPLETE")
print("=" * 60)
print(f"\nCatalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Volume: {VOLUME_PATH}")
print(f"\nGenerated files:")
print(f"  - members: {len(members_data):,} records")
print(f"  - policies: {len(policies_data):,} records")
print(f"  - claims: {len(claims_data):,} records")
print(f"  - interactions: {len(interactions_data):,} records")
print(f"  - crm_events: {len(crm_events_data):,} records")
print(f"  - digital_events: {len(digital_events_data):,} records")
print(f"  - campaign_responses: {len(campaign_responses_data):,} records")
print(f"  - care_management: {len(care_management_data):,} records")
print("\nRaw data is ready for pipeline processing.")
