# Databricks notebook source
# MAGIC %md
# MAGIC # 05_export_gold_b2 — export des tables Gold vers Backblaze B2
# MAGIC
# MAGIC Code original du document source (cellule export B2 du PDF).

# COMMAND ----------

# MAGIC %pip install boto3 -q

# COMMAND ----------

import os

import boto3

B2_ENDPOINT = os.getenv("B2_ENDPOINT", "https://s3.eu-central-003.backblazeb2.com")
BUCKET = os.getenv("B2_BUCKET", "atelier8-lakehouse")

try:
    B2_KEY_ID = dbutils.secrets.get(scope="b2", key="key_id")
    B2_APP_KEY = dbutils.secrets.get(scope="b2", key="app_key")
except Exception:
    B2_KEY_ID = os.environ["B2_KEY_ID"]
    B2_APP_KEY = os.environ["B2_APP_KEY"]

s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_KEY_ID,
    aws_secret_access_key=B2_APP_KEY,
)

out_dir = "/Volumes/workspace/default/b2_raw/export"
os.makedirs(out_dir, exist_ok=True)

tables_gold = [
    "gold_fact_ventes",
    "gold_ca_mensuel",
    "gold_ca_par_client",
    "gold_ca_par_categorie",
]

for t in tables_gold:
    local = f"{out_dir}/{t}.csv"
    spark.table(f"workspace.gold.{t}").toPandas().to_csv(local, index=False)
    s3.upload_file(local, BUCKET, f"gold/{t}.csv")
    print("Envoyé vers B2 :", f"gold/{t}.csv")

# COMMAND ----------

# Vérification : les fichiers gold/ sont bien arrivés
for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix="gold/").get("Contents", []):
    print(obj["Key"], obj["Size"], obj["LastModified"])
