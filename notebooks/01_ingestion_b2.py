# Databricks notebook source
# MAGIC %md
# MAGIC # 01_ingestion_b2 — Backblaze B2 → Unity Catalog Volume
# MAGIC
# MAGIC Version du PDF, avec une seule modification : les identifiants ne sont plus
# MAGIC écrits en dur (`"ton_keyID"`). Ils viennent des secrets Databricks ou de
# MAGIC variables d'environnement du cluster.
# MAGIC
# MAGIC Prérequis (une fois) :
# MAGIC
# MAGIC ```sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.b2_raw;
# MAGIC ```
# MAGIC
# MAGIC Configuration des secrets (recommandé) :
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets create-scope b2
# MAGIC databricks secrets put-secret b2 key_id
# MAGIC databricks secrets put-secret b2 app_key
# MAGIC ```
# MAGIC
# MAGIC Code original du document source (cellule ingestion B2 du PDF).
# MAGIC Code original du document source (cellule ingestion B2 du PDF).

# COMMAND ----------

# MAGIC %pip install boto3 -q

# COMMAND ----------

import os

import boto3

B2_ENDPOINT = os.getenv("B2_ENDPOINT", "https://s3.eu-central-003.backblazeb2.com")
BUCKET = os.getenv("B2_BUCKET", "atelier8-lakehouse")

# Les identifiants ne sont jamais écrits dans le notebook.
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

# Test : lister les fichiers du bucket
for obj in s3.list_objects_v2(Bucket=BUCKET).get("Contents", []):
    print(obj["Key"], obj["Size"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copier les fichiers dans le Volume
# MAGIC
# MAGIC Dans Backblaze B2 (comme dans Amazon S3), il n'y a pas vraiment de
# MAGIC dossiers : chaque fichier a un nom complet, appelé « clé ». Le préfixe est
# MAGIC le début de ce nom ; adapter `prefix` au dossier qui vous intéresse.

# COMMAND ----------

import os

prefix = os.getenv("B2_PREFIX", "")  # "" = tout le bucket, ou "data/ventes/"
dest = "/Volumes/workspace/default/b2_raw"

for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", []):
    key = obj["Key"]
    if key.endswith("/"):
        continue
    local = os.path.join(dest, key)
    os.makedirs(os.path.dirname(local), exist_ok=True)
    s3.download_file(BUCKET, key, local)
    print("OK", key)

# COMMAND ----------

# Lire avec Spark
path = f"/Volumes/workspace/default/b2_raw/{prefix}"

df = spark.read.csv(path, header=True, inferSchema=True)  # CSV
# df = spark.read.parquet(path)                           # Parquet
# df = spark.read.json(path)                              # JSON

display(df)

# COMMAND ----------

# Enregistrer en table (démo)
df.write.mode("overwrite").saveAsTable("workspace.default.ma_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.ma_table LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renvoyer des résultats vers B2 (démo)
# MAGIC
# MAGIC Écrire d'abord les résultats dans le Volume, puis les envoyer.

# COMMAND ----------

import os

out_dir = "/Volumes/workspace/default/b2_raw/export"
os.makedirs(out_dir, exist_ok=True)  # crée le dossier s'il n'existe pas
out = f"{out_dir}/resultat.csv"
df.toPandas().to_csv(out, index=False)
s3.upload_file(out, BUCKET, "export/resultat.csv")
print("Envoyé vers B2 :", "export/resultat.csv")

# COMMAND ----------

# Vérification : les objets exportés sont bien dans le bucket
for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix="export/").get("Contents", []):
    print(obj["Key"], obj["Size"], obj["LastModified"])
