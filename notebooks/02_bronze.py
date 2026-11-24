# Databricks notebook source
# MAGIC %md
# MAGIC # 02_bronze — copie brute des CSV en tables Delta
# MAGIC
# MAGIC La couche bronze est une copie brute des fichiers : aucune transformation,
# MAGIC toutes les colonnes restent en texte. On ajoute seulement la date
# MAGIC d'ingestion et le fichier source.
# MAGIC
# MAGIC Code original du document source (cellule bronze du PDF).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

sources = {
    # table bronze : (fichier CSV, description)
    "bronze_clients": (
        "clients.csv",
        "Copie brute de clients.csv (toutes colonnes en texte)",
    ),
    "bronze_articles": ("articles.csv", "Copie brute de articles.csv"),
    "bronze_commandes": ("commandes.csv", "Copie brute de commandes.csv"),
    "bronze_lignes_commandes": (
        "lignes_commandes.csv",
        "Copie brute de lignes_commandes.csv",
    ),
}

for table, (fichier, description) in sources.items():
    df = read_raw(fichier).select(
        "*",
        F.current_timestamp().alias("_ingested_at"),
        F.col("_metadata.file_path").alias("_source_file"),
    )

    save_table(df, f"{BRONZE}.{table}", description)

# COMMAND ----------

display(spark.table(f"{BRONZE}.bronze_clients").limit(10))
