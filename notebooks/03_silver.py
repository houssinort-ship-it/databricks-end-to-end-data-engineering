# Databricks notebook source
# MAGIC %md
# MAGIC # 03_silver — typage, nettoyage, dédoublonnage, contrôles qualité
# MAGIC
# MAGIC Code original du document source (cellule silver du PDF).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md ## silver_articles

# COMMAND ----------

src = spark.table(f"{BRONZE}.bronze_articles").select(
    to_bigint("id_article").alias("id_article"),
    clean_str("designation").alias("designation"),
    F.initcap(clean_str("categorie")).alias("categorie"),
    to_decimal("prix").alias("prix"),
    "_ingested_at",
)

silver_articles = dedup(src.filter(F.col("id_article").isNotNull()), ["id_article"])
save_table(silver_articles, f"{SILVER}.silver_articles", "Articles typés, prix en décimal")

# COMMAND ----------

# MAGIC %md ## silver_clients

# COMMAND ----------

src = spark.table(f"{BRONZE}.bronze_clients").select(
    to_bigint("id_client").alias("id_client"),
    clean_str("nom").alias("nom"),
    F.initcap(clean_str("ville")).alias("ville"),
    to_date_multi("date_creation").alias("date_creation"),
    "_ingested_at",
)

silver_clients = dedup(src.filter(F.col("id_client").isNotNull()), ["id_client"])
save_table(
    silver_clients,
    f"{SILVER}.silver_clients",
    "Clients typés, nettoyés, sans doublon",
)

# COMMAND ----------

# MAGIC %md ## silver_commandes

# COMMAND ----------

src = spark.table(f"{BRONZE}.bronze_commandes").select(
    to_bigint("id_commande").alias("id_commande"),
    to_bigint("id_client").alias("id_client"),
    to_date_multi("date_commande").alias("date_commande"),
    F.lower(clean_str("statut")).alias("statut"),
    "_ingested_at",
)

silver_commandes = dedup(src.filter(F.col("id_commande").isNotNull()), ["id_commande"])
save_table(
    silver_commandes,
    f"{SILVER}.silver_commandes",
    "Commandes typées, statut en minuscules",
)

# COMMAND ----------

# MAGIC %md ## silver_lignes_commandes

# COMMAND ----------

src = spark.table(f"{BRONZE}.bronze_lignes_commandes").select(
    to_bigint("id_commande").alias("id_commande"),
    to_bigint("id_article").alias("id_article"),
    to_int("quantite").alias("quantite"),
    to_decimal("prix_unitaire").alias("prix_unitaire"),
    "_ingested_at",
)

silver_lignes = dedup(
    src.filter(
        F.col("id_commande").isNotNull() & F.col("id_article").isNotNull()
    ),
    ["id_commande", "id_article"],
).withColumn("montant_ligne", F.col("quantite") * F.col("prix_unitaire"))

save_table(
    silver_lignes,
    f"{SILVER}.silver_lignes_commandes",
    "Lignes de commande typées + montant_ligne = quantite * prix_unitaire",
)

# COMMAND ----------

# MAGIC %md ## Contrôles qualité Silver

# COMMAND ----------

resultats_tests.clear()

# Clients
test_unique(f"{SILVER}.silver_clients", "id_client")
test_not_null(f"{SILVER}.silver_clients", "id_client")
test_not_null(f"{SILVER}.silver_clients", "date_creation", severity="warn")  # date illisible

# Articles
test_unique(f"{SILVER}.silver_articles", "id_article")
test_not_null(f"{SILVER}.silver_articles", "id_article")
test_not_null(f"{SILVER}.silver_articles", "prix", severity="warn")

# Commandes
test_unique(f"{SILVER}.silver_commandes", "id_commande")
test_not_null(f"{SILVER}.silver_commandes", "id_commande")
test_relationship(
    f"{SILVER}.silver_commandes", "id_client",
    f"{SILVER}.silver_clients", "id_client",
)  # client inconnu
test_not_null(f"{SILVER}.silver_commandes", "date_commande", severity="warn")

# Lignes de commande
test_not_null(f"{SILVER}.silver_lignes_commandes", "id_commande")
test_not_null(f"{SILVER}.silver_lignes_commandes", "id_article")
test_relationship(
    f"{SILVER}.silver_lignes_commandes", "id_commande",
    f"{SILVER}.silver_commandes", "id_commande",
)
test_relationship(
    f"{SILVER}.silver_lignes_commandes", "id_article",
    f"{SILVER}.silver_articles", "id_article",
)
test_not_null(f"{SILVER}.silver_lignes_commandes", "quantite", severity="warn")

bilan_tests()
