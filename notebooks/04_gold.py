# Databricks notebook source
# MAGIC %md
# MAGIC # 04_gold — table de faits et agrégats métier
# MAGIC
# MAGIC Les indicateurs excluent les commandes annulées (`statut like 'annul%'`).
# MAGIC
# MAGIC Code original du document source (cellule gold du PDF).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md ## gold_fact_ventes

# COMMAND ----------

l = spark.table(f"{SILVER}.silver_lignes_commandes").alias("l")
c = spark.table(f"{SILVER}.silver_commandes").alias("c")
cl = spark.table(f"{SILVER}.silver_clients").alias("cl")
a = spark.table(f"{SILVER}.silver_articles").alias("a")

fact_ventes = (
    l.join(c, F.col("l.id_commande") == F.col("c.id_commande"), "left")
    .join(cl, F.col("c.id_client") == F.col("cl.id_client"), "left")
    .join(a, F.col("l.id_article") == F.col("a.id_article"), "left")
    .select(
        F.col("l.id_commande"),
        F.col("c.date_commande"),
        F.date_trunc("month", F.col("c.date_commande")).alias("mois"),
        F.col("c.statut"),
        F.col("c.id_client"),
        F.col("cl.nom").alias("nom_client"),
        F.col("cl.ville").alias("ville_client"),
        F.col("l.id_article"),
        F.col("a.designation"),
        F.col("a.categorie"),
        F.col("l.quantite"),
        F.col("l.prix_unitaire"),
        F.col("l.montant_ligne"),
    )
)

save_table(
    fact_ventes,
    f"{GOLD}.gold_fact_ventes",
    "Faits de vente au niveau ligne, enrichis client et article",
)

# COMMAND ----------

fact = spark.table(f"{GOLD}.gold_fact_ventes")
ventes_valides = fact.filter(~F.coalesce(F.col("statut"), F.lit("")).like("annul%"))

# COMMAND ----------

# MAGIC %md ## gold_ca_mensuel

# COMMAND ----------

ca_mensuel = (
    ventes_valides.filter(F.col("mois").isNotNull())
    .groupBy("mois")
    .agg(
        F.countDistinct("id_commande").alias("nb_commandes"),
        F.countDistinct("id_client").alias("nb_clients"),
        F.sum("quantite").alias("quantite_vendue"),
        F.sum("montant_ligne").alias("chiffre_affaires"),
    )
    .withColumn(
        "panier_moyen",
        F.round(F.col("chiffre_affaires") / F.col("nb_commandes"), 2),
    )
    .orderBy("mois")
)

save_table(
    ca_mensuel,
    f"{GOLD}.gold_ca_mensuel",
    "CA, nb commandes, nb clients et panier moyen par mois",
)

# COMMAND ----------

# MAGIC %md ## gold_ca_par_categorie

# COMMAND ----------

ca_par_categorie = ventes_valides.groupBy(
    F.coalesce(F.col("categorie"), F.lit("Inconnue")).alias("categorie"),
    "id_article",
    "designation",
).agg(
    F.countDistinct("id_commande").alias("nb_commandes"),
    F.sum("quantite").alias("quantite_vendue"),
    F.sum("montant_ligne").alias("chiffre_affaires"),
)

save_table(
    ca_par_categorie,
    f"{GOLD}.gold_ca_par_categorie",
    "Quantités et CA par catégorie et article",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_ca_par_client
# MAGIC
# MAGIC Vue 360° client : tous les clients apparaissent, même ceux sans commande
# MAGIC (CA = 0).

# COMMAND ----------

clients = spark.table(f"{SILVER}.silver_clients")

ca_par_client = (
    clients.alias("cl")
    .join(
        ventes_valides.alias("f"),
        F.col("cl.id_client") == F.col("f.id_client"),
        "left",
    )
    .groupBy("cl.id_client", "cl.nom", "cl.ville", "cl.date_creation")
    .agg(
        F.countDistinct("f.id_commande").alias("nb_commandes"),
        F.coalesce(F.sum("f.montant_ligne"), F.lit(0)).alias("chiffre_affaires"),
        F.min("f.date_commande").alias("premiere_commande"),
        F.max("f.date_commande").alias("derniere_commande"),
    )
    .withColumn(
        "panier_moyen",
        F.when(
            F.col("nb_commandes") > 0,
            F.round(F.col("chiffre_affaires") / F.col("nb_commandes"), 2),
        ),
    )
    .select(
        "id_client",
        "nom",
        "ville",
        "date_creation",
        "nb_commandes",
        "chiffre_affaires",
        "panier_moyen",
        "premiere_commande",
        "derniere_commande",
    )
)

save_table(
    ca_par_client,
    f"{GOLD}.gold_ca_par_client",
    "Indicateurs par client (CA, nb commandes, panier moyen, 1re/dernière commande)",
)

# COMMAND ----------

# MAGIC %md ## Contrôles qualité Gold

# COMMAND ----------

resultats_tests.clear()
test_unique(f"{GOLD}.gold_ca_mensuel", "mois")
test_not_null(f"{GOLD}.gold_ca_mensuel", "mois")
test_unique(f"{GOLD}.gold_ca_par_client", "id_client")
test_not_null(f"{GOLD}.gold_ca_par_client", "id_client")
bilan_tests()

display(spark.table(f"{GOLD}.gold_ca_mensuel"))
