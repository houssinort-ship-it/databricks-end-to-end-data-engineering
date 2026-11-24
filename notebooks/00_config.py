# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config — paramètres et fonctions partagées
# MAGIC
# MAGIC Ce notebook ne traite aucune donnée. Il définit les paramètres et les
# MAGIC fonctions utilisées par `02_bronze`, `03_silver` et `04_gold` via
# MAGIC `%run ./00_config`.
# MAGIC
# MAGIC Code original du document source (paramètres + fonctions communes).
# MAGIC `transformations.py`, `tables.py`, `quality.py`.

# COMMAND ----------

# Paramètres du pipeline
CATALOG = "workspace"
RAW_PATH = "/Volumes/workspace/default/b2_raw"
CSV_SEP = ","

BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window


def read_raw(nom_fichier):
    """Lit un CSV brut du Volume : toutes les colonnes restent en texte
    (le typage propre se fait en silver)."""
    return (
        spark.read.option("header", True)
        .option("sep", CSV_SEP)
        .option("inferSchema", False)
        .csv(f"{RAW_PATH}/{nom_fichier}")
    )


def save_table(df, table, description=None):
    """Écrit un DataFrame en table Delta (écrasement complet) et ajoute un
    commentaire."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    if description:
        spark.sql(f"COMMENT ON TABLE {table} IS '{description}'")
    print(f"✅ {table} : {spark.table(table).count()} lignes")

# COMMAND ----------


def to_decimal(col, precision=12, scale=2):
    """Texte -> décimal : accepte "12,50", "12.50", "12,50 ", " 1 200,00 " ..."""
    return F.expr(
        f"""
        try_cast(
            regexp_replace(replace(trim(cast(`{col}` as string)), ',', '.'),
                           '[^0-9.-]', '')
            as decimal({precision}, {scale}))"""
    )


def to_int(col):
    """Texte -> entier"""
    return F.expr(
        f"try_cast(regexp_replace(trim(cast(`{col}` as string)), '[^0-9-]', '') as int)"
    )


def to_bigint(col):
    """Texte -> identifiant numérique"""
    return F.expr(f"try_cast(trim(`{col}`) as bigint)")


def to_date_multi(col):
    """Texte -> date : essaie plusieurs formats courants"""
    c = f"trim(cast(`{col}` as string))"
    return F.expr(
        f"""
        to_date(coalesce(
            try_to_timestamp({c}, 'yyyy-MM-dd'),
            try_to_timestamp({c}, 'dd/MM/yyyy'),
            try_to_timestamp({c}, 'dd-MM-yyyy'),
            try_to_timestamp({c}, 'yyyy/MM/dd'),
            try_to_timestamp({c}, 'yyyy-MM-dd HH:mm:ss')))"""
    )


def clean_str(col):
    """Supprime les espaces et transforme une chaîne vide en NULL"""
    return F.expr(f"nullif(trim(`{col}`), '')")


def dedup(df, keys, order_col="_ingested_at"):
    """Garde une seule ligne par clé (la plus récente)"""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
    )

# COMMAND ----------

# Fonctions de contrôle qualité
# Chaque contrôle a une sévérité : error (le notebook s'arrête) ou warn
# (simple avertissement).
resultats_tests = []


def _report(table, test, nb_ko, severity):
    statut = "OK" if nb_ko == 0 else ("WARN" if severity == "warn" else "ERROR")
    resultats_tests.append((table, test, nb_ko, statut))
    print(f"[{statut}] {table} - {test} : {nb_ko} ligne(s) en anomalie")


def test_not_null(table, col, severity="error"):
    nb = spark.table(table).filter(F.col(col).isNull()).count()
    _report(table, f"not_null({col})", nb, severity)


def test_unique(table, cols, severity="error"):
    cols = [cols] if isinstance(cols, str) else cols
    nb = spark.table(table).groupBy(*cols).count().filter("count > 1").count()
    _report(table, f"unique({', '.join(cols)})", nb, severity)


def test_relationship(table, col, ref_table, ref_col, severity="warn"):
    enfants = spark.table(table).filter(F.col(col).isNotNull())
    parents = spark.table(ref_table).select(F.col(ref_col).alias(col))
    nb = enfants.join(parents, on=col, how="left_anti").count()
    _report(table, f"relationship({col} -> {ref_table}.{ref_col})", nb, severity)


def bilan_tests():
    df = spark.createDataFrame(
        resultats_tests,
        "table string, test string, nb_ko long, statut string",
    )
    display(df)
    erreurs = [r for r in resultats_tests if r[3] == "ERROR"]
    if erreurs:
        raise Exception(f"{len(erreurs)} test(s) bloquant(s) en échec")
