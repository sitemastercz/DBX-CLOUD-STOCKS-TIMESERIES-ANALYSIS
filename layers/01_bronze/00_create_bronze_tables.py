# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC #00 Bronze DDL: Inicializace schématu a tabulek
# MAGIC
# MAGIC `Vytvoření databázového schématu bronze a definice struktury (DDL) surových Delta tabulek pro denní burzovní data a kvartální fundamentální výkazy.`
# MAGIC
# MAGIC Vytvářené tabulky:
# MAGIC `L_IN_DAILY_MARKET_DATA`
# MAGIC `L_IN_QUARTERLY_FUNDAMENTALS`

# COMMAND ----------

# MAGIC %md
# MAGIC <hr></hr>

# COMMAND ----------

# DBTITLE 1,Settings
# Načtení notebooku settings, který obsahuje primární konfiguraci.
%run ../../config/settings

# COMMAND ----------

# DBTITLE 1,Create bronze schema
# Import Pyspark komponent pro definici datových schémat a Delta Lake pro správu tabulek.
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    StringType,
    DoubleType,
    LongType,
    TimestampType
)
from delta.tables import DeltaTable

# Získání aktivní SparkSession (vstupního bodu pro práci s clusterem a katalogem).
spark = SparkSession.builder.getOrCreate()

# Nastavení aktivního katalogu a vytvoření schématu (proměnné jsou načteny z %run v buňce Settings).
spark.catalog.setCurrentCatalog(CATALOG_NAME)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_BRONZE}")
spark.catalog.setCurrentDatabase(SCHEMA_BRONZE)

# Výpis konfigurace pro kontrolu uživatelem.
print(f"Aktivní kontext nastaven na: {spark.catalog.currentCatalog()}.{spark.catalog.currentDatabase()}")

# COMMAND ----------

# DBTITLE 1,Create L_IN_DAILY_MARKET_DATA
# Pojmenování tabulky.
table_market = f"{SCHEMA_BRONZE}.L_IN_DAILY_MARKET_DATA"

# Definice tabulky.
(
    DeltaTable.createOrReplace(spark)
    .tableName(table_market)
    .comment("Surová denní tržní data technologických akcií a burzovních indexů z Yahoo Finance")
    .addColumn("DATE", DateType(), nullable=False, comment="Obchodní den")
    .addColumn("TICKER", StringType(), nullable=False, comment="Burzovní symbol")
    .addColumn("ASSET_TYPE", StringType(), nullable=False, comment="Kategorie aktiva: STOCK nebo BENCHMARK")
    .addColumn("OPEN", DoubleType(), nullable=True, comment="Otevírací cena v daný obchodní den (USD)")
    .addColumn("HIGH", DoubleType(), nullable=True, comment="Nejvyšší cena v daný obchodní den (USD)")
    .addColumn("LOW", DoubleType(), nullable=True, comment="Nejnižší cena v daný obchodní den (USD)")
    .addColumn("CLOSE", DoubleType(), nullable=True, comment="Uzavírací cena bez očištění o splity (USD)")
    .addColumn("ADJ_CLOSE", DoubleType(), nullable=True, comment="Uzavírací cena očištěná o splity akcií a dividendy (USD)")
    .addColumn("VOLUME", LongType(), nullable=True, comment="Objem obchodovaných akcií v daný den")
    .addColumn("DIVIDENDS", DoubleType(), nullable=True, comment="Vyplacená dividenda na akcii v daný den (USD)")
    .addColumn("STOCK_SPLITS", DoubleType(), nullable=True, comment="Poměr štěpení akcií v daný den")
    .addColumn("INSERTED_AT", TimestampType(), nullable=False, comment="UTC časové razítko stažení záznamu")
    .addColumn("SOURCE_SYSTEM", StringType(), nullable=False, comment="Zdrojový systém")
    # Rozdělení dat podle symbolu pro optimalizaci dotazování.
    .partitionedBy("TICKER")
    # Automatická optimalizace zápisu a slučování malých souborů v Delta Lake.
    .property("delta.autoOptimize.optimizeWrite", "true")
    .property("delta.autoOptimize.autoCompact", "true")
    # Uživatelská vlastnost definující příslušnost k vrstvě Medallion architektury BRONZE.
    .property("quality_layer", "BRONZE")
    .execute()
)

# Výpis konfigurace pro kontrolu uživatelem.
print(f"Tabulka '{table_market}' byla úspěšně vytvořena (createOrReplace).")