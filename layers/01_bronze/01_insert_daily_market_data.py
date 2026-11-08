# Databricks notebook source
# MAGIC %md
# MAGIC #01 Bronze Insert: Stažení a uložení denních tržních dat
# MAGIC
# MAGIC `Stažení denních historických dat z rozhraní Yahoo Finance pro vybrané cloudové společnosti, bez úpravy hodnot a jejich uložení do tabulky.`
# MAGIC
# MAGIC Plnění tabulky:
# MAGIC `L_IN_DAILY_MARKET_DATA`

# COMMAND ----------

# MAGIC %md
# MAGIC <hr></hr>

# COMMAND ----------

# DBTITLE 1,Settings
# Načtení notebooku settings, který obsahuje primární konfiguraci.
%run ../../config/settings

# COMMAND ----------

# DBTITLE 1,Install dependencies
# Instalace knihovny yfinance v aktuální session (Spark clusteru).
%pip install yfinance --quiet

# COMMAND ----------

# DBTITLE 1,Define market data insert function
# Import potřebných knihoven pro stažení dat a práci s časovou složkou v PySpark.
from pyspark.sql import SparkSession
from datetime import datetime, timezone
import pandas as pd
import yfinance as yf

# Získání aktivní session a nastavení katalogu se schématem dle settings.
spark = SparkSession.builder.getOrCreate()
spark.catalog.setCurrentCatalog(CATALOG_NAME)
spark.catalog.setCurrentDatabase(SCHEMA_BRONZE)