# Databricks notebook source
# MAGIC %md
# MAGIC #Settings
# MAGIC
# MAGIC `Primání konfigurace dimplomové práce, která je využívána napříč notebooky a vrstvami.`

# COMMAND ----------

# MAGIC %md
# MAGIC <hr></hr>

# COMMAND ----------

# DBTITLE 1,Code
# Databricks schémata
CATALOG_NAME = "workspace"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
SCHEMA_RESULTS = "results"

# Hlavní analyzované technologické firmy (Cloud computing)
STOCK_TICKERS = ["MSFT", "AMZN", "GOOGL"]

# Sledované období: Q1 2016 až konec Q3 2026 (10,75 roku = 43 ucelených kvartálů)
START_DATE = "2016-01-01"
END_DATE = "2026-10-01"  # yfinance end je exkluzivní