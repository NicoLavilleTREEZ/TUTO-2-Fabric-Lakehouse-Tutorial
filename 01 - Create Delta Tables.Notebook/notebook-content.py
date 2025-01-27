# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b6641329-39a3-41c7-8d62-9afd4f2710b7",
# META       "default_lakehouse_name": "wwilakehouse",
# META       "default_lakehouse_workspace_id": "b2679ed0-5ef9-4181-915e-34dab745a1c7",
# META       "known_lakehouses": [
# META         {
# META           "id": "82e78e41-11a3-448e-a1aa-0bc48fc09cb6"
# META         },
# META         {
# META           "id": "2c52a91e-6ef4-4364-a525-33ceae21618a"
# META         },
# META         {
# META           "id": "b6641329-39a3-41c7-8d62-9afd4f2710b7"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Spark session configuration
# This cell sets Spark session settings to enable _Verti-Parquet_ and _Optimize on Write_. More details about _Verti-Parquet_ and _Optimize on Write_ in tutorial document.

# CELL ********************

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fact - Sale
# 
# This cell reads raw data from the _Files_ section of the lakehouse, adds additional columns for different date parts and the same information is being used to create partitioned fact delta table.

# CELL ********************

from pyspark.sql.functions import col, year, month, quarter

table_name = 'fact_sale'

df = spark.read.format("parquet").load('Files/wwi-raw-data/WideWorldImportersDW/parquet/full/fact_sale_1y_full')
df = df.withColumn('Year', year(col("InvoiceDateKey")))
df = df.withColumn('Quarter', quarter(col("InvoiceDateKey")))
df = df.withColumn('Month', month(col("InvoiceDateKey")))

df.write.mode("overwrite").format("delta").partitionBy("Year","Quarter").save("Tables/" + table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dimensions
# This cell creates a function to read raw data from the _Files_ section of the lakehouse for the table name passed as a parameter. Next, it creates a list of dimension tables. Finally, it has a _for loop_ to loop through the list of tables and call above function with each table name as parameter to read data for that specific table and create delta table.

# CELL ********************

from pyspark.sql.types import *

def loadFullDataFromSource(table_name):
    df = spark.read.format("parquet").load('Files/wwi-raw-data/WideWorldImportersDW/parquet/full/' + table_name)
    df = df.select([c for c in df.columns if c != 'Photo'])
    df.write.mode("overwrite").format("delta").save("Tables/" + table_name)

full_tables = [
    'dimension_city',
    'dimension_customer',
    'dimension_date',
    'dimension_employee',
    'dimension_stock_item'
    ]

for table in full_tables:
    loadFullDataFromSource(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql import SparkSession

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("Load Full Data to Delta") \
    .getOrCreate()

def loadFullDataFromSource(table_name):
    try:
        # Charger les données depuis le dossier parquet
        df = spark.read.format("parquet").load(f'Files/wwi-raw-data/WideWorldImportersDW/parquet/full/{table_name}')
        
        # Exclure la colonne 'Photo' si elle existe
        if 'Photo' in df.columns:
            df = df.select([c for c in df.columns if c != 'Photo'])
        
        # Définir le chemin de la table Delta
        delta_path = f"Tables/{table_name}"
        
        # Écriture des données au format Delta avec gestion des conflits de schéma
        df.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(delta_path)
        print(f"Table {table_name} chargée avec succès dans {delta_path}")
    
    except Exception as e:
        print(f"Erreur lors du traitement de la table {table_name}: {str(e)}")

# Liste des tables à traiter
full_tables = [
    'dimension_city',
    'dimension_customer',
    'dimension_date',
    'dimension_employee',
    'dimension_stock_item'
]

# Traitement des tables
for table in full_tables:
    loadFullDataFromSource(table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
