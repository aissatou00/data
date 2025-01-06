# Databricks notebook source
from pyspark.sql import SparkSession

dbutils.fs.mkdirs("dbfs:/bronze/vehicules/")
dbutils.fs.mkdirs("dbfs:/bronze/usagers/")
dbutils.fs.mkdirs("dbfs:/bronze/lieux/")

# Initialiser SparkSession
spark = SparkSession.builder.appName("Nettoyage et Vérification des Colonnes").getOrCreate()

# Lire les fichiers CSV dans des DataFrames avec le bon séparateur (;) et gestion des guillemets
df_vehicules = spark.read.csv('/FileStore/tables/vehicules_2023.csv', header=True, inferSchema=True, sep=';', quote='"')
df_usagers = spark.read.csv('/FileStore/tables/usagers_2023.csv', header=True, inferSchema=True, sep=';', quote='"')
df_lieux = spark.read.csv('/FileStore/tables/lieux_2023.csv', header=True, inferSchema=True, sep=';', quote='"')

# Copier les fichiers dans bronze
dbutils.fs.cp("/FileStore/tables/vehicules_2023.csv", "dbfs:/bronze/vehicules/vehicules-2023.csv")
dbutils.fs.cp("/FileStore/tables/usagers_2023.csv", "dbfs:/bronze/usagers/usagers-2023.csv")
dbutils.fs.cp("/FileStore/tables/lieux_2023.csv", "dbfs:/bronze/lieux/lieux-2023.csv")

# Lire les fichiers CSV dans des DataFrames avec le bon séparateur (;) et gestion des guillemets
df_vehicules = spark.read.csv('/bronze/vehicules/vehicules-2023.csv', header=True, inferSchema=True, sep=';', quote='"')
df_usagers = spark.read.csv('/bronze/usagers/usagers-2023.csv', header=True, inferSchema=True, sep=';', quote='"')
df_lieux = spark.read.csv('/bronze/lieux/lieux-2023.csv', header=True, inferSchema=True, sep=';', quote='"')



# Nettoyer les noms de colonnes pour enlever les guillemets
df_vehicules = df_vehicules.toDF(*[col.strip('"') for col in df_vehicules.columns])
df_usagers = df_usagers.toDF(*[col.strip('"') for col in df_usagers.columns])
df_lieux = df_lieux.toDF(*[col.strip('"') for col in df_lieux.columns])

# Afficher les colonnes des fichiers CSV
print("Colonnes dans df_vehicules : ", df_vehicules.columns)
print("Colonnes dans df_usagers : ", df_usagers.columns)
print("Colonnes dans df_lieux : ", df_lieux.columns)

# Vérifier si la colonne Num_Acc existe dans chaque DataFrame
if 'Num_Acc' not in df_vehicules.columns:
    print("La colonne 'Num_Acc' est manquante dans df_vehicules")
else:
    print("La colonne 'Num_Acc' est présente dans df_vehicules")

if 'Num_Acc' not in df_usagers.columns:
    print("La colonne 'Num_Acc' est manquante dans df_usagers")
else:
    print("La colonne 'Num_Acc' est présente dans df_usagers")

if 'Num_Acc' not in df_lieux.columns:
    print("La colonne 'Num_Acc' est manquante dans df_lieux")
else:
    print("La colonne 'Num_Acc' est présente dans df_lieux")

# Si la colonne est présente dans tous les DataFrames, procéder au nettoyage
if 'Num_Acc' in df_vehicules.columns and 'Num_Acc' in df_usagers.columns and 'Num_Acc' in df_lieux.columns:
    # Suppression des données manquantes
    df_vehicules_cleaned = df_vehicules.dropna()
    df_usagers_cleaned = df_usagers.dropna()
    df_lieux_cleaned = df_lieux.dropna()

    # Suppression des doublons
    df_vehicules_cleaned = df_vehicules_cleaned.dropDuplicates()
    df_usagers_cleaned = df_usagers_cleaned.dropDuplicates()
    df_lieux_cleaned = df_lieux_cleaned.dropDuplicates()

    # Afficher un aperçu des données après nettoyage
    print("Données des véhicules :")
    df_vehicules_cleaned.show(5)

    print("Données des usagers :")
    df_usagers_cleaned.show(5)

    print("Données des lieux :")
    df_lieux_cleaned.show(5)

    # Enregistrer les données nettoyées dans la zone Silver
    df_vehicules_cleaned.write.csv('/dbfs:/silver/vehicules_clean.csv', header=True, mode='overwrite')
    df_usagers_cleaned.write.csv('/dbfs:/silver/usagers_clean.csv', header=True, mode='overwrite')
    df_lieux_cleaned.write.csv('/dbfs:/silver/lieux_clean.csv', header=True, mode='overwrite')

    print("Les données nettoyées ont été enregistrées dans la zone Silver.")
else:
    print("Erreur : La colonne 'Num_Acc' est manquante dans un ou plusieurs fichiers CSV. Veuillez vérifier les fichiers.")

