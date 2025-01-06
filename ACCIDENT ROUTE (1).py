# Databricks notebook source

dbutils.fs.mkdirs("dbfs:/bronze/vehicules/")
dbutils.fs.mkdirs("dbfs:/bronze/usagers/")
dbutils.fs.mkdirs("dbfs:/bronze/lieux/")


# COMMAND ----------


display(dbutils.fs.ls("dbfs:/bronze/"))


# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Créer une session Spark
spark = SparkSession.builder.appName("Usagers Cleaning").getOrCreate()

# Charger les données brutes
df_usagers = spark.read.csv("/FileStore/tables/usagers_2023.csv", header=True, inferSchema=True)

# Supprimer les doublons et gérer les valeurs nulles
df_usagers_clean = df_usagers.dropna().dropDuplicates()

# Sauvegarder dans la zone Silver
df_usagers_clean.write.format("parquet").mode("overwrite").save("dbfs:/silver/usagers/")


# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Créer une session Spark
spark = SparkSession.builder.appName("Vehicules Cleaning").getOrCreate()

# Charger les données brutes
df_vehicules = spark.read.csv("/FileStore/tables/vehicules_2023.csv", header=True, inferSchema=True)

# Supprimer les doublons et gérer les valeurs nulles
df_vehicules_clean = df_vehicules.dropna().dropDuplicates()

# Sauvegarder dans la zone Silver
df_vehicules_clean.write.format("parquet").mode("overwrite").save("dbfs:/silver/vehicules/")


# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Créer une session Spark
spark = SparkSession.builder.appName("Lieux Cleaning").getOrCreate()

# Charger les données brutes
df_lieux = spark.read.csv("/FileStore/tables/lieux_2023.csv", header=True, inferSchema=True)

# Supprimer les doublons et gérer les valeurs nulles
df_lieux_clean = df_lieux.dropna().dropDuplicates()

# Sauvegarder dans la zone Silver
df_lieux_clean.write.format("parquet").mode("overwrite").save("dbfs:/silver/lieux/")


# COMMAND ----------


display(dbutils.fs.ls("dbfs:/silver/vehicules/"))
display(dbutils.fs.ls("dbfs:/silver/usagers/"))
display(dbutils.fs.ls("dbfs:/silver/lieux/"))


# COMMAND ----------


dbutils.fs.cp("/FileStore/tables/vehicules_2023.csv", "dbfs:/bronze/vehicules/vehicules-2023.csv")


# COMMAND ----------


dbutils.fs.cp("/FileStore/tables/usagers_2023.csv", "dbfs:/bronze/usagers/usagers-2023.csv")

# COMMAND ----------


dbutils.fs.cp("/FileStore/tables/lieux_2023.csv", "dbfs:/bronze/lieux/lieux-2023.csv")

# COMMAND ----------


df_vehicules = spark.read.csv("dbfs:/bronze/vehicules/vehicules-2023.csv", header=True, inferSchema=True)
df_vehicules_clean = df_vehicules.dropna().dropDuplicates()
df_vehicules_clean.write.format("parquet").mode("overwrite").save("dbfs:/silver/vehicules/")


# COMMAND ----------


df_usagers = spark.read.csv("dbfs:/bronze/usagers/usagers-2023.csv", header=True, inferSchema=True)
df_usagers_clean = df_usagers.dropna().dropDuplicates()
df_usagers_clean.write.format("parquet").mode("overwrite").save("dbfs:/silver/usagers/")

# COMMAND ----------


df_vehicules = spark.read.csv("dbfs:/bronze/vehicules/vehicules-2023.csv", header=True, inferSchema=True)
df_vehicules_clean = df_vehicules.dropna().dropDuplicates()
df_vehicules_clean.write.format("parquet").mode("overwrite").save("dbfs:/silver/vehicules/")

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Créer une session Spark
spark = SparkSession.builder.appName("Accidents Cleaning").getOrCreate()

# Charger les fichiers CSV depuis la zone Bronze avec le bon séparateur
df_usagers = spark.read.option("header", True).option("sep", ";").csv("dbfs:/bronze/usagers/usagers-2023.csv")
df_vehicules = spark.read.option("header", True).option("sep", ";").csv("dbfs:/bronze/vehicules/vehicules-2023.csv")
df_lieux = spark.read.option("header", True).option("sep", ";").csv("dbfs:/bronze/lieux/lieux-2023.csv")

# Vérifiez les colonnes chargées
print("Schéma des données chargées :")
df_usagers.printSchema()
df_vehicules.printSchema()
df_lieux.printSchema()

# Assurons-nous que les types de Num_Acc sont cohérents
df_usagers = df_usagers.withColumn("Num_Acc", col("Num_Acc").cast("string"))
df_vehicules = df_vehicules.withColumn("Num_Acc", col("Num_Acc").cast("string"))
df_lieux = df_lieux.withColumn("Num_Acc", col("Num_Acc").cast("string"))

# Joindre les datasets sur Num_Acc pour créer la table des accidents
df_accidents = df_usagers.join(df_vehicules, "Num_Acc").join(df_lieux, "Num_Acc")

# Supprimer les colonnes en double
df_accidents = df_accidents.drop("id_vehicule", "num_veh")

# Afficher un aperçu des données après nettoyage
print("Aperçu des données des usagers après nettoyage :")
df_usagers.show(5)

print("Aperçu des données des véhicules après nettoyage :")
df_vehicules.show(5)

print("Aperçu des données des lieux après nettoyage :")
df_lieux.show(5)

# Sauvegarder la table finale dans la zone Gold
df_accidents.write.format("parquet").mode("overwrite").save("dbfs:/gold/accidents_model/")

# Vérifiez le contenu de la table sauvegardée dans la zone Gold
print("Contenu de la zone Gold :")
display(dbutils.fs.ls("dbfs:/gold/accidents_model/"))

# Afficher un aperçu de la table finale
print("Aperçu de la table des accidents après transformation :")
df_accidents.show(10)



# COMMAND ----------


# vérifiez le contenu de la table dans la zone Gold 
display(dbutils.fs.ls("dbfs:/gold/accidents_model/"))

