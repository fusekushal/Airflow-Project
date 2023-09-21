import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("sparkfunction")\
        .config("spark.jars", "/usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.5.0.jar").getOrCreate()


df=spark.read.format("csv").option("header","true").load("/tmp/giveaways.csv")



db_url = "jdbc:postgresql://localhost:5432/airflow_input"
table_name = "giv_input_af"
properties = {
    "user": "postgres",
    "password": "kushal2psg",
    "driver": "org.postgresql.Driver"
}

#Selecting only 3 columns ID, Title and Worth
df = df.select(col("id"), col("title"), col("worth"))
df.write.parquet("/home/kushal/airflow/Airflow_output/giveaways.parquet", mode="overwrite")

df.write.mode("overwrite").jdbc(url=db_url, table=table_name, properties=properties)
df.show()
spark.stop()