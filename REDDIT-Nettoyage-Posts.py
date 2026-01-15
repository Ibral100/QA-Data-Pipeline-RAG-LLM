from minio.error import S3Error
from io import BytesIO
from config_Api_Reddit import *
from config_miniO import *
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,row_number
from pyspark.sql import Window
from config_source_dest import *






# Initialiser la session Spark

spark = SparkSession.builder \
    .appName("Nettoyage des posts") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()




## nettoyage des posts Reddit
themesource=theme_source_Reddit
themedesti=theme_dest


df_posts = spark.read.csv(
    f"s3a://donneesbrutes/reddit/{themesource}/posts.csv",
    header=True,
    inferSchema=True,
    sep=",",  # séparateur de champ
    quote='"',  # caractère pour encadrer les champs contenant des séparateurs
    escape='"',  # caractère d'échappement
    multiLine=True,  # essentiel si texte contient des sauts de ligne
    encoding="UTF-8"
)



# Filtrage




## normalisation

df_posts = df_posts.withColumnRenamed("created_utc", "date") \
    .withColumnRenamed("Subreddit", "SubReddit/Community") \




pdf_posts= df_posts.toPandas()

csv_posts = pdf_posts.to_csv(index=False).encode('utf-8')


client = Minio(
    "minio:9000",
    access_key="sxhFJWY6czxnrhr6sUW4",  # cle d'acces
    secret_key="WqXxlKyNEs4cL9NQSyJAfZw1Bjcw3CTWs5XUWAky",  # cle secrete
    secure=False  
)

try:
    
    client.put_object(
        bucket_name="cleaneddata",
        object_name=f"{themedesti}/posts.csv",
        data=BytesIO(csv_posts),
        length=len(csv_posts),
        content_type="text/csv"
    )
    print("Fichier uploade avec succes !")
except S3Error as e:
    print(f"Erreur lors de l'upload dans MinIO: {e}")





