from minio.error import S3Error
from io import BytesIO
from config_Api_Reddit import *
from config_miniO import *
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,row_number
from pyspark.sql import Window
from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import io
from config_source_dest import *

spark = SparkSession.builder \
    .appName("Merge") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()



theme=theme_dest
fichierReddit="infosReddit"
fichierSpark="infosStack"

df_posts = spark.read.csv(
    f"s3a://cleaneddata/{theme}/{fichierReddit}.csv",
    header=True,
    inferSchema=True,
    sep=",",  
    quote='"',  
    escape='"',  
    multiLine=True,  
    encoding="UTF-8"
)

df_questions = spark.read.csv(
    f"s3a://cleaneddata/{theme}/{fichierSpark}.csv",
    header=True,
    inferSchema=True,
    sep=",",  
    quote='"',  
    escape='"',  
    multiLine=True,  
    encoding="UTF-8"
)



df_merged= df_posts.union(df_questions)


pdf_merged= df_merged.toPandas()

csv_posts = pdf_merged.to_csv(index=False).encode('utf-8')


client = Minio(
    "minio:9000",
    access_key="sxhFJWY6czxnrhr6sUW4",  
    secret_key="WqXxlKyNEs4cL9NQSyJAfZw1Bjcw3CTWs5XUWAky",  
    secure=False  
)

try:
    
    client.put_object(
        bucket_name="cleaneddata",
        object_name=f"{theme}/infos.csv",
        data=BytesIO(csv_posts),
        length=len(csv_posts),
        content_type="text/csv"
    )
    print("Fichier uploade avec succes !")
except S3Error as e:
    print(f"Erreur lors de l'upload dans MinIO: {e}")

