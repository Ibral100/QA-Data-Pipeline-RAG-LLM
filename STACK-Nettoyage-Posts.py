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

themesource=theme_source_Stack
themedestination=theme_dest

df_posts = spark.read.csv(
    f"s3a://donneesbrutes/stackexchange/{themesource}/questions.csv",
    header=True,
    inferSchema=True,
    sep=",",  
    quote='"',  
    escape='"',  
    multiLine=True,  
    encoding="UTF-8"
)



# Filtrage




## normalisation

df_posts = df_posts.withColumnRenamed("site", "SubReddit/Community") \
    .withColumnRenamed("question_id", "id_post") \
    .withColumnRenamed("creation_date", "date")


## drop les colonnees answer_count et creation_date


df_posts = df_posts.drop("answer_count","last_activity_date")
    



## nettoyage du body en enlevant les balises html avec beautifulsoup



def clean_html(raw_html):
    if raw_html is None:
            return None
    return BeautifulSoup(raw_html, "html.parser").get_text()


clean_html_udf = udf(clean_html, StringType())


df_posts = df_posts.withColumn("body", clean_html_udf("body"))
df_posts = df_posts.withColumn("title", clean_html_udf("title"))


 


pdf_posts= df_posts.toPandas()

csv_posts = pdf_posts.to_csv(index=False).encode('utf-8')



client = Minio(
    "minio:9000",
    access_key="sxhFJWY6czxnrhr6sUW4",  
    secret_key="WqXxlKyNEs4cL9NQSyJAfZw1Bjcw3CTWs5XUWAky",  
    secure=False  
)

try:
    
    client.put_object(
        bucket_name="cleaneddata",
        object_name=f"{themedestination}/questions.csv",
        data=BytesIO(csv_posts),
        length=len(csv_posts),
        content_type="text/csv"
    )
    print("Fichier uploade avec succes !")
except S3Error as e:
    print(f"Erreur lors de l'upload dans MinIO: {e}")





