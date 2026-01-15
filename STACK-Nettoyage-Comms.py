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

spark = SparkSession.builder \
    .appName("Nettoyage des commentaires") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()


## nettoyage des commentaires 





themesource=theme_source_Stack
themedestination=theme_dest

df_comms = spark.read.csv(
    f"s3a://donneesbrutes/stackexchange/{themesource}/answers.csv",
    header=True,
    inferSchema=True,
    sep=",",  
    quote='"',  
    escape='"',  
    multiLine=True, 
    encoding="UTF-8"
)






## normalisation


df_comms = df_comms.withColumnRenamed("answer_id", "id_comment") \
    .withColumnRenamed("created_date", "created_utc") \
    .withColumnRenamed("parent_question_id", "parent_post_id")

## supprimer les colonnes non utiles

df_comms = df_comms.drop("is_accepted","owner_reputation","owner_display_name")






## supprimer les commmentaires deleted et removed 

df_comms = df_comms.filter(~col("body").isin(["[deleted]", "[removed]"]))



## supprimer balises html 

def clean_html(raw_html):
    return BeautifulSoup(raw_html, "html.parser").get_text()


clean_html_udf = udf(clean_html, StringType())

df_comms = df_comms.withColumn("body", clean_html_udf("body"))



## selection des n meilleurs commentaires




## garder les n meilleurs commentaires par post

nombres_de_comms_a_garder = 20


windowSpec = Window.partitionBy("parent_post_id").orderBy(col("score").desc())

df_comms_classe = df_comms.withColumn("rank", row_number().over(windowSpec))

df_comms_cleaned = df_comms_classe.filter(col("rank") <= nombres_de_comms_a_garder).drop("rank")





## supprimer les commentaires des bot/moderateurs 


auto_messages = [
    r"^Welcome to /r/.*",  
    r"^Thank you for your submission to .*",  
    r"^Your post was removed from. *",  
    r"^I'm a bot,.*",  
    r"^This comment was automatically.*",  
    r"^Reminder: This is a.*",  
    r"^Please read the rules before posting.*",  
]

combined_regex = "|".join(auto_messages)

df_comms_cleaned = df_comms_cleaned.filter(~col("body").rlike(combined_regex))




pdf_comms= df_comms_cleaned.toPandas()


csv_comms = pdf_comms.to_csv(index=False).encode('utf-8')



client = Minio(
    "minio:9000",
    access_key="sxhFJWY6czxnrhr6sUW4",  # cle d'acces
    secret_key="WqXxlKyNEs4cL9NQSyJAfZw1Bjcw3CTWs5XUWAky",  # cle secrete
    secure=False  
)


try:
    
    client.put_object(
        bucket_name="cleaneddata",
        object_name=f"{themedestination}/answers.csv",
        data=BytesIO(csv_comms),
        length=len(csv_comms),
        content_type="text/csv"
    )
    print("Fichier uploade avec succes !")
except S3Error as e:
    print(f"Erreur lors de l'upload dans MinIO: {e}")