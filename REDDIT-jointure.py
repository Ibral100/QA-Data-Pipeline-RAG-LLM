from minio.error import S3Error
from io import BytesIO
from config_Api_Reddit import *
from config_miniO import *
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,row_number,collect_list
from pyspark.sql import Window
from config_source_dest import *



theme=theme_dest

postsReddit="posts"
postsStack="questions"

commsReddit="comms"
commsStack="answers"

fichierDestReddit="infosReddit"
fichierDestStack="infosStack"

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


df_posts = spark.read.csv(
    f"s3a://cleaneddata/{theme}/{postsReddit}.csv",
    header=True,
    inferSchema=True, 
    sep=",",  
    quote='"',  
    escape='"',  
    multiLine=True,  
    encoding="UTF-8"
)






df_comms = spark.read.csv(
    f"s3a://cleaneddata/{theme}/{commsReddit}.csv",
    header=True,
    inferSchema=True,
    sep=",",  
    quote='"',  
    escape='"',  
    multiLine=True,  
    encoding="UTF-8"
)


## jointure relationelle en ajoutant les ids des commentaires au dataframe des posts


df_comms_grouped = df_comms.groupBy("parent_post_id") \
    .agg(collect_list("id_comment").alias("comment_ids"))


df_posts_enrichi = df_posts.join(
    df_comms_grouped,
    df_posts["id_post"] == df_comms_grouped["parent_post_id"],
    how="left"
).drop("parent_post_id")




pdf_final= df_posts_enrichi.toPandas()

csv_merged = pdf_final.to_csv(index=False).encode('utf-8')

client = Minio(
    "minio:9000",
    access_key="sxhFJWY6czxnrhr6sUW4",  # cle d'acces
    secret_key="WqXxlKyNEs4cL9NQSyJAfZw1Bjcw3CTWs5XUWAky",  # cle secrete
    secure=False  
)

try:
    
    client.put_object(
        bucket_name="cleaneddata",
        object_name=f"{theme}/{fichierDestReddit}.csv",
        data=BytesIO(csv_merged),
        length=len(csv_merged),
        content_type="text/csv"
    )
    print("Fichier uploade avec succes !")
except S3Error as e:
    print(f"Erreur lors de l'upload dans MinIO: {e}")