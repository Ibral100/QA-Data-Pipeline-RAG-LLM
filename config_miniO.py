from minio import Minio
from minio.error import S3Error

client = Minio(
    "localhost:9000",
    access_key="sxhFJWY6czxnrhr6sUW4",  # cle d'acces
    secret_key="WqXxlKyNEs4cL9NQSyJAfZw1Bjcw3CTWs5XUWAky",  # cle secrete
    secure=False  
)


bucket_name = "donneesbrutes"