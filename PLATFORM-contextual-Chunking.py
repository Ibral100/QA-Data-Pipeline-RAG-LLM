import io
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer,util
import pandas as pd
from minio import Minio
from minio.error import S3Error
from config_miniO import *
import os
import numpy as np
from sklearn.cluster import DBSCAN
from qdrant_client.models import VectorParams, Distance
import torch
from sklearn.preprocessing import normalize
from io import BytesIO
from config_source_dest import *
import hdbscan

##########################################################################

# FONCTIONS

def minioCsv_to_df(theme):

    reponse = client.get_object(bucket_name="cleaneddata",
                    object_name=f"{theme}/infos.csv"
    )

    csv_file = io.BytesIO(reponse.read())

    df = pd.read_csv(csv_file)

    df['body'] = df['body'].fillna('').astype(str)

    return df





def load_data():
    try:
        df = pd.read_csv("PFE/infos.csv", encoding='utf-8')
        # Nettoyage des données
        df['body'] = df['body'].fillna('').astype(str)
        return df
    except Exception as e:
        print(f"Erreur lecture CSV: {e}")
        raise SystemExit(1)
    


def generate_embeddings(df):
    try:
        model = SentenceTransformer("all-MiniLM-L6-v2",device="cuda")
        
        # preparation des textes

        texts = []
        for title,body in zip(df["title"],df["body"]):
            if not body:
                text = f"Title : {title}"
            else:
                text = f"Title : {title}\nBody : {body.strip()}"
            
            texts.append(text)
        
        
        embeddings = model.encode(texts, 
                               batch_size=32,                   
                               show_progress_bar=True
                               )
        
        df['embedding'] = embeddings.tolist()
        return df
    except Exception as e:
        print(f"Erreur generation embeddings: {e}")  ##
        raise SystemExit(1)
    




def clustering(df: pd.DataFrame):
    
    df["embedding"] = df["embedding"].apply(lambda x: np.array(x).astype(np.float32))
    X = np.stack(df["embedding"].values)

    
    hdb = hdbscan.HDBSCAN(
        min_cluster_size=5,         
        metric="euclidean",
        cluster_selection_method="leaf"  
    )
    df["cluster"] = hdb.fit_predict(X)

    
    cluster_to_post_ids = df.groupby('cluster')['id_post'].apply(list).to_dict()

    # creation des colonnes "posts_similaires"
    def clusteriser_posts(row, max_similaires=20):
        post_id_actuel = row['id_post']
        cluster_actuel = row['cluster']

        if cluster_actuel == -1:
            return []

        all_post_ids = cluster_to_post_ids.get(cluster_actuel, [])
        similaires = [pid for pid in all_post_ids if pid != post_id_actuel]

        # limiter le nombre de posts similaires
        return similaires[:max_similaires]

    df['posts_similaires'] = df.apply(lambda row: clusteriser_posts(row, max_similaires=20), axis=1)
    return df

def save_to_minio():
    client.fput_object(
        bucket_name="cleaneddata",
        object_name="history2/infos_embeddings.csv",
        file_path="PFE/infos_embeddings.csv"
    )


def init_qdrant_client():

    client = QdrantClient(host='localhost', port=6333)

    return client



def create_collection(client,df,col_name):

    if client.collection_exists(collection_name=col_name):
        client.delete_collection(collection_name=col_name)
    try:
        client.create_collection(
            collection_name=col_name,
            vectors_config=VectorParams(

                size=384,
                distance=Distance.COSINE
            )
        )
    except Exception as e :
        print(f"Erreur lors de la creation de la collection : {e}")


    ## conversion des embedding en liste 
    try:
        df['embedding'] = df['embedding'].apply(lambda x: [np.float32(i) for i in x])

    except Exception as e :
        print(f"Erreur lors de la conversion des embedding en liste : {e}")

    print(type(df["embedding"][0]),type(df["embedding"][0][0]))

    ## ajouter les vecteurs dans la collection 
    try :
        client.upsert(
            collection_name=col_name,

            points=[
                {
                    "id":i,
                    "vector":df["embedding"].iloc[i],
                    "payload": {
                        "id_post": df["id_post"][i],
                    }
                
                }
                for i in range(len(df))
            ]
        )
    except:
        print("Erreur lors de l'ajout des vecteurs dans la collection")

    
    
    
   
##################################################################

# MAIN


CSV_PATH = "PFE/infos.csv"

if __name__ == "__main__":

    '''''
    if not os.path.exists(CSV_PATH):
        client.fget_object(
            bucket_name="cleaneddata",
            object_name="history2/infos.csv",
            file_path=CSV_PATH
        )

    '''
    

    #df = load_data() 

    df = minioCsv_to_df(theme=theme_dest)
    
    df = generate_embeddings(df) 
    
    df = clustering(df) 





    '''
    # Sauvegarde des embeddings dans un fichier CSV
    if not os.path.exists("PFE/infos_embeddings3.csv"): 
        try:
            df.to_csv("PFE/infos_embeddings.csv", index=False, encoding='utf-8') 
            print("Fichier CSV avec embeddings sauvegardé avec succès.")
        except Exception as e: 
            print(f"Erreur sauvegarde CSV: {e}")
            raise SystemExit(1)
    '''
    

    ## sauvegarde du fichier infos_embedding csv dans le bucket cleaneddata

    clientq = init_qdrant_client()

    collec_name = theme_dest

    try:
        create_collection(clientq, df, col_name=collec_name)
        print(f"Collection ",collec_name," creee avec succes dans Qdrant.")
    except Exception as e:
        print(f"Erreur lors de l'initialisation de  la collection : {e}")
        raise SystemExit(1)




    ## sauvegarde du dataframe sur minio 


    df.drop(["embedding"], axis=1, inplace=True)

    

    csv_posts = df.to_csv(index=False).encode('utf-8')


    try:
    
        client.put_object(
            bucket_name="cleaneddata",
            object_name=f"{collec_name}/infos_clusterises.csv",
            data=BytesIO(csv_posts),
            length=len(csv_posts),
            content_type="text/csv"
        )
        print("Fichier uploade avec succes !")
    except S3Error as e:
        print(f"Erreur lors de l'upload dans MinIO: {e}")


