from sentence_transformers import SentenceTransformer,util
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance
from minio.error import S3Error
from config_miniO import *
import pandas as pd
import io
import sys
from ollama import Client as OllamaClient
import ast
from config_source_dest import *




## modifier encodage pour affichage correct 
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


def minioCsv_to_df(bucket,object):

    reponse = client.get_object(bucket_name=bucket,
                    object_name=object
    )

    csv_file = io.BytesIO(reponse.read())

    df = pd.read_csv(csv_file)

    return df

def get_similar_posts(phrase,col_name,threshold = 0.5):


    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    embedding_phrase = model.encode(phrase,
                                batch_size=32,
                                )
    vec = [float(x) for x in embedding_phrase]

    try:
        results = clientq.search(
            collection_name=col_name,
            query_vector=vec,
            limit=1,
            with_payload=True,
            score_threshold=threshold
        )
    except Exception as e:
        print(f"Erreur lors de la recuperation des donnees : {e}")
        raise SystemExit(1)
    
    

    results = [result.payload["id_post"] for result in results]

    return results[0]

def get_comments(post_similaire,df):
    comments_list=[]
    similar_posts_list=[]

    try:

        ligne = df.loc[df["id_post"] == post_similaire]

        comment_ids_str = ligne["comment_ids"].values[0]
        comments_list = ast.literal_eval(comment_ids_str)

        posts_similaires_str = ligne["posts_similaires"].values[0]
        similar_posts_list = ast.literal_eval(posts_similaires_str)

    except Exception as e:
        print("erreur exctraction commentaires : ",e )
    
    
    return comments_list,similar_posts_list

def get_other_comments(post_similaire,df):
    comments_list=[]

    try :
        ligne = df.loc[df["id_post"] == post_similaire]
       
        comment_ids_str = ligne["comment_ids"].values[0]
        comments_list = ast.literal_eval(comment_ids_str)
    
    except Exception as e :
        print("erreur extraction autres commentaires",e )
    
    return comments_list




def gather_comments(posts,df):
    comments_list=[]

    for post in posts:
        try:
            comments_list.append(get_other_comments(post,df))

            if not comments_list:
                continue
        except Exception as e:
            print(e)

    return comments_list





clientq = QdrantClient(
    url="http://localhost:6333",
    prefer_grpc=False
)   

phrase = "in which year the state of israel was founded"


theme =theme_dest


## trouver les vecteurs similaires et extraire les posts_id

df = minioCsv_to_df("cleaneddata",f"{theme}/infos_clusterises.csv")

post_similaire = get_similar_posts(phrase,col_name=theme)

print(post_similaire)



##extraire les commentaires des posts similaires recuperees 


retour=get_comments(post_similaire,df)

comments=retour[0]
posts_sim=retour[1]

print(comments)
all_comments = gather_comments(posts_sim,df)
all_comments = [item for sublist in all_comments for item in sublist]

print(all_comments)





## recuperer fichier des commentaires depuis minio 


df_comments_Reddit = minioCsv_to_df("cleaneddata",f"{theme}/comms.csv")
df_comments_Stack = minioCsv_to_df("cleaneddata",f"{theme}/answers.csv")


# extraire le body des commentaires correspondant aux posts similaires

filtr = df_comments_Reddit[df_comments_Reddit["id_comment"].isin(all_comments)]
filtr2 = df_comments_Stack[df_comments_Stack["id_comment"].isin(all_comments)]

filtr_combined = pd.concat([filtr, filtr2])

bodies = filtr_combined['body'].tolist()

print(bodies)

print(len(bodies))



## se connecter a ollama et  envoyer un prompt au LLM 






def generate_response(comments, question):

    client = OllamaClient(host='http://localhost:11434')
    try:
        
        prompt = (
            "Voici des commentaires pertinents:\n"
            f"{comments}\n\n"
            f"En te basant sur ces commentaires, réponds à cette question : {question}\n"
            "Ta réponse doit se basee uniquement sur les commentaires"
        )
        
        # Envoyer la requête au modèle
        response = client.generate(
            model="llama3.2:latest",
            prompt=prompt,
            options={
                'temperature': 0.3,  # creativite
                'num_ctx': 4096,  # taille du contexte
                'num_predict': 1000

            }
        )
        
        return response['response'] 
    
    except Exception as e:
        print(f"Erreur lors de la génération: {str(e)}")
        return None


llm_response = generate_response(bodies, phrase)
print(llm_response)











 



