import streamlit as st
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from config_miniO import client  
from minio.error import S3Error
import pandas as pd
import io
from ollama import Client as OllamaClient
import ast
import requests

st.set_page_config(page_title="QA Platform", layout="wide")

# Initialisation des clients
@st.cache_resource
def init_qdrant():
    return QdrantClient(url="http://localhost:6333", prefer_grpc=False)

@st.cache_resource
def init_embedding_model():
    return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

clientq = init_qdrant()
embedding_model = init_embedding_model()
ollama_client = OllamaClient(host='http://localhost:11434')

# Fonctions utilitaires
def minioCsv_to_df(bucket, object):
    response = client.get_object(bucket_name=bucket, object_name=object)
    return pd.read_csv(io.BytesIO(response.read()))

def get_similar_posts(phrase, col_name, threshold=0.5):
    vec = embedding_model.encode(phrase)
    try:
        results = clientq.search(
            collection_name=col_name,
            query_vector=vec.tolist(),
            limit=1,
            with_payload=True,
            score_threshold=threshold
        )
        return results[0].payload["id_post"] if results else None
    except Exception as e:
        st.error(f"Erreur Qdrant : {e}")
        return None

def get_comments_data(post_id, df_meta, df_reddit, df_stack):
    try:
        row = df_meta[df_meta["id_post"] == post_id].iloc[0]
        comment_ids = ast.literal_eval(row["comment_ids"])
        similar_posts = ast.literal_eval(row["posts_similaires"])
        
        all_comment_ids = comment_ids.copy()
        for post in similar_posts:
            all_comment_ids.extend(ast.literal_eval(
                df_meta[df_meta["id_post"] == post].iloc[0]["comment_ids"]
            ))
        
        df_comments = pd.concat([
            df_reddit[df_reddit["id_comment"].isin(all_comment_ids)],
            df_stack[df_stack["id_comment"].isin(all_comment_ids)]
        ])
        
        return df_comments["body"].tolist(), df_comments
    except Exception as e:
        st.warning(f"Erreur extraction commentaires : {e}")
        return [], pd.DataFrame()

def get_installed_ollama_models():
    try:
        response = requests.get("http://localhost:11434/api/tags")
        return [model["name"] for model in response.json().get("models", [])]
    except Exception as e:
        st.warning(f"Erreur récupération modèles Ollama : {e}")
        return []

def list_folders(bucket_name):
    try:
        objects = client.list_objects(bucket_name, recursive=False)
        return list({obj.object_name.split("/")[0] for obj in objects if "/" in obj.object_name})
    except Exception as e:
        st.error(f"Erreur récupération thèmes MinIO : {e}")
        return []
    




# Initialisation de l'état de session
if "messages" not in st.session_state:
    st.session_state.messages = []
if "selected_model" not in st.session_state:
    st.session_state.selected_model = ""
if "current_theme" not in st.session_state:
    st.session_state.current_theme = ""

# Interface utilisateur

st.title("Plateforme de QA base sur les LLMS")

with st.sidebar:
    st.header("Configuration")
    
    # Sélection du thème
    themes = list_folders("cleaneddata")
    st.session_state.current_theme = st.selectbox(
        "Sélectionnez un thème",
        themes,
        index=0 if themes else None
    )
    
    # Sélection du modèle
    models = get_installed_ollama_models()
    st.session_state.selected_model = st.selectbox(
        "Modèle Ollama",
        models,
        index=0 if models else None
    )
    
    if st.button("Nouvelle conversation"):
        st.session_state.messages = []
        st.rerun()

# Affichage des messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if message["role"] == "assistant" and "sources" in message:
            with st.expander("📚 Sources utilisées"):
                st.dataframe(message["sources"], hide_index=True)

# Gestion de l'entrée utilisateur
if prompt := st.chat_input("Posez votre question ..."):
    if not st.session_state.current_theme:
        st.warning("Veuillez sélectionner un thème dans la sidebar")
        st.stop()
    
    if not st.session_state.selected_model:
        st.warning("Veuillez sélectionner un modèle Ollama")
        st.stop()
    
    # Ajout du message utilisateur
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Recherche de réponses
    with st.chat_message("assistant"):
        with st.spinner("Recherche d'informations externes..."):
            try:
                # Chargement des données
                df_meta = minioCsv_to_df("cleaneddata", f"{st.session_state.current_theme}/infos_clusterises.csv")
                df_reddit = minioCsv_to_df("cleaneddata", f"{st.session_state.current_theme}/comms.csv")
                df_stack = minioCsv_to_df("cleaneddata", f"{st.session_state.current_theme}/answers.csv")
                
                # Recherche de posts similaires
                post_id = get_similar_posts(prompt, st.session_state.current_theme)
                
                if post_id: 
                    # Récupération des commentaires
                    comments, df_sources = get_comments_data(post_id, df_meta, df_reddit, df_stack)
                    
                    if comments:
                        # Génération de la réponse
                        response = ollama_client.generate(
                            model=st.session_state.selected_model,
                            prompt=f"""
                                    Question: {prompt}

                                    If the external comments below contain useful information, use them to enrich your answer.Otherwise, ignore them.

                                    External Comments:
                                    {comments}
                                    
                                    """,
                            options={'temperature': 0.3}
                        )["response"]

                        
                        # Affichage
                        st.markdown(response)
                        with st.expander("Sources utilisées"):
                            st.dataframe(df_sources[["body"]], hide_index=True)
                        
                        # Sauvegarde du message
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": response,
                            "sources": df_sources[["body"]]
                        })
                    else:
                        response = ollama_client.generate(
                            model=st.session_state.selected_model,
                            prompt=f"""
                            [Warning]  
                            No external information found for this question.  
                            
                            [Instructions]  
                            1. Answer using only your own knowledge  
                            2. Include "[Model knowledge]" in your response  
                            
                            Question: {prompt}
                            """,
                            options={'temperature': 0.3}
                        )["response"]

                        # Affichage
                        st.markdown(response)
                        
                        # Sauvegarde du message
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": response,
                            "sources": df_sources[["body"]]
                        })

                        
                        
                else:
                    response = ollama_client.generate(
                            model=st.session_state.selected_model,
                            prompt=f"""
                            [Warning]  
                            No external information found for this question.  
                            
                            [Instructions]  
                            1. Answer using only your own knowledge  
                            2. Include "[Model knowledge]" in your response  
                            
                            Question: {prompt}
                            """,
                            options={'temperature': 0.3}
                        )["response"]
                    
                    # Affichage
                    st.markdown(response)

                        
                    # Sauvegarde du message
                    st.session_state.messages.append({
                            "role": "assistant",
                            "content": response
                            
                        })
            except Exception as e:
                st.error(f"Une erreur est survenue : {str(e)}")
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"⚠️ Erreur: {str(e)}"
                })