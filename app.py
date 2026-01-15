import streamlit as st
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from config_miniO import client  
from qdrant_client.models import VectorParams, Distance
from minio.error import S3Error
import pandas as pd
import io
import sys
from ollama import Client as OllamaClient
import ast
import requests


# Init Qdrant client
clientq = QdrantClient(
    url="http://localhost:6333",
    prefer_grpc=False
)

# Chargement CSV depuis MinIO
def minioCsv_to_df(bucket, object):
    response = client.get_object(bucket_name=bucket, object_name=object)
    csv_file = io.BytesIO(response.read())
    df = pd.read_csv(csv_file)
    return df

# Recherche du post le plus similaire
def get_similar_posts(phrase, col_name, threshold=0.4):
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    vec = model.encode(phrase)
    try:
        results = clientq.search(
            collection_name=col_name,
            query_vector=vec.tolist(),
            limit=1,
            with_payload=True,
            score_threshold=threshold
        )
        return results[0].payload["id_post"]
    
    except Exception as e:
        '''
        st.error(f"Erreur Qdrant : {e}")
        '''
        return None


# Extraction des commentaires liés à un post
def get_comments(post_id, df):
    try:
        row = df[df["id_post"] == post_id]
        comment_ids = ast.literal_eval(row["comment_ids"].values[0])
        similar_posts = ast.literal_eval(row["posts_similaires"].values[0])
        return comment_ids, similar_posts
    except Exception as e:
        st.warning(f"Erreur extraction commentaires : {e}")
        return [], []

def get_other_comments(post_id, df):
    try:
        row = df[df["id_post"] == post_id]
        return ast.literal_eval(row["comment_ids"].values[0])
    except:
        return []

def gather_comments(posts, df):
    all_comments = []
    for post in posts:
        all_comments.extend(get_other_comments(post, df))
    return all_comments

# Génération via LLM (Ollama)
def generate_response(comments, question, model_name,history=None):
    client = OllamaClient(host='http://localhost:11434')

    # Créer l'historique au format texte
    dialogue = ""
    if history:
        for turn in history:
            dialogue += f"\nUser: {turn['question']}\nAssistant: {turn['response']}"

    # Prompt final
    prompt = (
        "You are a helpful assistant. You must base your answers **only** on the comments below.\n"
        "Here are some relevant comments:\n"
        f"{comments}\n\n"
        f"{dialogue}\n"
        f"User: {question}\n"
        "Assistant:"
    )

    try:
        response = client.generate(
            model=model_name,
            prompt=prompt,
            options={
                'temperature': 0.3,
                # tu peux aussi ajuster num_ctx/num_predict ici
            }
        )
        return response['response']
    except Exception as e:
        return f"Erreur génération avec {model_name} : {e}"

def get_installed_ollama_models():
    try:
        response = requests.get("http://localhost:11434/api/tags")
        response.raise_for_status()
        data = response.json()
        models = [model["name"] for model in data.get("models", [])]
        return models
    except Exception as e:
        st.warning(f"Erreur récupération modèles Ollama : {e}")
        return []


def list_folders(bucket_name):
    try:
        
        objects = client.list_objects(bucket_name, recursive=False)
        folders = set()

        for obj in objects:
            if "/" in obj.object_name:
                prefix = obj.object_name.split("/")[0]
                folders.add(prefix)

        return list(folders)

    except Exception as e:
        print(f"Erreur recuperation themes minio : {e}")
        return []
















if "conversations" not in st.session_state:
    st.session_state.conversations = []
if "current_question" not in st.session_state:
    st.session_state.current_question = ""
if "current_response" not in st.session_state:
    st.session_state.current_response = ""
if "selected_model" not in st.session_state:
    st.session_state.selected_model = ""
if "dialogue_history" not in st.session_state:
    st.session_state.dialogue_history = []











# Streamlit UI

st.set_page_config(page_title="QA Platform", layout="wide")
st.title("Plateforme de QA base sur les LLM")

with st.sidebar:
    st.header("Parametres")

    st.subheader("Choix du theme")

    themes_available = list_folders("cleaneddata")
    if themes_available:
        theme = st.selectbox("Thème disponibles (depuis MinIO)", themes_available)
    else:
        theme = st.text_input("Aucun thème détecté")
    
    #  liste des modeles disponibles 
    installed_models = get_installed_ollama_models()
    selected_model = st.selectbox("Modele Ollama a utiliser", installed_models)
    st.session_state.selected_model = selected_model

    if st.button("➕ Nouvelle conversation"):
        # Sauvegarder l'ancienne si elle existe
        if st.session_state.current_question and st.session_state.current_response:
            st.session_state.conversations.append({
                "title": f"Conversation {len(st.session_state.conversations)+1}",
                "question": st.session_state.current_question,
                "response": st.session_state.current_response,
                "model": st.session_state.selected_model
            })
        # Réinitialiser le nouveau chat
        st.session_state.current_question = ""
        st.session_state.current_response = ""
        st.session_state.dialogue_history = []

    st.markdown("---")
    st.subheader("Conversations précédentes")
    for i, conv in enumerate(st.session_state.conversations):
        with st.expander(conv["title"]):
            st.markdown(f"**Modèle** : {conv['model']}")
            st.markdown(f"**Question :** {conv['question']}")
            st.markdown(f"**Réponse :** {conv['response']}")




    


st.session_state.current_question = st.text_area("Pose ta question :", value=st.session_state.current_question, height=150)

if st.button("Envoyer") :
    with st.spinner(f"Génération avec le modèle {selected_model}..."):

        try:
            df_meta = minioCsv_to_df("cleaneddata", f"{theme}/infos_clusterises.csv")
            df_reddit = minioCsv_to_df("cleaneddata", f"{theme}/comms.csv")
            df_stack = minioCsv_to_df("cleaneddata", f"{theme}/answers.csv")
        except Exception as e:
            st.error(f"Erreur chargement des données : {e}")
            st.stop()

        post_sim = get_similar_posts(st.session_state.current_question, col_name=theme)

        if post_sim:
            comment_ids, similar_posts = get_comments(post_sim, df_meta)
            all_comment_ids = gather_comments(similar_posts, df_meta)
            all_comment_ids += comment_ids

            df_comments = pd.concat([
                df_reddit[df_reddit["id_comment"].isin(all_comment_ids)],
                df_stack[df_stack["id_comment"].isin(all_comment_ids)]
            ])

            bodies = df_comments["body"].tolist()

            if not st.session_state.selected_model:
                st.error("Aucun modèle Ollama sélectionné.")
                st.stop()

            response = generate_response(
                bodies,
                st.session_state.current_question,
                st.session_state.selected_model,
                history=st.session_state.dialogue_history
                )
            st.session_state.current_response = response

            st.session_state.dialogue_history.append({
            "question": st.session_state.current_question,
            "response": st.session_state.current_response
            })

            
            with st.expander("Commentaires utilisés"):
                for i, body in enumerate(bodies):
                    st.markdown(f"**Commentaire {i+1}**: {body}")
        else:
            st.warning("Aucun post similaire trouvé.")
        
        if st.session_state.current_response:
            st.subheader("Réponse générée")
            st.write(st.session_state.current_response)

