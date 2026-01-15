import pandas as pd
import time
from minio.error import S3Error
from io import BytesIO
from stackapi import StackAPI
from config_miniO import *

####################################################################




config = {
    'site_name': "economics",
    'directory_name': 'economics',
    'sorting_methods': [
        'activity',      
        'creation',      
        'votes',         
        'hot',           
        'week',          
        'month',
        ], 
    'limit': 10000,
    'min_question_answers': 1,
    'max_question_answers': 100,
    'request_delay': 2,  
    'max_attempts': 3,  
    'interval': 500,
    'min_score': 0,  # Score minimum pour les questions
    'backoff_multiplier': 2  # Multiplicateur pour les delais en cas d'erreur
}



#################################################

# FUNCTIONS 

def safe_fetch_questions(site_api, method, config):
    
    attempts = 0
    while attempts < config['max_attempts']:
        try:
            params = {
                'sort': method,
                'order': 'desc',
                'pagesize': 100,  
                'filter': 'withbody',  # Inclut le corps des questions
                'max_pages': 500
            }
            
            if config.get('min_score'):
                params['min'] = config['min_score']
            
            questions = site_api.fetch('questions', **params)
            return questions['items'] if 'items' in questions else []
            
        except Exception as e:
            print(f"Erreur avec {method} (tentative {attempts + 1}): {e}")
            attempts += 1
            sleep_time = config['request_delay'] * (config['backoff_multiplier'] ** attempts)
            time.sleep(sleep_time)
    return []

def safe_fetch_answers(site_api, question_id, config):
    """Récupère les réponses d'une question"""
    try:
        answers = site_api.fetch(f'questions/{question_id}/answers', 
                                filter='withbody', 
                                sort='votes', 
                                order='desc')
        return answers['items'] if 'items' in answers else []
    except Exception as e:
        print(f"Erreur recuperation reponses pour question {question_id}: {e}")
        return []

def process_answers(question_id, site_api, config):
    """Traite les réponses d'une question"""
    answers = []
    try:
        raw_answers = safe_fetch_answers(site_api, question_id, config)
        
        for i, answer in enumerate(raw_answers):
            if i >= config["max_question_answers"]:
                break
                
            # Vérifier que la réponse n'est pas supprimée
            if answer.get('body') and not answer.get('is_deleted', False):
                answers.append({
                    "answer_id": answer.get('answer_id'),
                    "body": answer.get('body', ''),
                    "score": answer.get('score', 0),
                    "creation_date": answer.get('creation_date'),
                    "is_accepted": answer.get('is_accepted', False),
                    "parent_question_id": question_id,
                    "owner_reputation": answer.get('owner', {}).get('reputation', 0),
                    "owner_display_name": answer.get('owner', {}).get('display_name', 'Anonymous')
                })
                
    except Exception as e:
        print(f"Erreur de traitement des reponses pour la question {question_id}: {e}")
    
    return answers




def ingestion(config):
    questions = {}
    answers = []
    processed_count = 0
    
    
    site_api = StackAPI(config['site_name'],key='rl_RL5mbzYZ79gWcR6Y8jQXnxSP4')
    
    for method in config['sorting_methods']:
        if len(questions) >= config['limit']:
            break
            
        print(f"Recuperation des questions via: {method}")
        raw_questions = safe_fetch_questions(site_api, method, config)
        
        for question in raw_questions:
            question_id = question.get('question_id')
            
            # Filtres d'exclusion
            if (question_id in questions or 
                not question.get('title') or
                question.get('answer_count', 0) < config["min_question_answers"] or 
                question.get('closed_date')):
                continue

            # Ajout de la question
            questions[question_id] = {
                "platform": "StackExchange",
                "site": config['site_name'],
                "question_id": question_id,
                "title": question.get('title', ''),
                "body": question.get('body', ''),
                "score": question.get('score', 0),
                "answer_count": question.get('answer_count', 0),
                "creation_date": question.get('creation_date'),
                "last_activity_date": question.get('last_activity_date'),
                "link": question.get('link', '')
            }

            # Traitement des réponses
            answers.extend(process_answers(question_id, site_api, config))
            
            processed_count += 1
            if processed_count % config['interval'] == 0:
                print(f"Progression: {len(questions)}/{config['limit']} questions traitees")
            
    
    return questions, answers

######################################################

# MAIN

if __name__ == "__main__":
    try:
        print("Début de l'extraction Stack Exchange...")
        questions, answers = ingestion(config)
        
        # Conversion en DataFrames
        df_questions = pd.DataFrame(list(questions.values()))
        df_answers = pd.DataFrame(answers)
        
        # Sauvegarde des questions sur MinIO
        try:
            client.put_object(
                bucket_name=bucket_name,
                object_name=f"stackexchange/{config['directory_name']}/questions.csv",
                data=BytesIO(df_questions.to_csv(index=False).encode('utf-8')),
                length=len(df_questions.to_csv(index=False).encode('utf-8')),
                content_type="text/csv"
            )
            print("Questions sauvegardees avec succes!")
        except S3Error as e:
            print(f"Erreur MinIO (questions): {e}")
        
        # Sauvegarde des réponses sur MinIO
        try:
            client.put_object(
                bucket_name=bucket_name,
                object_name=f"stackexchange/{config['directory_name']}/answers.csv",
                data=BytesIO(df_answers.to_csv(index=False).encode('utf-8')),
                length=len(df_answers.to_csv(index=False).encode('utf-8')),
                content_type="text/csv"
            )
            print("Reponses sauvegardees avec succes!")
        except S3Error as e:
            print(f"Erreur MinIO (answers): {e}")
            
        print(f"Extraction terminee. {len(questions)} questions et {len(answers)} reponses recuperees.")
        
    except Exception as e:
        print(f"Erreur majeure: {e}")