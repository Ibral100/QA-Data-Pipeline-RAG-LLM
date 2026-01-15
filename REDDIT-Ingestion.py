import praw
import pandas as pd
import time
from minio.error import S3Error
from io import BytesIO
from config_Api_Reddit import *
from config_miniO import *

####################################################################

# CONFIG REDDIT ET SUBMISSIONS


reddit = praw.Reddit(
    client_id='ZXvWZW3WJkeMPF7KWSiKVQ',
    client_secret='T3_FQlH1EbKtJOAQTWqZBW_XxM-FUw',
    username='Ok_Nectarine8371',
    password='Balzac01',
    user_agent='MyRedditApp/0.1 (by /u/Ok_Nectarine8371)'
)

config = {
    'subreddit_name': "askhistorians",
    'directory_name':'history2',
    'sorting_methods': ['top', 'new', 'hot'],
    'limit': 10000,
    'time_filter': 'all',
    'min_post_comments': 2,
    'max_post_comments': 100,
    'request_delay': 2,  
    'max_attempts': 3,  
    'interval': 500
}





#################################################

# FUNCTIONS 

def safe_fetch_submissions(subreddit, method, config):
    batch_size = 50
    count = 0
    
    if method == 'top':
        gen = subreddit.top(limit=config['limit'],time_filter=config['time_filter'])
    elif method == 'hot':
        gen = subreddit.hot(limit=config['limit'])
    elif method == 'new':
        gen = subreddit.new(limit=config['limit'])
    
    for submission in gen:
        yield submission
        count += 1
        if count % batch_size == 0:
            print(f"Pause apres {count} submissions...")
            time.sleep(config['request_delay'])


def process_comments(submission, config):
    
    comments = []
    try:
        submission.comments.replace_more(limit=0) 
        for i, comment in enumerate(submission.comments.list()):
            if i >= config["max_post_comments"]:
                break
            if isinstance(comment, praw.models.Comment) and comment.body not in ["[deleted]", "[removed]"]:
                comments.append({
                    "id_comment": comment.id,
                    "body": comment.body,
                    "score": comment.score,
                    "created_utc": comment.created_utc,
                    "parent_post_id": submission.id.replace("t3_", "") if submission.id.startswith("t3_") else submission.id,
                })
    except Exception as e:
        print(f"Erreur de traitement des commentaires pour le post {submission.id}: {e}")
    return comments


def is_moderator_post(submission):
    
    # Vérifier le flair de l'auteur
    if hasattr(submission, 'author_flair_text') and submission.author_flair_text:
        mod_indicators = ['mod', 'moderator', 'modérateur', 'admin']
        return any(indicator in submission.author_flair_text.lower() for indicator in mod_indicators)
    return False


def has_mod_flair(submission):
    
    if submission.link_flair_text:
        mod_flairs = ['announcement', 'mod post', 'sticky', 'pinned', 'meta']
        return any(flair in submission.link_flair_text.lower() for flair in mod_flairs)
    return False



def ingestion(config):
    posts = {}
    comments = []
    processed_count = 0
    
    for method in config['sorting_methods']:
        if len(posts) >= config['limit']:
            break
            
        print(f"Recuperation des posts via: {method}")
        subreddit = reddit.subreddit(config["subreddit_name"])
        submissions = safe_fetch_submissions(subreddit, method, config)
        
        for submission in submissions:
                
            if (submission.id in posts or not submission.title or 
                submission.title in ["[deleted]", "[removed]"] or
                submission.num_comments < config["min_post_comments"] or
                is_moderator_post(submission) or 
                has_mod_flair(submission)):
                continue

            # Ajout du post
            posts[submission.id] = {
                "platform": "Reddit",
                "Subreddit": submission.subreddit.display_name,
                "id_post": submission.id.replace("t3_", "") if submission.id.startswith("t3_") else submission.id,
                "title": submission.title,
                "body": submission.selftext,
                "score": submission.score,
                "created_utc": submission.created_utc,
                "link": submission.url,
            }

            # Traitement des commentaires
            comments.extend(process_comments(submission, config))
            

            processed_count += 1
            if processed_count % config['interval'] == 0:
                print(f"Progression: {len(posts)}/{config['limit']} posts traites")
                
    return posts, comments




######################################################

# MAIN



if __name__ == "__main__":
    try:
        print("Debut de l'extraction...")
        posts, comments = ingestion(config)
        
        
        df_posts = pd.DataFrame(list(posts.values()))
        df_comments = pd.DataFrame(comments)
        
        # save posts et comms sur MINIO
        try:
            client.put_object(
                bucket_name=bucket_name,
                object_name=f"reddit/{config['directory_name']}/posts.csv",
                data=BytesIO(df_posts.to_csv(index=False).encode('utf-8')),
                length=len(df_posts.to_csv(index=False).encode('utf-8')),
                content_type="text/csv"
            )
            print("Posts sauvegardes avec succes!")
        except S3Error as e:
            print(f"Erreur MinIO (posts): {e}")
        
        try:
            client.put_object(
                bucket_name=bucket_name,
                object_name=f"reddit/{config['directory_name']}/comments.csv",
                data=BytesIO(df_comments.to_csv(index=False).encode('utf-8')),
                length=len(df_comments.to_csv(index=False).encode('utf-8')),
                content_type="text/csv"
            )
            print("Commentaires sauvegardes avec succes!")
        except S3Error as e:
            print(f"Erreur MinIO (comments): {e}")
            
        print(f"Extraction terminee. {len(posts)} posts et {len(comments)} commentaires recuperes.")
        
    except Exception as e:
        print(f"Erreur majeure: {e}")