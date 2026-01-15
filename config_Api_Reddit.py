import requests


##identifiants
CLIENT_ID = ""
CLIENT_SECRET = ""
USERNAME = ""
PASSWORD = ""

auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)

# Paramètres
data = {
    'grant_type': 'password',
    'username': USERNAME,
    'password': PASSWORD
}

# Headers
headers = {'User-Agent': 'MyRedditApp/0.1'}

# Requête pour obtenir un token
res = requests.post('https://www.reddit.com/api/v1/access_token',
                    auth=auth, data=data, headers=headers)

# Récupération du token
TOKEN = res.json().get('access_token')


headers['Authorization'] = f'bearer {TOKEN}'