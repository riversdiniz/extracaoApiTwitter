from datetime import datetime, timedelta
import requests
import json
import os

#Montando url
TIMESTAMP_FOMART = "%Y-%m-%dT%H:%M:%S.00Z" # ISO 8601/RFC 3339

# Extraido Dados de 7 dias Atras
# end_time = (datetime.now() + timedelta(hours=4, seconds=-30)).strftime(TIMESTAMP_FOMART)
# start_time = (datetime.now() + timedelta(days=-7, hours=4)).strftime(TIMESTAMP_FOMART)
# query = "datascience"

#Extraido Dados do dia Atual
end_time = datetime.now().strftime(TIMESTAMP_FOMART)
start_time = (datetime.now() + timedelta(days=-1)).date().strftime(TIMESTAMP_FOMART)
query = "datascience"

tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

url_raw = f"https://api.twitter.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

# Montando headers
bearer_token = os.environ.get("BEARER_TOKEN")
headers = {"Authorization": "Bearer {}".format(bearer_token)}
response = requests.request("GET", url_raw, headers=headers)

# Imprimir json na tela
json_response = response.json()

print(json.dumps(json_response, indent=4, sort_keys=True))


# Paginate

while "next_token" in json_response.get("meta",{}):
    next_token = json_response['meta']['next_token']
    url = f"{url_raw}&next_token={next_token}"
    response = requests.request("GET", url, headers=headers)
    json_response = response.json()
    print(json.dumps(json_response, indent=4, sort_keys=True))
