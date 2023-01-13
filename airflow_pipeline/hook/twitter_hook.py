from airflow.providers.http.hooks.http import HttpHook
import requests
from datetime import datetime, timedelta
import json

class TwitterHook(HttpHook):

    def __init__(self, end_time, start_time, query, conn_id=None):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.conn_id = conn_id or "twitter_default"
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):

        TIMESTAMP_FOMART = "%Y-%m-%dT%H:%M:%S.00Z" # ISO 8601/RFC 3339

        end_time = self.end_time
        start_time = self.start_time
        query = self.query

        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

        url_raw = f"{self.base_url}/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

        return url_raw

    def connect_to_endpoint(self, url, session):
        request = requests.Request("GET", url)
        prep = session.prepare_request(request)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {})

    def paginate(self, url_raw, session):

        lista_json_response = []

        # Imprimir json na tela
        response = self.connect_to_endpoint(url_raw, session)
        json_response = response.json()
        lista_json_response.append(json_response)
        contador = 1

        # Paginate
        while "next_token" in json_response.get("meta",{}) and contador<100:
            next_token = json_response['meta']['next_token']
            url = f"{url_raw}&next_token={next_token}"
            response = self.connect_to_endpoint(url, session) 
            json_response = response.json()
            lista_json_response.append(json_response)
            contador += 1

        return lista_json_response

    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()

        return self.paginate(url_raw, session)

if __name__ == "__main__":

    #Montando url
    TIMESTAMP_FOMART = "%Y-%m-%dT%H:%M:%S.00Z" # ISO 8601/RFC 3339

    end_time = datetime.now().strftime(TIMESTAMP_FOMART)
    start_time = (datetime.now() + timedelta(days=-1)).date().strftime(TIMESTAMP_FOMART)
    query = "datascience"

    for pg in TwitterHook(end_time, start_time, query).run():
        print(json.dumps(pg, indent=4, sort_keys=True))