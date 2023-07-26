import json
import time

import boto3
import jwt
import requests as requests

from src.s3.client_s3 import S3Client
from src.config.enviroments import ENVS


class WebhookService:
    def __init__(self):
        pass

    def envio_webhook(self, jsons):

        relation_webhook = S3Client.read_json(jsons)

        for email, url in relation_webhook.items():
            token = self.generate_authorization_token(email)
            data_form = {
                'email': email,
                'url': url
            }
            headers = {
                'Authorization': token,
            }
            self.invoke_another_lambda(data_form, headers)


    def invoke_another_lambda(self, data, headers):
        try:
            lambda_client = boto3.client('lambda')
            target_lambda_name = ENVS.LAMBDA_WEBHOOK

            response = requests.post(ENVS.LAMBDA_WEBHOOK, headers=headers, files=data)

            if response.status_code != 200:
                while response.status_code == 401 or response.status_code == 504:
                    response = requests.post(ENVS.LAMBDA_WEBHOOK, headers=headers, files=data)

            response_data = response.json()
        except Exception as e:
            print(e)


    def generate_authorization_token(self, email):
        secret = "H3r3-45impl3-T3$t"
        issuer = "reporty_plus"
        expiration_time = int(time.time()) + 120
        issued_at = int(time.time())

        payload = {
            "iss": issuer,
            "sub": email,
            "exp": expiration_time,
            "iat": issued_at
        }

        token = jwt.encode(payload, secret, algorithm='HS256')
        return f"Bearer {token}"
