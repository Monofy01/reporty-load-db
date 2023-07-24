import json
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError

from src.config.enviroments import ENVS


class S3Client:
    def __init__(self):
        self.data = ""
    @staticmethod
    def generate_signed_url(filename):  # Set expiration to 3 minutes (180 seconds)
        s3 = boto3.client('s3')
        try:
            expiration_time = datetime.utcnow() + timedelta(seconds=180)
            response = s3.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': ENVS.S3_BUCKET_NAME,
                    'Key': filename
                },
                ExpiresIn=180,
                HttpMethod='GET'
            )
            return response
        except ClientError as e:
            return f"Error al generar la URL firmada: {str(e)}"

    @staticmethod
    def read_json(json_list):
        json_relations = {}
        try:
            for item in json_list:
                s3 = boto3.client('s3')

                response = s3.get_object(Bucket=ENVS.S3_BUCKET_NAME, Key=item)

                if 'Body' in response:
                    json_content = json.loads(response['Body'].read())
                    json_relations[json_content['email']] = S3Client.generate_signed_url(item)
            return json_relations

        except Exception as e:
            print(str(e))
            return {
                'statusCode': 500,
                'body': str(e)
            }
