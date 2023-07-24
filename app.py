import json

from src.db.dynamo_client import DynamoClient
from src.services.webhook_service import WebhookService


def handler(event, context):
    print(f"REQUEST :: {event}")
    dynamodb = DynamoClient()
    try:
        jsons = []
        for record in event['Records']:
            s3_event = record['body']
            body = json.loads(s3_event)
            for item_record in body['Records']:
                if item_record['s3']['object']['key'].endswith(".json"):
                    jsons.append((item_record['s3']['object']['key']))
                dynamodb.update_metadata(item_record['s3']['object']['key'])
                dynamodb.insert_report(item_record['s3']['object']['key'])

        webhook = WebhookService()
        webhook.envio_webhook(jsons)


        return {
            'statusCode': 200,
            'body': json.dumps("Se ha procesado correctamente los metadatos")
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps(f"Ha ocurrido un error en el procesamiento de los metadatos:: {e}")
        }
