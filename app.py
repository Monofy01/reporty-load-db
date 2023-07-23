import json

from src.db.dynamo_client import DynamoClient


def handler(event, context):
    dynamodb = DynamoClient()
    try:
        for record in event['Records']:
            s3_event = record['body']
            body = json.loads(s3_event)
            for item_record in body['Records']:
                dynamodb.update_metadata(item_record['s3']['object']['key'])
                dynamodb.insert_report(item_record['s3']['object']['key'])

        # TODO:  AVISAR DE ALGUNA FORMA QUE CAMBIO EL ESTATUS DE DYNAMODB
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
