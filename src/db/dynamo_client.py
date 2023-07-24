import datetime
import hashlib

import boto3
import pytz

from src.config.enviroments import ENVS


class DynamoClient:
    def __init__(self):
        self.dynamodb = boto3.client('dynamodb')

    def insert_report(self, filename):
        dynamodb = boto3.client('dynamodb')

        current_datetime = datetime.datetime.now(pytz.timezone('UTC'))
        iso_format_date = current_datetime.isoformat()
        md5_hash = hashlib.md5(f'{filename.split("/")[-1].split(".")[0].split("-logs")[0]}'.encode()).hexdigest()

        item_to_check_existence = {
            'id': {'S': md5_hash}
        }

        try:
            response = self.dynamodb.get_item(TableName=ENVS.DYNAMO_TABLE_REPORTS, Key=item_to_check_existence)
            if 'Item' in response:
                print("Existe un item con  el mismo ID, se realiza UPDATE del status [REPORTS]")
                attr_name = self.type_determine(filename)
                self.dynamodb.update_item(
                    TableName=ENVS.DYNAMO_TABLE_REPORTS,
                    Key=item_to_check_existence,
                    UpdateExpression=f'SET #{attr_name}Attr = :{attr_name}Value',
                    ExpressionAttributeNames={f'#{attr_name}Attr': f'{attr_name}'},
                    ExpressionAttributeValues={f':{attr_name}Value': {'S': f'{filename}'}},
                )
                print("UPDATE del status exitoso [REPORTS]")
                try:
                    # Add new index
                    new_index = [
                        {
                            'Put': {
                                'TableName': ENVS.DYNAMO_TABLE_REPORTS,
                                'Item': {
                                    'id': {'S': f'{self.type_determine(filename)}#' + f'{filename}'},
                                },
                                'ConditionExpression': 'attribute_not_exists(id)'  # Ensure id does not already exist
                            }
                        }
                    ]
                    dynamodb.transact_write_items(TransactItems=new_index)
                except Exception as e:
                    print(f"Ha ocurrido un error en la ACTUALIZACION del reporte {e}")

            else:
                print(f"Se inserta primer valor de archivo {filename} en las tablas de DynamoDB [REPORTS]")
                metadata_xlsx = [
                    {
                        'Put': {
                            'TableName': ENVS.DYNAMO_TABLE_REPORTS,
                            'Item': {
                                'id': {'S': md5_hash},
                                # 'file_json': {'S': f'{filename}'},
                                # 'file_xlsx': {'S': f'{filename}'},
                                # 'file_log': {'S': f'{filename}'},
                                'user_owner': {'S': f'{filename}'},
                                'created_at': {'S': iso_format_date}
                            },
                            'ConditionExpression': 'attribute_not_exists(id)'  # Ensure id does not already exist
                        }
                    },
                    {
                        'Put': {
                            'TableName': ENVS.DYNAMO_TABLE_REPORTS,
                            'Item': {
                                'id': {'S': f'{self.type_determine(filename)}#' + f'{filename}'},
                            },
                            'ConditionExpression': 'attribute_not_exists(id)'  # Ensure id does not already exist
                        }
                    }
                ]
                metadata_xlsx[0]['Put']['Item'][self.type_determine(filename)] = {'S': filename}
                response = dynamodb.transact_write_items(TransactItems=metadata_xlsx)
                print(f"Insercion exitosa de {filename} en las tablas de DynamoDB [REPORTS]")
        except Exception as e:
            print(f"Ha ocurrido un error en la PRIMERA insercion del reporte {e}")

    def update_metadata(self, filename):
        if filename.endswith('.xlsx'):
            md5_hash = hashlib.md5(f'{filename.split("/")[-1].split(".")[0]}'.encode()).hexdigest()
            item_to_check_existence = {
                'id': {'S': md5_hash}
            }

            try:
                # Verificar si el ítem con el nombre de archivo ya existe en la tabla
                response = self.dynamodb.get_item(TableName=ENVS.DYNAMO_TABLE_METADATA, Key=item_to_check_existence)

                if 'Item' in response:
                    print("Existe un item con  el mismo ID, se realiza UPDATE del status [METADATA]")
                    self.dynamodb.update_item(
                        TableName=ENVS.DYNAMO_TABLE_METADATA,
                        Key=item_to_check_existence,
                        UpdateExpression='SET #statusAttr = :statusValue',
                        ExpressionAttributeNames={'#statusAttr': 'status'},
                        ExpressionAttributeValues={':statusValue': {'S': 'Procesado'}}
                    )
                    print("UPDATE del status exitoso [METADATA]")
            except Exception as e:
                print(f"Ha ocurrido un error en la ACTUALIZACION de los metadatos {e}")

    def type_determine(self, filename):
        if filename.endswith(".xlsx"):
            return 'file_xlsx'
        if filename.endswith(".txt"):
            return 'file_log'
        if filename.endswith(".json"):
            return 'file_json'
        if filename.endswith(".zip"):
            return 'file_zip'
