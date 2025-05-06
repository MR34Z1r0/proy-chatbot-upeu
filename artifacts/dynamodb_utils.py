import boto3
from datetime import datetime, timedelta 
import os
from dotenv import load_dotenv

class DYNAMODB_CHAT_HISTORY():

    def __init__(self, AWS_ACCESS_KEY_ID = None, AWS_SECRET_ACCESS_KEY = None): 
        DYNAMO_CHAT_HISTORY_TABLE = os.getenv("DYNAMO_CHAT_HISTORY_TABLE")
        if AWS_ACCESS_KEY_ID == None and AWS_SECRET_ACCESS_KEY == None:            
            self.dynamodb_resource = boto3.resource('dynamodb')
        else:
            self.dynamodb_resource = boto3.resource(
                'dynamodb',
                region_name='us-east-1',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
        self.table = self.dynamodb_resource.Table(DYNAMO_CHAT_HISTORY_TABLE)

    def upload_message(self, alumno_id: str, silabo_id: str, user_msg: str, ai_msg: str, prompt: str):
        """
        Sube un mensaje a la tabla DynamoDB con los datos especificados y un TTL de una semana.
        """
        try:
            current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Calcula el TTL como un timestamp en segundos desde la época UNIX para una semana
            ttl_seconds = 604800  # 7 días
            ttl_timestamp = int((datetime.now() + timedelta(seconds=ttl_seconds)).timestamp())

            item = {
                "ALUMNO_ID": alumno_id,
                "DATE_TIME": current_datetime,
                "SILABUS_ID": silabo_id,
                "USER_MESSAGE": user_msg,
                "AI_MESSAGE": ai_msg,
                "PROMPT": prompt,
                "IS_DELETED": False,
                "TTL": ttl_timestamp  # Agregar el TTL
            }

            self.table.put_item(Item=item)
            print("Elemento subido con éxito:", item)
        except Exception as e:
            print("Error al subir el elemento:", e)

    def get_message_history(self, alumno_id :str, silabo_id : str, cant_items : int = 100):
        """
        Obtiene los mensajes no eliminados de un ALUMNO_ID y SILABUS_ID específicos.
        """
        try:
            if cant_items > 0:
                response = self.table.query(
                    KeyConditionExpression="ALUMNO_ID = :alumno_id",
                    FilterExpression="SILABUS_ID = :silabo_id AND IS_DELETED = :is_deleted",
                    ProjectionExpression="USER_MESSAGE, AI_MESSAGE, DATE_TIME, PROMPT",
                    ExpressionAttributeValues={
                        ":alumno_id": alumno_id,
                        ":silabo_id": silabo_id,
                        ":is_deleted": False
                    },
                    ScanIndexForward=False, 
                    Limit=cant_items
                )
                messages = response.get("Items", [])
                print(f"Mensajes obtenidos para ALUMNO_ID: {alumno_id}, SILABUS_ID: {silabo_id}: {messages}")
                return messages
            return []
        except Exception as e:
            print("Error al obtener los mensajes:", e)
            return []
        
    def delete_messages(self, alumno_id :str, silabo_id : str):
        """
        Marca los mensajes de un ALUMNO_ID y SILABUS_ID específicos como eliminados.
        """
        try:
            response = self.table.query(
                KeyConditionExpression="ALUMNO_ID = :alumno_id",
                FilterExpression="SILABUS_ID = :silabo_id AND IS_DELETED = :is_deleted",  
                ExpressionAttributeValues={
                    ":alumno_id": alumno_id,
                    ":silabo_id": silabo_id,
                    ":is_deleted": False
                }
            )
            for item in response.get("Items", []):
                self.table.update_item(
                    Key={
                        "ALUMNO_ID": item["ALUMNO_ID"],
                        "DATE_TIME": item["DATE_TIME"]
                    },
                    UpdateExpression="SET IS_DELETED = :is_deleted",
                    ExpressionAttributeValues={
                        ":is_deleted": True
                    }
                )
            print(f"Mensajes de ALUMNO_ID: {alumno_id}, SILABUS_ID: {silabo_id} marcados como eliminados.")
        except Exception as e:
            print("Error al eliminar los mensajes:", e)

class DYNAMODB_LIBRARY():
    def __init__(self, AWS_ACCESS_KEY_ID = None, AWS_SECRET_ACCESS_KEY = None): 
        DYNAMO_LIBRARY_TABLE = os.getenv("DYNAMO_LIBRARY_TABLE")
        if AWS_ACCESS_KEY_ID == None and AWS_SECRET_ACCESS_KEY == None:            
            self.dynamodb_resource = boto3.resource('dynamodb')
        else:
            self.dynamodb_resource = boto3.resource(
                'dynamodb',
                region_name='us-east-1',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
        self.table = self.dynamodb_resource.Table(DYNAMO_LIBRARY_TABLE)

    def save_to_library_dynamodb(self, rows):
        """Guarda los datos en DynamoDB."""
        grouped_data = {}
        for row in rows:
            silabus_id, resource_id = row
            if silabus_id not in grouped_data:
                grouped_data[silabus_id] = []
            grouped_data[silabus_id].append({
                #"resource_reference_id": resource_reference_id,
                "resource_id": resource_id
            })

        for silabus_id, resources in grouped_data.items(): 
            self.table.put_item(
                Item={
                    "silabus_id": str(silabus_id),
                    "resources": resources
                }
            ) 
 
    def search_in_library_dynamodb(self, silabus_id):
        """Busca en DynamoDB por silabus_id."""
        response = self.table.get_item(Key={"silabus_id": silabus_id})
        return response.get("Item", None)

    def remove_resource_from_library(self, silabus_id, resource_id):
        """
        Elimina un resource_id del listado 'resources' en la tabla de library de un silabus_id específico.
        """
        try:
            # Obtener el item del silabus
            silabus_item = self.table.get_item(Key={'silabus_id': str(silabus_id)})
            
            if 'Item' in silabus_item:
                resources = silabus_item['Item'].get('resources', [])
                # Filtrar los recursos eliminando el que coincide con resource_id
                updated_resources = [r for r in resources if r['resource_id'] != resource_id]
                
                # Actualizar la entrada en DynamoDB
                self.table.update_item(
                    Key={'silabus_id': str(silabus_id)},
                    UpdateExpression="SET resources = :res",
                    ExpressionAttributeValues={':res': updated_resources}
                )
                logging.info(f"Recurso '{resource_id}' eliminado del silabus '{silabus_id}'.")
                return True
            else:
                logging.warning(f"No se encontró el silabus_id '{silabus_id}'.")
                return False

        except Exception as e:
            logging.error(f"Error al eliminar el recurso del silabus: {e}")
            return False
    
class DYNAMODB_RESOURCES():
    def __init__(self, AWS_ACCESS_KEY_ID = None, AWS_SECRET_ACCESS_KEY = None): 
        DYNAMO_RESOURCES_TABLE = os.getenv("DYNAMO_RESOURCES_TABLE")
        DYNAMO_RESOURCES_HASH_TABLE = os.getenv("DYNAMO_RESOURCES_HASH_TABLE")
        if AWS_ACCESS_KEY_ID is None and AWS_SECRET_ACCESS_KEY is None:            
            self.dynamodb_resource = boto3.resource('dynamodb')
        else:
            self.dynamodb_resource = boto3.resource(
                'dynamodb',
                region_name='us-east-1',
                aws_access_key_id = AWS_ACCESS_KEY_ID,
                aws_secret_access_key = AWS_SECRET_ACCESS_KEY
            )
        self.table_resources = self.dynamodb_resource.Table(DYNAMO_RESOURCES_TABLE)
        self.table_resources_hash = self.dynamodb_resource.Table(DYNAMO_RESOURCES_HASH_TABLE)

    def upload_in_resources(self, item):
        """Sube un registro a las tablas DynamoDB especificadas."""
        result = True
        file_hash = item['file_hash']

        # Verifica si el hash ya existe en la tabla hash
        response = self.table_resources_hash.get_item(Key={'file_hash': file_hash})
        if 'Item' not in response:
            # Registra en la tabla hash
            self.table_resources_hash.put_item(Item={'file_hash': file_hash, 's3_path': item['s3_path']})
            # Registra en la tabla principal
            self.table_resources.put_item(Item=item)
        else:
            result = False
            logging.info(f"El hash {file_hash} ya existe en la tabla DynamoDB hash.")
            
        return result
    
    def get_current_pinecone_ids(self, resource_id):
        """Recupera los IDs de Pinecone actuales asociados al resource_id en DynamoDB."""
        try:
            response = self.table_resources.get_item(
                Key={'resource_id': resource_id}
            )
            if 'Item' in response:
                return response['Item'].get('pinecone_ids', [])
            else:
                return []
        except Exception as e:
            logging.error(f"Error al obtener Pinecone IDs desde DynamoDB para {resource_id}: {e}")
            return []

    def update_in_resources_to_pinecone_ids(self, resource_id, pinecone_ids):
        """Actualiza los IDs de Pinecone en el registro correspondiente de DynamoDB."""
        try:
            self.table_resources.update_item(
                Key={'resource_id': resource_id},
                UpdateExpression="SET pinecone_ids = :ids",
                ExpressionAttributeValues={':ids': pinecone_ids}
            )
            return True
        except Exception as e:
            logging.error(f"Error al actualizar Pinecone IDs en DynamoDB: {e}")
            return False
        
    def delete_resource_and_vectors(self, resource_id, index):
        """
        Elimina un recurso de DynamoDB y sus vectores de Pinecone.
        """
        try:
            # Obtener el elemento
            response = self.table_resources.get_item(Key={'resource_id': resource_id})   
            if 'Item' not in response:
                message = f"El recurso con resource_id '{resource_id}' no existe."
                logging.info(message)
                return message

            logging.info(f"Se encontraron los registros")
            item = response['Item'] 
            file_hash = item.get('file_hash')
            pinecone_ids = item.get('pinecone_ids', [])

            # Eliminar de tabla de hash
            self.table_resources_hash.delete_item(Key={'file_hash': file_hash})

            # Eliminar vectores de Pinecone
            if pinecone_ids:
                index.delete(ids=pinecone_ids)

            # Eliminar de tabla principal
            self.table_resources.delete_item(Key={'resource_id': resource_id})

            return f"Recurso '{resource_id}' eliminado correctamente."

        except Exception as e:
            logging.error(f"No se pudo eliminar debido al error: {e}")
            return f"No se pudo eliminar el registro: {resource_id}"