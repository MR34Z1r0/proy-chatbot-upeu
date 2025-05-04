import boto3
import hashlib
import requests
import logging
import os
import unicodedata
from uuid import uuid4
from langchain.document_loaders import PyPDFLoader
from langchain.text_splitter import CharacterTextSplitter
from langchain.embeddings.bedrock import BedrockEmbeddings
from langchain_pinecone import PineconeVectorStore
from langchain_core.documents import Document
from pptx import Presentation as PptxPresentation
from docx import Document as DocxDocument
from openpyxl import load_workbook
from dotenv import load_dotenv
from langchain.text_splitter import TokenTextSplitter

load_dotenv()

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuración inicial
boto3.setup_default_session(profile_name='prd-sofia-admin')
s3 = boto3.client('s3')

EMBEDDINGS_MODEL_ID = os.getenv("EMBEDDINGS_MODEL_ID")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME")
DIMENSION = 1024

logging.info("Inicializando conexión con Pinecone.")
from pinecone import Pinecone
pc = Pinecone(api_key=PINECONE_API_KEY)
index = pc.Index(PINECONE_INDEX_NAME)

# Parámetros
bucket_name = 'datalake-cls-509399624591-landing-s3-bucket'
files_table_name = 'db_learning_resources'
hash_table_name = 'db_learning_resources_hash'
download_folder = "./downloads"
os.makedirs(download_folder, exist_ok=True)

# Funciones auxiliares
def sanitize_filename(filename):
    """Limpia caracteres especiales y espacios en el nombre del archivo."""
    filename = ''.join(c for c in unicodedata.normalize('NFD', filename) if unicodedata.category(c) != 'Mn')
    return filename.replace(' ', '_')

def download_file_from_gdrive(file_name, gdrive_id):
    """Descarga un archivo desde Google Drive y lo guarda localmente."""
    url = f"https://drive.google.com/uc?export=download&id={gdrive_id}"
    file_path = os.path.join(download_folder, file_name)
    logging.info(f"Descargando {file_name} desde Google Drive.")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return file_path

def generate_file_hash(file_path):
    """Genera un hash SHA256 para el archivo dado."""
    logging.info("Generando hash para el archivo en memoria")
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def upload_to_s3(file_path, bucket, object_name):
    """Sube un archivo local al bucket de S3."""
    full_object_name = f"SOFIA_FILE/PLANIFICACION/AV_Recursos/{object_name}"
    logging.info(f"Subiendo archivo a S3 bucket {bucket} en la ruta {full_object_name}.")
    s3.upload_file(file_path, bucket, full_object_name)
    logging.info(f"Archivo {full_object_name} subido exitosamente.")
    return f"s3://{bucket}/{full_object_name}"

def delete_resource(event):
    resource_id = event['RecursoDidacticoId']
    dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
    files_table = dynamodb.Table(files_table_name)
    hash_table = dynamodb.Table(hash_table_name)

    try:
        # Obtener el elemento de la tabla de archivos
        response = files_table.get_item(Key={'resource_id': resource_id})
        item = response.get('Item')
        
        
        if not item:
            message = f"El recurso con resource_id '{resource_id}' no existe."
            logging.info(message)
            return message
        
        file_hash = item.get('file_hash')
        pinecone_ids = response.get('pinecone_ids')

        hash_table.delete_item(Key={'file_hash': file_hash}) 
        index.delete(ids = pinecone_ids)

        files_table.delete_item(Key={'resource_id': resource_id})
    except Exception as e:
        logging.error(f"no se pudo eliminar debido al error: {e}")
        return f"no se pudo eliminar el registro : {resource_id}"

def upload_to_dynamodb(item, files_table_name, hash_table_name):
    """Sube un registro a las tablas DynamoDB especificadas."""
    result = True
    dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
    files_table = dynamodb.Table(files_table_name)
    hash_table = dynamodb.Table(hash_table_name)

    file_hash = item['file_hash']

    # Verifica si el hash ya existe en la tabla hash
    response = hash_table.get_item(Key={'file_hash': file_hash})
    if 'Item' not in response:
        # Registra en la tabla hash
        hash_table.put_item(Item={'file_hash': file_hash, 's3_path': item['s3_path']})
        # Registra en la tabla principal
        files_table.put_item(Item=item)
    else:
        result = False
        logging.info(f"El hash {file_hash} ya existe en la tabla DynamoDB hash.")
        
    return result

def process_resource_pdf(metadata, file_path):
    """Procesa un archivo PDF."""
    loader = PyPDFLoader(file_path=file_path)
    data = loader.load()
    text_splitter = CharacterTextSplitter.from_tiktoken_encoder(
        chunk_size=400,
        chunk_overlap=20
    )
    docs = text_splitter.split_documents(documents=data)
    return add_to_pinecone(metadata, docs)

def process_resource_pptx(metadata, file_path):
    """Procesa un archivo PPTX."""
    prs = PptxPresentation(file_path)
    all_slides = []
    for slide in prs.slides:
        list_slide = []
        for shape in slide.shapes:
            if hasattr(shape, 'text'):
                list_slide.append(shape.text)
        all_slides.append("\n".join(list_slide))

    documents = [Document(page_content=slide, metadata=metadata) for slide in all_slides]
    text_splitter = CharacterTextSplitter.from_tiktoken_encoder(
        chunk_size=400,
        chunk_overlap=20
    )
    docs = text_splitter.split_documents(documents=documents)
    return add_to_pinecone(metadata, docs)

def process_resource_docx(metadata, file_path):
    """Procesa un archivo DOCX."""
    doc = DocxDocument(file_path)
    all_page = [para.text for para in doc.paragraphs]

    documents = [Document(page_content=content, metadata=metadata) for content in all_page]
    text_splitter = CharacterTextSplitter.from_tiktoken_encoder(
        chunk_size=400,
        chunk_overlap=20
    )
    docs = text_splitter.split_documents(documents=documents)
    return add_to_pinecone(metadata, docs)

def add_to_pinecone(metadata, docs):
    """Agrega documentos a Pinecone y devuelve los IDs generados."""
    embeddings = BedrockEmbeddings(model_id=EMBEDDINGS_MODEL_ID, credentials_profile_name="prd-sofia-admin", region_name="us-west-2")
    vector_store = PineconeVectorStore(index=index, embedding=embeddings)
    uuids = [str(uuid4()) for _ in range(len(docs))] 
    updated_docs = []
    for doc, uuid in zip(docs, uuids):
        doc.metadata.update(metadata)
        updated_docs.append(doc)

    vector_store.add_documents(documents=updated_docs, ids=uuids)
    logging.info("Documentos agregados a Pinecone.")
    return uuids
 
def process_resource_xlsx(metadata, file_path):
    """Procesa un archivo XLSX dividiendo el contenido estrictamente para cumplir con el límite de tokens."""
    try:
        workbook = load_workbook(file_path)
        all_sheets_data = []
        max_tokens = 8000  # Límite seguro de tokens (menor que 8192)

        # Itera sobre todas las hojas del archivo
        for sheet_name in workbook.sheetnames:
            sheet = workbook[sheet_name]
            sheet_data = []

            # Lee todas las filas y columnas de la hoja
            for row in sheet.iter_rows(values_only=True):
                sheet_data.append(row)

            # Convierte los datos de la hoja a un string
            content = f"Sheet Name: {sheet_name}\nData:\n{sheet_data}"

            # Divide el contenido estrictamente en fragmentos de 8000 tokens
            text_splitter = TokenTextSplitter(chunk_size=max_tokens, chunk_overlap=0)
            chunks = text_splitter.split_text(content)

            for chunk in chunks:
                all_sheets_data.append(Document(page_content=chunk, metadata=metadata))

        # Agrega los documentos a Pinecone
        return add_to_pinecone(metadata, all_sheets_data)

    except Exception as e:
        logging.error(f"Error procesando archivo XLSX: {e}")
        return []
 
def process_event(event):
    """Procesa el evento para descargar, subir a S3 y registrar en DynamoDB."""
    try:
        logging.info("Procesando evento.")

        # Descarga el archivo desde Google Drive
        file_name = sanitize_filename(event['TituloRecurso'])
        gdrive_id = event['DriveId']
        file_path = download_file_from_gdrive(file_name, gdrive_id)

        # Genera el hash del archivo descargado
        file_hash = generate_file_hash(file_path)

        # Sube el archivo a S3
        s3_path = upload_to_s3(file_path, bucket_name, file_name)

        # Construye el objeto para DynamoDB
        dynamodb_item = {
            #'syllabus_event_id': event['SilaboEventoId'],
            #'syllabus_title': event['TituloSilabo'],
            #'learning_unit_id': event['UnidadAprendizajeId'],
            #'unit_title': event['TituloUnidad'],
            #'learning_session_id': event['SesionAprendizajeId'],
            #'session_title': event['TituloSesion'],
            #'resource_reference_id': event['RecursoReferenciaId'],
            'resource_id': event['RecursoDidacticoId'],
            'resource_title': event['TituloRecurso'],
            'drive_id': event['DriveId'],
            'file_hash': file_hash,
            's3_path': s3_path,
            'pinecone_ids': []  # Lista para almacenar los IDs de Pinecone
        }
        ####
        print(dynamodb_item)
        # Sube los datos a DynamoDB
        result = upload_to_dynamodb(dynamodb_item, files_table_name, hash_table_name)
        if result:
            logging.info("Procesando el archivo para indexación en Pinecone.")
            pinecone_ids = []

            if file_name.lower().endswith('pdf'):
                pinecone_ids = process_resource_pdf(dynamodb_item, file_path)
            elif file_name.lower().endswith('pptx'):
                pinecone_ids = process_resource_pptx(dynamodb_item, file_path)
            elif file_name.lower().endswith('docx'):
                pinecone_ids = process_resource_docx(dynamodb_item, file_path)
            elif file_name.lower().endswith('xlsx'):
                pinecone_ids = process_resource_xlsx(dynamodb_item, file_path)
            else: 
                logging.warning(f"Formato de archivo no compatible: {file_name}")

            # Actualiza los IDs de Pinecone en DynamoDB
            dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
            files_table = dynamodb.Table(files_table_name)

            files_table.update_item(
                Key={'resource_id': event['RecursoDidacticoId']},
                UpdateExpression="SET pinecone_ids = :ids",
                ExpressionAttributeValues={':ids': pinecone_ids}
            )

            logging.info("Evento procesado exitosamente.")

        # Eliminar el archivo descargado
        os.remove(file_path)
        logging.info(f"Archivo temporal {file_path} eliminado.")

    except Exception as e:
        logging.error(f"Error procesando el evento: {e}")
