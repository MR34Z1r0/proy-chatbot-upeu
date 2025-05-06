import logging
import os
from langchain.embeddings.bedrock import BedrockEmbeddings
from langchain_core.tools import tool
from pinecone import Index
from pinecone.grpc import PineconeGRPC as Pinecone

# Cargar variables de entorno
from dotenv import load_dotenv

# Cargar las variables del archivo .env
load_dotenv()

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuración inicial
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME")
PINECONE_MAX_RETRIEVE_DOCUMENTS = int(os.getenv("PINECONE_MAX_RETRIEVE_DOCUMENTS", 5))
PINECONE_MIN_THRESHOLD = float(os.getenv("PINECONE_MIN_THRESHOLD", 0.75))

# Configuración de Bedrock Embeddings
EMBEDDINGS_MODEL_ID = os.getenv("EMBEDDINGS_MODEL_ID")
BEDROCK_REGION = "us-west-2"
CREDENTIALS_PROFILE = "prd-sofia-admin"

# Inicializa la conexión con Pinecone
pc = Pinecone(api_key=PINECONE_API_KEY)
pinecone_index = pc.Index(PINECONE_INDEX_NAME)

def get_embeddings_from_bedrock(input_text):
    """Obtiene embeddings del modelo Bedrock usando LangChain."""
    embeddings = BedrockEmbeddings(
        model_id=EMBEDDINGS_MODEL_ID,
        credentials_profile_name=CREDENTIALS_PROFILE,
        region_name=BEDROCK_REGION
    )
    return embeddings.embed_query(input_text)

def query_pinecone(embeddings, syllabus_event_id=None, data=None):
    """Realiza una consulta en Pinecone y devuelve los resultados relevantes filtrando por syllabus_event_id o resource_id."""
    print(PINECONE_API_KEY)
    print(PINECONE_INDEX_NAME)

    filter_conditions = {}

    # Si syllabus_event_id tiene valor, agregarlo al filtro
    if syllabus_event_id is not None:
        filter_conditions["syllabus_event_id"] = float(syllabus_event_id)

    # Si data tiene valor, extraer los resource_id y agregarlos al filtro
    if data:
        resource_ids = [str(item[2]) for item in data]
        filter_conditions["resource_id"] = {"$in": resource_ids}

    print("filter_conditions....")
    print(filter_conditions)
    # Ejecutar la consulta en Pinecone con el filtro dinámico
    pinecone_results = pinecone_index.query(
        vector=embeddings,
        top_k=PINECONE_MAX_RETRIEVE_DOCUMENTS,
        include_metadata=True,
        filter=filter_conditions if filter_conditions else None  # Si no hay filtros, enviar None
    )

    results = pinecone_results.get("matches", [])
     
    print(f"min value : {PINECONE_MIN_THRESHOLD}")

    relevant_data = ""
    for result in results:
        if result["score"] >= PINECONE_MIN_THRESHOLD:
            document_content = str(result["metadata"].get("text", "no hay datos"))
            relevant_data += document_content.replace("\n", " ").replace("  ", " ") + '\n'

    return relevant_data

def get_documents_context(question, syllabus_event_id = None, data = None):
    """Obtiene contexto relevante para una pregunta desde Pinecone."""
    print(f"Pregunta: {question}")
    #print(f"Syllabus Event ID: {syllabus_event_id}")

    embeddings = get_embeddings_from_bedrock(question)
    #print(f"Embeddings: {embeddings}")
    relevant_data = query_pinecone(embeddings, syllabus_event_id, data)

    print(f"Datos relevantes: {relevant_data}\n" + '-'*100)
    return relevant_data

# Función para eliminar vectores asociados a un file_hash en Pinecone
def delete_vectors_by_file_hash(file_hash):
    """Elimina todos los vectores en Pinecone asociados a un file_hash."""
    try:
        query_response = pinecone_index.query(
            vector=[0.0] * 1536,  # Usando el tamaño adecuado de vector (por ejemplo, 1536)
            top_k=1000,
            include_metadata=True,
            filter={"file_hash": file_hash}
        )
        matches = query_response.get("matches", [])
        ids_to_delete = [match["id"] for match in matches]

        if ids_to_delete:
            pinecone_index.delete(ids=ids_to_delete)
            logging.info(f"Eliminados {len(ids_to_delete)} vectores antiguos con file_hash: {file_hash}.")
        else:
            logging.info(f"No se encontraron vectores antiguos para el file_hash: {file_hash}.")
    except Exception as e:
        logging.error(f"Error eliminando vectores por file_hash {file_hash}: {e}")
        raise

@tool
def search_pinecone(input_data: str) -> str:
    """Herramienta para buscar datos existentes en Pinecone basado en una pregunta."""
    try:
        print(f"Datos de entrada: {input_data}")
        data = input_data.split(',')
        question = data[1].split(":")[-1].strip()
        syllabus_event_id = int(data[0].split(":")[-1].strip())

        print(f"Pregunta: {question}")
        print(f"Syllabus Event ID: {syllabus_event_id}")

        embeddings = get_embeddings_from_bedrock(question)
        relevant_data = query_pinecone(embeddings, syllabus_event_id)

        return relevant_data
    except Exception as e:
        print(f"Error al buscar en Pinecone: {e}")
        return "Error al procesar la consulta."
