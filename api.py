# api.py (versión optimizada)
from fastapi import FastAPI, HTTPException, Query, status, Depends
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from typing import Optional, Annotated, Any
import boto3
import logging
from contextlib import asynccontextmanager
from fastapi.responses import JSONResponse

from langchain_aws import BedrockLLM
from langchain.prompts import PromptTemplate
from artifacts.pinecone_utils import get_documents_context
from artifacts.message import SILABO_DATA, RESOURCE_DATA
from artifacts.dynamodb_utils import DYNAMODB_CHAT_HISTORY
from artifacts.documents_load import add_file, delete_file
from artifacts.bd_utils import SQLServerToSQLite

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    aws_access_key_id: str
    aws_secret_access_key: str
    pinecone_api_key: str
    pinecone_index_name: str
    sql_server: str
    sql_database: str
    sql_username: str
    sql_password: str
    
    # Campos opcionales con valores por defecto
    langchain_agent: str = "zero-shot-react-description"
    serp_api_key: Optional[str] = None
    max_characters: int = 500
    temperature: float = 0.4
    embeddings_model_id: str = "amazon.titan-embed-text-v2:0"
    aws_profile: str = "upu"
    max_iterations: int = 5
    pinecone_min_threshold: float = 0.75
    pinecone_max_retrieve_documents: int = 5
    embeddings_model_region: str = "us-west-2" 
    dynamo_library_table: str = "db_library_resources"
    dynamo_resources_table: str = "db_learning_resources"
    dynamo_resources_hash_table: str = "db_learning_resources_hash"
    bedrock_model: str = "us.meta.llama3-2-3b-instruct-v1:0"
    dynamo_chat_history_table: str
    history_cant_elements: int = 5
    llm_max_tokens: int = 512
    aws_region: str = "us-west-2"
    
    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'

settings = Settings()

# Contexto de aplicación y inicialización de recursos
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicializar recursos
    logger.info("Inicializando recursos...")
    app.state.extractor = SQLServerToSQLite()
    #app.state.extractor.run()
    app.state.extractor.drop_and_create_table()
    logger.info("Se inicialó el servicio de SQLServerToSQLite...")
    app.state.dynamo_controller = DYNAMODB_CHAT_HISTORY( 
        settings.aws_access_key_id,
        settings.aws_secret_access_key
    )
    logger.info("Se inicialó el servicio de DynamoDB...")
    app.state.bedrock_client = boto3.client(
        'bedrock-runtime',
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key
    )
    logger.info("Se inicialó el servicio de Bedrock...")
    app.state.classifier_llm = BedrockLLM(
        model_id=settings.bedrock_model,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_region,
        temperature=0,
        max_tokens=3
    )
    logger.info("Se inicialó el servicio de LLM...")
    yield
    
    # Limpiar recursos al apagar
    logger.info("Liberando recursos...")
    del app.state.extractor
    del app.state.dynamo_controller
    del app.state.bedrock_client
#SE AJUSTA EL SCORE
app = FastAPI(lifespan=lifespan)
  
# Modelos de respuesta
class APIResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Any] = None  # Hacer el campo opcional
    error: Optional[str] = None

class AnswerResponse(BaseModel):
    answer: str
    input_tokens: Optional[int] = None  # Hacer opcionales
    output_tokens: Optional[int] = None
    success: Optional[bool] = True
    message: Optional[str] = None

class DataSearchRequest(BaseModel):
    syllabus_event_id: str

class DataSearchRequest2(BaseModel):
    resource_id: str

# Middleware de manejo de errores
@app.middleware("http")
async def error_handling_middleware(request, call_next):
    try:
        response = await call_next(request)
        return response
    except HTTPException as he:
        return JSONResponse(
            status_code=he.status_code,
            content=APIResponse(
                success=False,
                message=he.detail, 
                data=None
            ).dict()
        )
    except Exception as e:
        logger.error(f"Error no controlado: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content=APIResponse(
                success=False,
                message="Error interno del servidor",
                data=None
            ).dict()
        )
    
@app.post("/refresh_data") 
def refresh_data(body: SILABO_DATA):
    try:
        app.state.extractor.run(body.syllabus_event_id)
        return APIResponse(
            success=True,
            message="Datos actualizados exitosamente",
            data={}  # Añadir campo data aunque sea vacío
        )
    except Exception as e:
        return APIResponse(
            success=False,
            message="Error al actualizar datos",
            error=str(e)
        )
    
@app.post("/add_resource", response_model=APIResponse)   ####
async def add_resource(body: RESOURCE_DATA):
    event = {
        'RecursoDidacticoId': body.RecursoDidacticoId,
        'TituloRecurso': body.TituloRecurso,
        'DriveId': body.DriveId
    }
    # Si sube el mismo archivo, se validará su hash para no volverlo a subir a Pinecone
    add_file(event)
    # Esto sólo va a sincronizar la tabla library en DynamoDB 
    app.state.extractor.run(body.SilaboEventoId)
    return APIResponse(
        success=True,
        message="Recurso agregado exitosamente"
    )

@app.post("/remove_resource", response_model=APIResponse) ###
async def remove_resource(body: RESOURCE_DATA):
    event = {
        'RecursoDidacticoId': body.RecursoDidacticoId,
        'TituloRecurso': body.TituloRecurso,
        'SilaboEventoId': body.SilaboEventoId
    }
    delete_file(event)
    #app.state.extractor.run(body.SilaboEventoId) #Ya no es necesario, solo actualizamos esa fila en el dynamodb
    
    # Es necesario borrar de la base de datos local el recurso eliminado
    app.state.extractor.delete_resource_in_local_db(body.RecursoDidacticoId)
    return APIResponse(
        success=True,
        message="Recurso eliminado exitosamente"
    )
 
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)