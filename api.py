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
from artifacts.message import MESSAGE, USER_DATA, SILABO, RESOURCE
from artifacts.dynamodb_utils import DYNAMODB_CHAT_HISTORY, DYNAMODB_LIBRARY
from artifacts.documents_load import process_event, delete_resource
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
    temperature: float = 1
    embeddings_model_id: str = "amazon.titan-embed-text-v2:0"
    aws_profile: str = "upu"
    max_iterations: int = 5
    pinecone_min_threshold: float = 0.5
    pinecone_max_retrieve_documents: int = 6
    embeddings_model_region: str = "us-west-2" 
    dynamo_library_table: str = "db_library_resources"
    bedrock_model: str = "us.meta.llama3-2-3b-instruct-v1:0"
    dynamo_chat_history_table: str
    history_cant_elements: int = 5
    llm_max_tokens: int = 1024
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
    app.state.extractor.run()
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
  
# Constantes y plantillas
'''DATA_PROMPT = PromptTemplate.from_template("""  
    Rol del Asistente Virtual:
        Eres {asistente_nombre}, un mentor personal basado en Inteligencia Artificial. Tu objetivo es guiar y apoyar al usuario, proporcionando respuestas claras y precisas según su contexto y la información disponible.

    Información Proporcionada por el Usuario:
        Nombre: {usuario_nombre}
        Rol: {usuario_rol} (Docente/Alumno)
        Institución: {institucion}
        Curso: {curso}
                  
    Contexto de la Conversación:
        Historial de Conversación:
            {chat_history}
        Base de Conocimiento:
            {bd_context}
        Pregunta del Usuario:
            {question}

    Instrucciones para la Respuesta:
        Evaluar el tipo de entrada:
            Si el usuario está saludando (ej. "Hola", "Buenos días", "Buenas tardes", etc.), responde de manera natural sin usar la base de conocimiento.
            Si la pregunta requiere información académica o técnica, utiliza la información proporcionada por el usuario, la base de conocimiento y el historial de conversación para dar una respuesta fundamentada.
        Uso del historial de conversación:
            Si la pregunta necesita contexto previo, revisa el historial y utilízalo para mantener coherencia en la respuesta.
            Si no es necesario, responde directamente sin hacer referencia a conversaciones pasadas.
        Uso de la información proporcionada por el usuario:
            Si el usuario ya ha compartido información relevante, úsala para enriquecer la respuesta.
            Si la información proporcionada es suficiente para responder la pregunta, no recurras a la base de conocimiento innecesariamente.
    Formato de respuesta:
        Si hay historial de conversación, evita saludos innecesarios y responde directamente.
        Proporciona una respuesta clara, bien estructurada y adaptada al contexto del usuario.
    Ejemplo de Respuesta:
        "Basado en la información que proporcionaste, [respuesta basada en datos del usuario]."
        "Según la base de conocimiento, [respuesta técnica]."

    Respuesta:
""")'''

'''DATA_PROMPT = PromptTemplate.from_template("""  
                                           
    ### Configuración del Chatbot "{asistente_nombre}"

    **Parámetros del Contexto de la Conversación:**
    - Rol del usuario: {usuario_rol}
    - Nombre del usuario: {usuario_nombre}
    - Curso en el que se encuentran: {curso}
    - Institución en la que se encuentran: {institucion}
    - Nombre del chatbot: {asistente_nombre}

    **Historial de la Conversación:**
    {chat_history}
                                            
    **Mensaje Original del Usuario:**
    {question}
                                            
    **Base de Conocimientos:**
    {bd_context}
                                                                                                                    
    ### Instrucciones para {asistente_nombre}:
    Eres un chatbot llamado {asistente_nombre}. 
    Tu misión es actuar como un mentor o guía para los usuarios, en este caso el usuario tiene el rol de {usuario_rol}. 
    Debes responder y conversar de manera natural como si estuvieras respondiendo directamente la respuesta del usuario sin instrucciones de código como "Respuesta de {asistente_nombre}", 
    debes ajustarte al tono formal y amigable, adapta tus respuestas según el rol del usuario y utiliza la "Base de Conocimientos" solo cuando sea relevante al mensaje original del usuario.
    Si existe historial de conversación debes responder directamente al usuario sin saludar, caso contrario debes saludar primero como el "Ejemplo de respuesta".
                                            
    Responde a la siguiente pregunta considerando todos los parámetros y la información proporcionada:

    **Pregunta del Usuario:**
    {question}
                                            
    ### Ejemplo de Respuesta:
    Hola {usuario_nombre}, soy {asistente_nombre}, tu guía en {curso}. ¿En qué puedo ayudarte hoy en relación con {curso}?

""")
'''
DATA_PROMPT = PromptTemplate.from_template("""  
                                           
    ### Configuración del Chatbot "{asistente_nombre}"

    **Parámetros del Contexto de la Conversación:**
    - Rol del usuario: {usuario_rol}
    - Nombre del usuario: {usuario_nombre}
    - Curso en el que se encuentran: {curso}
    - Institución en la que se encuentran: {institucion}
    - Nombre del chatbot: {asistente_nombre}

    **Historial de la Conversación:**
    {chat_history}
                                            
    **Mensaje Original del Usuario:**
    {question}
                                            
    **Base de Conocimientos:**
    {bd_context}
                                                                                                                    
    ### Instrucciones para {asistente_nombre}:
    Eres un chatbot llamado {asistente_nombre}. 
    Debes responder y conversar de manera natural como si estuvieras respondiendo directamente la respuesta del usuario sin instrucciones de código como "Respuesta de {asistente_nombre}", 
    debes ajustarte al tono formal y amigable, adapta tus respuestas según el rol del usuario.
    Si existe una base de conocimientos (contexto), utilízala para responder y haz la tarea indicada por el usuario, y si no hay una base de conocimiento (contexto) responde de tu propio conocimiento.                    
    Si existe historial de conversación debes responder directamente al usuario sin saludar, caso contrario debes saludar primero como el "Ejemplo de respuesta".
                                       
    Responde a la siguiente pregunta considerando todos los parámetros y la información proporcionada:

    **Pregunta del Usuario:**
    {question}
                                            
    ### Ejemplo de Respuesta:
    Hola {usuario_nombre}, soy {asistente_nombre}, tu guía en {curso}. ¿En qué puedo ayudarte hoy en relación con {curso}?

""")
    
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

# Funciones auxiliares
def classify_intent(question: str) -> str:
    chain = INTENT_PROMPT | app.state.classifier_llm
    return chain.invoke({"question": question}).strip()

def build_prompt(prompt_template: str, question: str, context: str = "", bd_context: str = "") -> str:
    return f"{prompt_template}".format(
        question=question,
        context=context,
        bd_context=bd_context
    )

def bedrock_converse(client, prompt: str, max_tokens: int, temperature: float = 0.3) -> dict:
    return client.converse(
        modelId=settings.bedrock_model,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={
            'maxTokens': max_tokens,
            'temperature': temperature,
            'topP': 0.2
        }
    )

# Endpoints   OK
@app.post("/ask", response_model=AnswerResponse)
async def ask_question(body: MESSAGE):
    try:
        # Obtener historial de conversación
        chat_history = app.state.dynamo_controller.get_message_history(
            body.user_id,
            body.syllabus_event_id,
            cant_items=settings.history_cant_elements
        )
        
        # Formatear historial
        formatted_history = "\n".join(
            [f"Usuario: {h['USER_MESSAGE']}\nChatbot: {h['AI_MESSAGE']}" 
             for h in chat_history]
        ) if chat_history else "No hay historial previo"
        
        # Obtener contexto
        data = app.state.extractor.search_in_local_db(body.syllabus_event_id)
        print("data")
        print(data)
        pinecone_context = get_documents_context(body.message, None, data)
        
        # Construir prompt
        prompt = DATA_PROMPT.format(
            asistente_nombre=body.asistente_nombre,
            usuario_nombre=body.usuario_nombre,
            usuario_rol=body.usuario_rol,
            institucion=body.institucion,
            curso=body.curso,
            chat_history=formatted_history,
            bd_context=pinecone_context,
            question=body.message
        )
        print("*******************************************************************************************")
        print(prompt)

        # Generar respuesta
        response = bedrock_converse(
            app.state.bedrock_client,
            prompt,
            settings.llm_max_tokens
        )
        answer = response['output']['message']['content'][0]['text']
        
        # Guardar en historial
        app.state.dynamo_controller.upload_message(
            body.user_id,
            body.syllabus_event_id,
            body.message,
            answer,
            body.context
        )
        
        return AnswerResponse(
            answer=answer,
            input_tokens=response['usage']['inputTokens'],
            output_tokens=response['usage']['outputTokens']
        )
        
    except Exception as e:
        logger.error(f"Error en /ask: {str(e)}")
        return AnswerResponse(
            success=False,
            answer="Ocurrió un error procesando tu solicitud",
            message=str(e)
        )
 
# 2.2. Crea el endpoint
@app.post("/search_data_db")
async def search_data2(body: DataSearchRequest):
    try:
        # Obtener los datos del silabo
        print(body.syllabus_event_id)
        data = app.state.extractor.search_in_dynamodb(body.syllabus_event_id) 
        return data
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "message": "Error al buscar datos del silabo"
            }
        )             
 
@app.post("/refresh_data") ####### CODIGO C SHARP
def refresh_data():
    try:
        app.state.extractor.run()
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
async def add_resource(body: SILABO):
    event = {
        'RecursoDidacticoId': body.RecursoDidacticoId,
        'TituloRecurso': body.TituloRecurso,
        'DriveId': body.DriveId
    }
    process_event(event)
    return APIResponse(
        success=True,
        message="Recurso agregado exitosamente"
    )

@app.post("/delete_resource", response_model=APIResponse) ###
async def delete_resource_endpoint(body: RESOURCE):
    event = {'RecursoDidacticoId': body.RecursoDidacticoId}
    delete_resource(event)
    return APIResponse(
        success=True,
        message="Recurso eliminado exitosamente"
    )

@app.post("/delete_history", response_model=APIResponse)  ###HOY
async def delete_history(body: USER_DATA):
    app.state.dynamo_controller.delete_messages(
        body.user_id,
        body.syllabus_event_id
    )
    return APIResponse(
        success=True,
        message="Historial eliminado exitosamente"
    )

@app.post("/get_history", response_model=APIResponse) ###HOY
async def get_history(body: USER_DATA):
    history = app.state.dynamo_controller.get_message_history(
        body.user_id,
        body.syllabus_event_id
    )
    return APIResponse(
        success=True,
        message="OK",
        data={"history": history}
    )

@app.get("/health")
async def health_check():
    return {"status": "OK"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)