from pydantic import BaseModel
from typing import Optional

class MESSAGE(BaseModel):
    user_id : str
    syllabus_event_id : str
    message : str
    context : Optional[str] = ""
    asistente_nombre : Optional[str] = ""
    usuario_nombre : Optional[str] = ""
    usuario_rol : Optional[str] = ""
    institucion  : Optional[str] = ""
    curso : Optional[str] = ""
    model_id: Optional[str] = None
 
class USER_DATA(BaseModel):
    user_id : str
    syllabus_event_id : str

class SILABO_DATA(BaseModel): 
    syllabus_event_id : str

class RESOURCE_DATA(BaseModel):
    RecursoDidacticoId : Optional[str] = None,
    TituloRecurso : Optional[str] = None,
    DriveId : Optional[str] = None
    SilaboEventoId : Optional[str] = None

class SILABO(BaseModel):
    SilaboEventoId : Optional[str] = None
    TituloSilabo : Optional[str] = None
    UnidadAprendizajeId : Optional[str] = None
    TituloUnidad : Optional[str] = None
    SesionAprendizajeId : Optional[str] = None
    TituloSesion : Optional[str] = None
    RecursoReferenciaId : Optional[str] = None
    RecursoDidacticoId : Optional[str] = None
    TituloRecurso : Optional[str] = None
    DriveId : Optional[str] = None
