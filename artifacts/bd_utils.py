import pyodbc
import sqlite3
import logging
import os
import boto3
from dotenv import load_dotenv
from collections import defaultdict 
from artifacts.dynamodb_utils import DYNAMODB_LIBRARY

class SQLServerToSQLite:
    def __init__(self):
        """Inicializa la conexión y configuración."""
        load_dotenv()

        # Configuración de SQL Server
        self.sql_server = os.getenv("SQL_SERVER")
        self.sql_database = os.getenv("SQL_DATABASE")
        self.sql_username = os.getenv("SQL_USERNAME")
        self.sql_password = os.getenv("SQL_PASSWORD")

        # Configuración de SQLite
        self.local_db = "local_database.db"

        # Configuración de DynamoDB
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.dynamodb = DYNAMODB_LIBRARY(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

        # Query de SQL Server
        self.sql_query = """
        SELECT 
            CASE 
                WHEN Referencia.ActividadAprendizajeId IS NOT NULL THEN 
                    (SELECT Unidad.SilaboEventoId
                    FROM dbo.T_GC_MAE_UNIDAD_APRENDIZAJE_EVENTO AS Unidad 
                    INNER JOIN dbo.T_GC_MAE_SESION_APRENDIZAJE_EVENTO AS Sesion 
                        ON Unidad.UnidadAprendizajeId = Sesion.UnidadAprendizajeId 
                    INNER JOIN dbo.T_GC_MAE_ACTIVIDAD_EVENTO AS Actividad 
                        ON Sesion.SesionAprendizajeId = Actividad.SesionAprendizajeId
                    WHERE Actividad.ActividadAprendizajeId = Referencia.ActividadAprendizajeId)
                
                WHEN Referencia.SesionAprendizajeId IS NOT NULL THEN 
                    (SELECT Unidad.SilaboEventoId
                    FROM dbo.T_GC_MAE_UNIDAD_APRENDIZAJE_EVENTO AS Unidad 
                    INNER JOIN dbo.T_GC_MAE_SESION_APRENDIZAJE_EVENTO AS Sesion 
                        ON Unidad.UnidadAprendizajeId = Sesion.UnidadAprendizajeId
                    WHERE Sesion.SesionAprendizajeId = Referencia.SesionAprendizajeId)
                
                WHEN Referencia.UnidadAprendizajeId IS NOT NULL THEN 
                    (SELECT Unidad.SilaboEventoId
                    FROM dbo.T_GC_MAE_UNIDAD_APRENDIZAJE_EVENTO AS Unidad
                    WHERE Unidad.UnidadAprendizajeId = Referencia.UnidadAprendizajeId)
                
                ELSE Referencia.SilaboEventoId 
            END AS SilaboEventoId,
            Referencia.RecursoReferenciaId,
            Referencia.RecursoDidacticoId
        FROM T_GC_REL_RECURSO_EVENTO_REFERENCIA AS Referencia 
        INNER JOIN dbo.T_GC_MAE_RECURSO_DIDATICO_EVENTO AS Recurso 
            ON Referencia.RecursoDidacticoId = Recurso.RecursoDidacticoId
        WHERE 
            (Referencia.SilaboEventoId IS NOT NULL 
            OR Referencia.UnidadAprendizajeId IS NOT NULL 
            OR Referencia.SesionAprendizajeId IS NOT NULL 
            OR Referencia.ActividadAprendizajeId IS NOT NULL)
            AND ISNULL(Referencia.Oculto, 0) = 0
            AND Recurso.TipoId IN (397, 400, 401, 402);
        """

        # Configuración de logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    def connect_to_sql_server(self):
        """Establece conexión con SQL Server."""
        try:
            conn = pyodbc.connect(
                f"DRIVER={{SQL Server}};SERVER={self.sql_server};DATABASE={self.sql_database};"
                f"UID={self.sql_username};PWD={self.sql_password}"
            )
            logging.info("Conectado a SQL Server exitosamente.")
            return conn
        except Exception as e:
            logging.error(f"Error conectando a SQL Server: {e}")
            return None

    def connect_to_local_db(self):
        """Conecta a la base de datos local SQLite."""
        return sqlite3.connect(self.local_db)

    def create_or_truncate_table(self):
        """Crea la tabla si no existe o la vacía si ya existe."""
        conn = self.connect_to_local_db()
        cursor = conn.cursor()

        # Verificar si la tabla existe
        cursor.execute("""
            SELECT name FROM sqlite_master WHERE type='table' AND name='recursos_didacticos';
        """)
        table_exists = cursor.fetchone()

        if table_exists:
            # Si la tabla existe, limpiamos los datos
            cursor.execute("DELETE FROM recursos_didacticos;")
            logging.info("Tabla 'recursos_didacticos' truncada en SQLite.")
        else:
            # Si la tabla no existe, la creamos
            cursor.execute("""
                CREATE TABLE recursos_didacticos (
                    SilaboEventoId INTEGER,
                    RecursoReferenciaId INTEGER,
                    RecursoDidacticoId VARCHAR 
                );
            """)
            logging.info("Tabla 'recursos_didacticos' creada en SQLite.")

        conn.commit()
        conn.close()

    def fetch_data_from_sql_server(self):
        """Obtiene datos desde SQL Server."""
        conn = self.connect_to_sql_server()
        if not conn:
            return []
        
        cursor = conn.cursor()
        cursor.execute(self.sql_query)
        rows = cursor.fetchall()
        conn.close()
        return rows
  
    def save_to_local_db(self, rows):
        """Guarda los datos en la base de datos local."""
        conn = self.connect_to_local_db()
        cursor = conn.cursor()

        cursor.executemany("INSERT INTO recursos_didacticos (SilaboEventoId, RecursoReferenciaId, RecursoDidacticoId) VALUES (?, ?, ?)", rows)

        conn.commit() 
        conn.close()
        logging.info("Datos guardados en SQLite.")

    def search_in_local_db(self, value):
        """Busca en la base de datos local por un campo específico."""
        conn = self.connect_to_local_db()
        cursor = conn.cursor()

        query = f"SELECT * FROM recursos_didacticos WHERE SilaboEventoId = ?"
        cursor.execute(query, (value,))
        results = cursor.fetchall()

        conn.close()
        return results

    def search_in_local_db2(self, value):
        """Busca en la base de datos local por un campo específico."""
        conn = self.connect_to_local_db()
        cursor = conn.cursor()

        query = f"SELECT * FROM recursos_didacticos WHERE RecursoDidacticoId = ?"
        cursor.execute(query, (value,))
        results = cursor.fetchall()

        conn.close()
        return results

    def search_in_dynamodb(self, value):
        results = self.dynamodb.search_in_dynamodb(value) 
        return results
    
    def run(self):
        """Ejecuta todo el proceso: limpiar tabla, obtener datos, guardarlos y permitir búsquedas."""
        logging.info("Verificando la tabla en SQLite...")
        self.create_or_truncate_table()  # Crear o vaciar la tabla antes de cargar nueva data

        logging.info("Obteniendo datos desde SQL Server...")
        data = self.fetch_data_from_sql_server()

        if data:
            logging.info("Iniciando el grabado en bd.")
            self.save_to_local_db(data)
            logging.info("Datos almacenados localmente.")
            self.dynamodb.save_to_dynamodb(data)
            logging.info("Datos almacenados en dynamodb.")
        else:
            logging.warning("No se obtuvieron datos de SQL Server.")
 

 