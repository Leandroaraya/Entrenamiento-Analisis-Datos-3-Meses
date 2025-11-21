#**Ejercicio**: Construir pipeline ETL completo con manejo robusto de errores y logging

"""
Logging:Logging es un sistema para registrar lo que hace tu programa mientras se está ejecutando.
Es como un diario o bitácora donde se va escribiendo:
qué acciones realiza el programa,
qué errores ocurren,
cuánto tarda cada parte,
qué datos se procesaron,
si algo salió mal y dónde,
si todo salió bien.
Sirve para saber exactamente qué pasó durante la ejecución del programa.


¿Qué problema soluciona?

Los pipelines ETL suelen fallar por:
Datos corruptos
Archivos faltantes
Conexiones a bases de datos
Tipos de datos incorrectos
Campos nulos
Errores inesperados
Si no tienes logging, es imposible saber exactamente dónde falló el ETL ni qué ocurrió.


"""
#Configurar logging estructurado:
import logging
import pandas as pd
import sqlite3
import time
from pathlib import Path

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('etl_pipeline')

#--------------------------------------------------------------------------------


#Crear clase de pipeline robusto:
"""
¿Qué es esa clase RobustETLPipeline?
Es una versión avanzada de un pipeline ETL que incorpora:
1️ Manejo de errores profesional
2️ Reintentos automáticos
3️ Transacciones seguras
4️ Métricas de ejecución
5️ Logging detallado

En otras palabras: Es un ETL que no se cae fácilmente y que deja registro de todo lo que pasó.

----------------------------
Logging = registro
RobustETL = proceso que usa ese registro
"""

"""
¿Qué hace __init__? Es el constructor. Se ejecuta automáticamente cuando creas un pipeline.
Al ejecutarse RobustETLPipeline, hace:

self.db_path = db_path                  # ruta de la base de datos
self.logger = logging.getLogger(...)    # logger para registrar eventos
self.metrics = {...}                    # métricas del pipeline
"""

"""
¿Qué hace run_pipeline?
Es el "jefe de la operación".
Orquesta TODO el ETL:
Extrae datos → extract_with_retry()
Transforma → transform_with_validation()
Carga → load_with_transaction()
Reporta éxito → report_success()
Si algo falla → report_failure()
Lo hace dentro de un try…except:
"""

"""
. ¿Qué hace extract_with_retry?

Esta parte es MUY importante y también MUY útil.
Normalmente, la extracción puede fallar:
API caída
archivo bloqueado
red intermitente
servidor lento
Entonces este método intenta 3 veces antes de rendirse.
"""
class RobustETLPipeline:    
    def __init__(self, db_path='etl_database.db'):  # Es el constructor.Se ejecuta automáticamente cuando creas un pipeline.
        self.db_path = db_path
        self.logger = logging.getLogger('etl_pipeline')
        self.metrics = {'processed': 0, 'errors': 0, 'start_time': None}
    
    def run_pipeline(self): #Es el "jefe de la operación". Orquesta TODO el ETL
        self.metrics['start_time'] = pd.Timestamp.now()
        self.logger.info("=== INICIANDO PIPELINE ETL ROBUSTO ===")
        
        try:
            # Fase 1: Extracción con reintentos
            data = self.extract_with_retry()
            
            # Fase 2: Transformación con validaciones
            transformed_data = self.transform_with_validation(data)
            
            # Fase 3: Carga con transacciones
            self.load_with_transaction(transformed_data)
            
            self.report_success()
            
        except Exception as e:
            self.report_failure(e)
            raise
    
    def extract_with_retry(self):
        """Extracción con estrategia de reintentos"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Intento de extracción #{attempt + 1}")
                
                # Simular extracción (reemplazar con lógica real)
                data = pd.DataFrame({
                    'id': range(1, 101),
                    'valor': [x * 1.1 for x in range(1, 101)],
                    'categoria': ['A', 'B', 'C'] * 33 + ['A']
                })
                
                self.logger.info(f"Extracción exitosa: {len(data)} registros")
                return data
                
            except Exception as e:
                self.logger.warning(f"Intento #{attempt + 1} falló: {e}")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(1)  # Esperar antes de reintentar

    """
    En resumen
    ✔ __init__
    Prepara el pipeline (ruta DB, logger, métricas).
    ✔ run_pipeline
    Ejecuta TODAS las fases en orden y maneja errores globales.
    ✔ extract_with_retry
    Intenta extraer datos hasta 3 veces.
    Es un método que previene fallos en la etapa de extracción.
    """
    #-----------------------------------------------------------------------------------------

    #Implementar transformación con validaciones:
    """
    ¿Qué es transform_with_validation?
    Es un método (una función dentro de la clase) que realiza:
    Validaciones
    Transformaciones
    Control de errores
    Logging detallado
    Representa la segunda fase del ETL: la T de Transform (Transformación).
    """
    def transform_with_validation(self, data):
            """Transformación con validaciones y logging detallado"""
            self.logger.info("Iniciando transformación")
            original_count = len(data)
            # Informa en el archivo log que está comenzando la transformación. Guarda cuántos registros
            # tenía el dataframe original (para comparar después).
            try:
                # Validación 1: Datos no nulos
                if data.isnull().any().any():
                    null_counts = data.isnull().sum()
                    self.logger.warning(f"Valores nulos encontrados: {null_counts[null_counts > 0].to_dict()}")
                """
                Busca si hay algún valor nulo en el dataframe.
                ✔ Si hay nulos, registra un WARNING (no detiene el pipeline).
                ✔ Te dice cuántos nulos tiene cada columna.
                """
                # Transformación 1: Limpiar datos Elimina todas las filas con nulos. Es una decisión común en ETL cuando no quieres imputar datos.
                data_clean = data.dropna()
                
                # Transformación 2: Crear nuevas columnas
                data_clean = data_clean.copy()  # Evitar SettingWithCopyWarning
                data_clean['valor_cuadrado'] = data_clean['valor'] ** 2
                data_clean['categoria_normalizada'] = data_clean['categoria'].str.upper()
                
                # Validación 2: Resultados razonables, Evita cargar datos corruptos o imposibles en la base de datos.
                if (data_clean['valor_cuadrado'] < 0).any():
                    raise ValueError("Valores cuadrados negativos detectados")
                
                self.logger.info(f"Transformación exitosa: {original_count} -> {len(data_clean)} registros")
                return data_clean
                
            except Exception as e:
                self.logger.error(f"Error en transformación: {e}")
                raise
    #-----------------------------------------------------------------------------------------------

    #Implementar carga con transacciones:
    """
    ¿Qué es load_with_transaction?

    Es un método dentro de la clase RobustETLPipeline que se encarga de la CARGA (Load) del proceso ETL.

    Su objetivo es:

    Guardar datos transformados en una base de datos SQLite
    De forma segura, controlada y sin riesgo de corrupción
    Usando transacciones para poder hacer rollback si algo sale mal
    """
    """
    Una transacción es un bloque de operaciones que funcionan como un todo o nada:

    ✔ Si todo sale bien → COMMIT → se guardan los cambios
    ❌ Si algo falla → ROLLBACK → se revierte todo

    Así te aseguras de que la base quede siempre en un estado válido.
    """
    def load_with_transaction(self, data):
            """Carga con soporte transaccional y rollback"""
            self.logger.info("Iniciando carga a base de datos")
            
            with sqlite3.connect(self.db_path) as conn: #Abrir conexión a SQLite
                try:
                    # Iniciar transacción 
                    conn.execute('BEGIN TRANSACTION')
                    
                    # Crear tabla si no existe
                    conn.execute('''
                        CREATE TABLE IF NOT EXISTS datos_transformados (
                            id INTEGER PRIMARY KEY,
                            valor REAL,
                            categoria TEXT,
                            valor_cuadrado REAL,
                            categoria_normalizada TEXT
                        )
                    ''')
                    
                    # Limpiar datos previos (estrategia replace)
                    conn.execute('DELETE FROM datos_transformados')
                    
                    # Insertar datos del df a la tabla
                    data.to_sql('datos_transformados', conn, index=False, if_exists='append')
                    
                    # Commit transacción, confirmar cambios, si no hay error se guardan los cambios
                    conn.commit()
                    
                    self.logger.info(f"Carga exitosa: {len(data)} registros insertados")
                    
                except Exception as e:
                    # Rollback automático por context manager si hay algun error dentro
                    self.logger.error(f"Error en carga, ejecutando rollback: {e}")
                    raise

    #-------------------------------------------------------------------------------------------------------

    #Implementar reporting y ejecutar pipeline:Estas funciones informan el resultado final del pipeline, usando logging y métricas.
        #
    def report_success(self):
        """Reportar métricas de éxito"""
        duration = pd.Timestamp.now() - self.metrics['start_time']
        self.logger.info("=== PIPELINE ETL COMPLETADO EXITOSAMENTE ===")
        self.logger.info(f"Duración total: {duration}")
        self.logger.info(f"Registros procesados: {self.metrics.get('processed', 0)}")
    
    def report_failure(self, error):
        """Reportar detalles de fallo"""
        duration = pd.Timestamp.now() - self.metrics['start_time']
        self.logger.error("=== PIPELINE ETL FALLÓ ===")
        self.logger.error(f"Duración hasta fallo: {duration}")
        self.logger.error(f"Error: {error}")

# Ejecución del pipeline
if __name__ == "__main__":
    pipeline = RobustETLPipeline()
    pipeline.run_pipeline()
    
    # Verificar resultados en la base de datos
    with sqlite3.connect('etl_database.db') as conn:
        result = pd.read_sql('SELECT COUNT(*) as registros FROM datos_transformados', conn)
        print(f"Registros en base de datos: {result.iloc[0,0]}")





"""
BEGIN → inicia transacción
Hace operaciones
Si todo bien → COMMIT
Si algo falló → ROLLBACK
Esto es lo que usa el pipeline.
"""

"""
Resumen general ultimo bloque

🔹 Cierra el pipeline
Llama al proceso ETL completo
Notifica si salió bien o mal
Mide tiempos y cantidad de datos
🔹 Facilita monitoreo
Logs de éxito y error quedan escritos
Permiten revisar la salud del pipeline
🔹 Valida la carga final
Verifica que los datos se guardaron efectivamente en SQLite
"""

"""
Todo este ejercicio funciona con un DataFrame “de mentira” que viene dentro del método extract_with_retry() solo para practicar la estructura completa de un 
pipeline ETL real.
Pero en un ETL real, tú NO vas a tener ese DataFrame inventad
Entonces… ¿qué pasa a futuro?
✔ 1. Sí, debes reemplazar el DataFrame interno por tus fuentes reale

A futuro (real)
Podría ser:
Desde un CSV:
data = pd.read_csv("mis_clientes.csv")
Desde una API:
data = requests.get(url).json()
data = pd.DataFrame(data)
Desde una base SQL:
data = pd.read_sql("SELECT * FROM clientes", conn)

No necesitas eliminar nada, pero sí reemplazar la fuente
Tu pipeline ETL queda así para producción:
EXTRACT → TRANSFORM → LOAD
La parte que cambia es EXTRACT.
Puedes dejar todo el pipeline igual, solo cambias el método extract_with_retry().


👉 El pipeline ETL existe justamente para limpiar, validar y transformar los datos automáticamente.
📌 Entonces… ¿cuándo deben estar limpios los datos?
❌ Antes del ETL NO deben estar limpios
El ETL recibe datos sucios, incompletos, inconsistentes… y su trabajo es arreglarlos.
✅ Después del ETL sí deben quedar limpios

el ETL identifica los problemas… pero no los repara automáticamente, a menos que tú programes esa lógica de reparación dentro del pipeline.

"""