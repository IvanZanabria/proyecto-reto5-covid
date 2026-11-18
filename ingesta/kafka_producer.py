import json
import time
import pandas as pd
from kafka import KafkaProducer

# 1. Configurar el productor de Kafka (apuntando al puerto del docker-compose)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
)

TOPIC_NAME = 'covid-casos-stream'

print("Iniciando simulación de ingesta en tiempo real hacia Kafka...")

# 2. Cargar una muestra o el dataset completo (asegúrate de la ruta de tu archivo)
# Usaremos tu archivo de respaldo o la ruta del raw
DATA_PATH = 'data/raw/casos_covid_600k.csv'  # O 'casos_covid_6k.csv' para pruebas rápidas

try:
    df = pd.read_csv(DATA_PATH)
except FileNotFoundError:
    print(f"No se encontró el archivo en {DATA_PATH}. Verifica la ruta.")
    exit()

# Rellenar valores nulos para evitar errores al serializar a JSON
df = df.where(pd.notnull(df), None)

# 3. Iterar sobre los registros y enviarlos simulando tiempo real
contador = 0
for index, row in df.iterrows():
    # Convertir la fila del DataFrame en un diccionario limpio
    caso_dict = {
        "id_caso": str(row.get("ID de caso")),
        "fecha_reporte": str(row.get("fecha reporte web")),
        "fecha_notificacion": str(row.get("Fecha de notificación")),
        "cod_depto": str(row.get("Código DIVIPOLA departamento")),
        "departamento": str(row.get("Nombre departamento")),
        "cod_muni": str(row.get("Código DIVIPOLA municipio")),
        "municipio": str(row.get("Nombre municipio")),
        "edad": int(row.get("Edad")) if row.get("Edad") is not None else 0,
        "sexo": str(row.get("Sexo")),
        "tipo_contagio": str(row.get("Tipo de contagio")),
        "ubicacion": str(row.get("Ubicación del caso")),
        "estado": str(row.get("Estado")),
        "fecha_sintomas": str(row.get("Fecha de inicio de síntomas")),
        "fecha_diagnostico": str(row.get("Fecha de diagnóstico"))
    }
    
    # Enviar evento al tópico de Kafka
    producer.send(TOPIC_NAME, value=caso_dict)
    contador += 1
    
    if contador % 100 == 0:
        print(f"Enviados {contador} casos a Kafka...")
    
    # Simular un pequeño retraso entre eventos (opcional para tiempo real, ej. 0.05 segundos)
    time.sleep(0.01)

producer.flush()
print(f"¡Ingesta finalizada! Se transmitieron exitosamente {contador} eventos al tópico '{TOPIC_NAME}' de Kafka.")