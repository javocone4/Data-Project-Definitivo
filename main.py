import requests
import csv
import psycopg2
import pandas as pd
import json 
import numpy as np
import psycopg2.extras as extras

  


#URL= 'https://valencia.opendatasoft.com/api/records/1.0/search/?dataset=hospitales&q=&facet=nombre&facet=financiaci&facet=tipo&facet=fecha&facet=barrio'
  
#Obtenemos el paquete/caja que nos viene de ahi con el get'

URL= 'https://valencia.opendatasoft.com/explore/dataset/hospitales/download/?format=json&timezone=Europe/Madrid&lang=es'
df = pd.read_json(URL)
#Obtenemos el paquete/caja que nos viene de ahi con el get'
print(df)
respuesta = requests.get(url=URL)

datos=respuesta.json()

df = pd.json_normalize(datos)
df.to_numpy().tolist()


#CONEXION A POSTGREESQL

connection = psycopg2.connect(user="postgres", password="Welcome01",host="postgres", port="5432", database="postgres")
cursor = connection.cursor()

cursor.execute(
  """
    CREATE TABLE IF NOT EXISTS hospitales(
    nombre varchar(50),
    coddistrit integer);
    
  """
)

for i in range(len(df)):  

  
  postgres_insert_query = """ INSERT INTO hospitales (nombre,coddistrit) VALUES (%s,%s)"""
  record_to_insert = (df['fields.barrio'][i],45)
  cursor.execute(postgres_insert_query, record_to_insert)





connection.commit()






