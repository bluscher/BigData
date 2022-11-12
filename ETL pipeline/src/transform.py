#-- librearias conexion DB
from sqlalchemy import create_engine 
import pyodbc 
import csv
import psycopg2
import pathlib
import time

import pandas as pd
# this is imported from config folder

import os
from os import remove

#obtener password de variables de ambiente
pwd = "postgres"
uid = "postgres"
server = "localhost"
db = "postgres"
port = "5438"

def transform_toCube():
    cur = create_engine(f'postgresql://{uid}:{pwd}@{server}:{port}/{db}')
    conn = cur.connect()
    print("[1/7] - tranforma los valores 0 que tiene la tabla en un valor default")
    cur.execute("update stg_chargebacks set debit_date = 19000101 where debit_date = 0;")

    # CREAR DIMENSION DE TIEMPO
    print("[2/7] - CREAR DIMENSION DE TIEMPO")
    cur.execute("CREATE TABLE IF NOT EXISTS tiempo_dim (id_tiempo SERIAL PRIMARY KEY, fecha date,anio int8,mes int8,dia int8);")
    
    
    #CREAR DIMENSION DE DIVISA CON LOS TIPOS DE MONEDAS
    print("[3/7] - CREAR DIMENSION DE DIVISA CON LOS TIPOS DE MONEDAS")
    cur.execute("CREATE TABLE IF NOT EXISTS divisa_dim (id_currency SERIAL PRIMARY KEY,currency_code varchar(3));")
    
    #CREAR DIMENSION CON LOS GATEWAY
    print("[4/7] - CREAR DIMENSION CON LOS GATEWAY")
    cur.execute("CREATE TABLE IF NOT EXISTS gateway_dim (id_gateway SERIAL PRIMARY KEY,gateway_code varchar(20));")
   
    #CREAR DIMENSION TIPO DE PAGO
    print("[5/7] - CREAR DIMENSION TIPO DE PAGO")
    cur.execute("CREATE TABLE IF NOT EXISTS metodoPago_dim (id_paymentMethod SERIAL PRIMARY KEY, payment_method varchar(20));")
    
    #CREAR FACT TABLE DE PAGOS }
    print("[6/7] - CREAR FACT TABLE DE PAGOS")
    cur.execute("CREATE TABLE IF NOT EXISTS Fact_payments (payment_id SERIAL PRIMARY KEY, currency_code int8, gateway_code int8,payment_method int8,payment_date int8,token_customer VARCHAR(50),is_credit VARCHAR(1) NOT NULL,is_debit VARCHAR(1) NOT NULL,amount float8,FOREIGN KEY(payment_date) REFERENCES tiempo_dim (id_tiempo),FOREIGN KEY(currency_code) REFERENCES divisa_dim (id_currency),FOREIGN KEY (gateway_code) REFERENCES gateway_dim (id_gateway),FOREIGN KEY (payment_method) REFERENCES metodoPago_dim (id_paymentMethod));")
    
    #CREAR FACT TABLE DE CHARGEBACKS
    print("[7/7] - CREAR FACT TABLE DE CHARGEBACKS")
    cur.execute("CREATE TABLE IF NOT EXISTS Fact_chargebacks (chargebacks_id SERIAL PRIMARY KEY, payment_date int8,notification_date int8, debit_date int8,currency_code int8,token_customer VARCHAR(50),amount float8,is_fraud VARCHAR(1) NOT null,FOREIGN KEY(payment_date) REFERENCES tiempo_dim (id_tiempo),FOREIGN KEY(notification_date) REFERENCES tiempo_dim (id_tiempo),FOREIGN KEY(debit_date) REFERENCES tiempo_dim (id_tiempo),FOREIGN KEY(currency_code) REFERENCES divisa_dim (id_currency));")
    
    conn.close()
    cur.dispose()

#main
try: 
    t_start = time.time()
    print("[T]RANSFORM DATA")
    transform_toCube()
    print(">>end - TRANSFORM DATA")
    t_end = time.time()
    print("El tiempo total de ejecucion del script de [tranformacion] es: ", end=" ")
    print("  ",t_end - t_start, "Segundos")

except Exception as e:
    print("Error mientras extracción de datos: " + str(e)) 