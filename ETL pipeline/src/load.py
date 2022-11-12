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

def load_Cube():
    cur = create_engine(f'postgresql://{uid}:{pwd}@{server}:{port}/{db}')
    conn = cur.connect()
    
     # Cargar DIMENSION DE TIEMPO
    print("[1/7] - CARGA DATOS EN DIMENSION DE TIEMPO")
    cur.execute("insert into tiempo_dim (fecha) select distinct to_date(c.payment_date::text, 'YYYYMMDD') from stg_chargebacks c union select distinct to_date(c.notification_date::text, 'YYYYMMDD') from stg_chargebacks c union select distinct to_date(c.debit_date::text, 'YYYYMMDD') from stg_chargebacks c where  c.debit_date <> 0 -- la tabla puede tener valores en 0 union select distinct to_date(p.payment_date :: text, 'YYYYMMDD')  from stg_payments p;")
    cur.execute("update tiempo_dim set anio = date_part('year',fecha);")
    cur.execute("update tiempo_dim set mes = date_part('month',fecha);")
    cur.execute("update tiempo_dim set dia = date_part('day',fecha);")

     #Cargar DIMENSION DE DIVISA CON LOS TIPOS DE MONEDAS
    print("[2/7] - CARGA DATOS EN DIMENSION DE DIVISA CON LOS TIPOS DE MONEDAS")
    cur.execute("insert into divisa_dim (currency_code) select distinct c.currency_code from stg_chargebacks c union select distinct p.currency_code from stg_payments p where p.gateway_code is not null;")

    #Cargar DIMENSION CON LOS GATEWAY
    print("[3/7] - CARGA DATOS EN DIMENSION CON LOS GATEWAY")
    cur.execute("insert into gateway_dim (gateway_code) select distinct p.gateway_code from public.stg_payments p where p.gateway_code is not null; ")

    #Cargar DIMENSION TIPO DE PAGO
    print("[4/7] - CARGA DATOS EN DIMENSION TIPO DE PAGO")
    cur.execute("insert into metodoPago_dim (payment_method) select distinct p.payment_method from public.stg_payments p where p.payment_method is not null; ")
    
    #Cargar FACT TABLE DE PAGOS }
    print("[5/7] - CARGA DATOS EN FACT TABLE DE PAGOS")
    cur.execute("insert into fact_payments (payment_date, token_customer, is_credit, is_debit, amount, currency_code ,gateway_code,payment_method)select td.id_tiempo, p.token_customer, p.is_credit, p.is_debit , p.amount,case when p.currency_code = 'BRL' then 01 when p.currency_code = 'UYP' then 02 when p.currency_code = 'ARS' then 03 end, case when p.gateway_code = 'SLOPE_PROVIDER' then 1 when p.gateway_code = 'SPREEDLY_PAYU' then 2 when p.gateway_code = 'SPREEDLY_ADYEN' then 3 when p.gateway_code = 'WOMPI' then 4 when p.gateway_code = 'ADYEN' then 5 when p.gateway_code = 'KALA' then 6  when p.gateway_code = 'GOD-PAY' then 7 when p.gateway_code = 'WALLET' then 8  when p.gateway_code = 'PAYMENT' then 9 end, case  when p.payment_method = 'ADJUSTMENT' then 1 when p.payment_method = 'TRANSFER' then 2 when p.payment_method = 'TRANSFER_LINK' then 3 when p.payment_method = 'CREDIT' then 4 when p.payment_method = 'BONO' then 5 when p.payment_method = 'CASH' then 6 when p.payment_method = 'REFUND' then 7 when p.payment_method = 'SMART_LINK' then 8 when p.payment_method = 'CREDIT_CARD' then 9 when p.payment_method = 'BOLETO_BANCARIO' then 10 when p.payment_method = 'LENDING' then 11 when p.payment_method = 'ANNULMENT' then 12 end from stg_payments p inner join tiempo_dim td on (td.fecha = to_date(p.payment_date::text, 'YYYYMMDD'));")
    
    #Cargar FACT TABLE DE CHARGEBACKS
    print("[6/7] - CARGA DATOS EN FACT TABLE DE CHARGEBACKS")
    cur.execute("insert into fact_chargebacks (payment_date ,token_customer,amount,is_fraud,currency_code)select td.id_tiempo,c.token_customer, c.amount, c.is_fraud,case when c.currency_code = 'BRL' then 01 when c.currency_code = 'UYP' then 02 when c.currency_code = 'ARS' then 03 end from stg_chargebacks c inner join tiempo_dim td on (td.fecha = to_date(c.payment_date::text, 'YYYYMMDD'));")
    cur.execute("update fact_chargebacks set debit_date  = td.id_tiempo from stg_chargebacks sc inner join tiempo_dim td on (td.fecha = to_date(sc.debit_date ::text, 'YYYYMMDD'));")

    print("[7/7] - TABLAS STAGING SET PROCESADAS")
    cur.execute("ALTER TABLE fact_payments RENAME TO 'fact_payments_procesado;")
    cur.execute("ALTER TABLE fact_chargebacks RENAME TO 'fact_chargebacks_procesado;")
    conn.close()
    cur.dispose()

#main
try: 
    t_start = time.time()
    print ("[L]OAD DATA ")
    load_Cube()
    print ("[end] - LOAD DATA ")
    t_end = time.time()
    print(">>El tiempo total de ejecucion del script de [carga] es: ", end=" ")
    print("  ",t_end - t_start, "Segundos")

except Exception as e:
    print("Error mientras extracción de datos: " + str(e)) 