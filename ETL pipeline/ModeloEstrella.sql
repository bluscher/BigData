-- tranforma los valores 0 que tiene la tabla en un valor default
update stg_chargebacks 
set debit_date = 19000101
where debit_date = 0;

-- CREAR DIMENSION DE TIEMPO
CREATE TABLE IF NOT EXISTS tiempo_dim (
   id_tiempo SERIAL PRIMARY KEY,
   -- fecha int8,
   fecha date,
   anio int8,
   mes int8,
   dia int8
);

/* convertir integer to date
select date(sc.payment_date :: text)
from stg_chargebacks sc;*/

-- #insertar en la fecha
insert into tiempo_dim (fecha)
select distinct to_date(c.payment_date::text, 'YYYYMMDD') from stg_chargebacks c 
union select distinct to_date(c.notification_date::text, 'YYYYMMDD') from stg_chargebacks c
union select distinct to_date(c.debit_date::text, 'YYYYMMDD') from stg_chargebacks c
	where  c.debit_date <> 0 -- la tabla puede tener valores en 0
union select distinct to_date(p.payment_date :: text, 'YYYYMMDD')  from stg_payments p;

/* uso de substring en caso de String
SELECT SUBSTRING(to_td.fecha, 2, 5) AS ExtractString
FROM tiempo_dim td ;*/


update tiempo_dim 
set anio = date_part('year',fecha);

update tiempo_dim 
set mes = date_part('month',fecha);

update tiempo_dim 
set dia = date_part('day',fecha);

-- CREAR DIMENSION DE DIVISA CON LOS TIPOS DE MONEDAS
CREATE TABLE IF NOT EXISTS divisa_dim (
   id_currency SERIAL PRIMARY KEY,
   currency_code varchar(3)
);

-- #insertar en la dimension los valores de las tablas Stagign#
insert into divisa_dim (currency_code) 
   select distinct c.currency_code from stg_chargebacks c union select distinct p.currency_code from stg_payments p 
   where p.gateway_code is not null; 

-- CREAR DIMENSION CON LOS GATEWAY
CREATE TABLE IF NOT EXISTS gateway_dim (
   id_gateway SERIAL PRIMARY KEY,
   gateway_code varchar(20)
);

-- #insertar en la dimension los valores de las tablas Stagign#
insert into gateway_dim (gateway_code) 
   select distinct p.gateway_code from public.stg_payments p
   where p.gateway_code is not null; 

-- CREAR DIMENSION TIPO DE PAGO
CREATE TABLE IF NOT EXISTS metodoPago_dim (
   id_paymentMethod SERIAL PRIMARY KEY,
   payment_method varchar(20)
);

-- #insertar en la dimension los valores de las tablas Stagign#
insert into metodoPago_dim (payment_method) 
   select distinct p.payment_method from public.stg_payments p
   where p.payment_method is not null; 

-- CREAR FACT TABLE DE PAGOS 
   CREATE TABLE IF NOT EXISTS Fact_payments (
   payment_id SERIAL PRIMARY KEY,
   currency_code int8,
   gateway_code int8,
   payment_method int8,
   payment_date int8,
   token_customer VARCHAR(50),
   is_credit VARCHAR(1) NOT NULL,
   is_debit VARCHAR(1) NOT NULL,
   amount float8,
   FOREIGN KEY(payment_date) REFERENCES tiempo_dim (id_tiempo),
   FOREIGN KEY(currency_code) REFERENCES divisa_dim (id_currency),
   FOREIGN KEY (gateway_code) REFERENCES gateway_dim (id_gateway),
   FOREIGN KEY (payment_method) REFERENCES metodoPago_dim (id_paymentMethod)
);

-- Carga de la tabla Fact de Pagos
insert into fact_payments (payment_date, token_customer, is_credit, is_debit, amount, currency_code ,gateway_code,payment_method)
select td.id_tiempo, p.token_customer, p.is_credit, p.is_debit , p.amount,
case 
	when p.currency_code = 'BRL' then 01
	when p.currency_code = 'UYP' then 02
	when p.currency_code = 'ARS' then 03
end, 
case 
	when p.gateway_code = 'SLOPE_PROVIDER' then 1
	when p.gateway_code = 'SPREEDLY_PAYU' then 2
	when p.gateway_code = 'SPREEDLY_ADYEN' then 3
	when p.gateway_code = 'WOMPI' then 4
	when p.gateway_code = 'ADYEN' then 5
	when p.gateway_code = 'KALA' then 6 
	when p.gateway_code = 'GOD-PAY' then 7
	when p.gateway_code = 'WALLET' then 8 
 	when p.gateway_code = 'PAYMENT' then 9
end,
case 
	when p.payment_method = 'ADJUSTMENT' then 1
	when p.payment_method = 'TRANSFER' then 2
	when p.payment_method = 'TRANSFER_LINK' then 3
	when p.payment_method = 'CREDIT' then 4
	when p.payment_method = 'BONO' then 5
	when p.payment_method = 'CASH' then 6
	when p.payment_method = 'REFUND' then 7
	when p.payment_method = 'SMART_LINK' then 8
	when p.payment_method = 'CREDIT_CARD' then 9
	when p.payment_method = 'BOLETO_BANCARIO' then 10
	when p.payment_method = 'LENDING' then 11
	when p.payment_method = 'ANNULMENT' then 12
end
from stg_payments p inner join tiempo_dim td on (td.fecha = to_date(p.payment_date::text, 'YYYYMMDD'));
;


-- CREAR FACT TABLE DE CHARGEBACKS
CREATE TABLE IF NOT EXISTS Fact_chargebacks (
   chargebacks_id SERIAL PRIMARY KEY,
   payment_date int8,
   notification_date int8,
   debit_date int8,
   currency_code int8,
   token_customer VARCHAR(50),
   amount float8,
   is_fraud VARCHAR(1) NOT null,
   FOREIGN KEY(payment_date) REFERENCES tiempo_dim (id_tiempo),
   FOREIGN KEY(notification_date) REFERENCES tiempo_dim (id_tiempo),
   FOREIGN KEY(debit_date) REFERENCES tiempo_dim (id_tiempo),
   FOREIGN KEY(currency_code) REFERENCES divisa_dim (id_currency)
);


/* prueba to remove
 * insert into fact_chargebacks (token_customer,amount,is_fraud,currency_code)
select c.token_customer, c.amount, c.is_fraud,
case 
	when c.currency_code = 'BRL' then 01
	when c.currency_code = 'UYP' then 02
	when c.currency_code = 'ARS' then 03
end
from stg_chargebacks c ;*/


-- Cargar FACT TABLE chargebacks
insert into fact_chargebacks (payment_date ,token_customer,amount,is_fraud,currency_code)
select td.id_tiempo,c.token_customer, c.amount, c.is_fraud,
case 
	when c.currency_code = 'BRL' then 01
	when c.currency_code = 'UYP' then 02
	when c.currency_code = 'ARS' then 03
end
from stg_chargebacks c inner join tiempo_dim td on (td.fecha = to_date(c.payment_date::text, 'YYYYMMDD'));
;

update fact_chargebacks  
set notification_date  = td.id_tiempo 
from stg_chargebacks sc 
inner join tiempo_dim td on (td.fecha = to_date(sc.notification_date ::text, 'YYYYMMDD'))
;

update fact_chargebacks  
set debit_date  = td.id_tiempo          
from stg_chargebacks sc 
inner join tiempo_dim td on (td.fecha = to_date(sc.debit_date ::text, 'YYYYMMDD'))
;



