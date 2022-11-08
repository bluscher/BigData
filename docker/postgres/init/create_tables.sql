-- Creation table payments
CREATE TABLE payments (
   payment_id int8 PRIMARY KEY,
   currency_code VARCHAR(3)NOT NULL,
   gateway_code  VARCHAR(20),
   payment_method VARCHAR(20),
   payment_date int8,
   token_customer VARCHAR(50),
   is_credit VARCHAR(1) NOT NULL,
   is_debit VARCHAR(1) NOT NULL,
   amount float8
);

-- Creation table chargebacks
CREATE TABLE chargebacks (
   chargebacks_id int8 PRIMARY KEY,
   payment_date int8,
   notification_date int8,
   debit_date int8,
   currency_code VARCHAR(3),
   token_customer VARCHAR(50),
   amount float8,
   --is_fraud boolean NOT NULL,
   is_fraud VARCHAR(1) NOT NULL
);
