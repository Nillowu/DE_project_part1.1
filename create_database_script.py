import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


connection = psycopg2.connect(
    dbname = 'postgres',
    user = 'postgres',
    password = '02091997',
    host = 'localhost',
    port = 5432
)

connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = connection.cursor()
cur.execute('CREATE DATABASE bank_database;')
cur.close()
connection.close()


connection = psycopg2.connect(
    dbname = 'bank_database',
    user = 'postgres',
    password = '02091997',
    host = 'localhost',
    port = 5432
)

cur = connection.cursor()

sql_script = """
CREATE SCHEMA IF NOT EXISTS DS;
CREATE SCHEMA IF NOT EXISTS LOGS;


CREATE TABLE IF NOT EXISTS DS.FT_BALANCE_F (
on_date DATE NOT NULL,
account_rk NUMERIC NOT NULL,
currency_rk NUMERIC,
balance_out DOUBLE PRECISION,
CONSTRAINT FT_BALANCE_F_pkey PRIMARY KEY (on_date,account_rk)
);

CREATE TABLE IF NOT EXISTS DS.FT_POSTING_F (
oper_date DATE NOT NULL,
credit_account_rk NUMERIC NOT NULL,
debet_account_rk NUMERIC NOT NULL,
credit_amount DOUBLE PRECISION,
debet_amount DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
data_actual_date DATE NOT NULL,
data_actual_end_date DATE NOT NULL,
account_rk NUMERIC NOT NULL,
account_number VARCHAR(20) NOT NULL,
char_type VARCHAR(1) NOT NULL,
currency_rk NUMERIC NOT NULL,
currency_code VARCHAR(3) NOT NULL,
CONSTRAINT MD_ACCOUNT_D_pkey PRIMARY KEY (data_actual_date,account_rk)
);

CREATE TABLE IF NOT EXISTS DS.MD_CURRENCY_D (
currency_rk NUMERIC NOT NULL,
data_actual_date DATE NOT NULL,
data_actual_end_date DATE,
currency_code VARCHAR(3),
code_iso_char VARCHAR(3),
CONSTRAINT MD_CURRENCY_D_pkey PRIMARY KEY (currency_rk,data_actual_date)
);

CREATE TABLE IF NOT EXISTS DS.MD_EXCHANGE_RATE_D (
data_actual_date DATE NOT NULL,
data_actual_end_date DATE,
currency_rk NUMERIC NOT NULL,
reduced_cource DOUBLE PRECISION,
code_iso_num VARCHAR(3),
CONSTRAINT MD_EXCHANGE_RATE_D_pkey PRIMARY KEY (data_actual_date,currency_rk)
);

CREATE TABLE IF NOT EXISTS DS.MD_LEDGER_ACCOUNT_S (
chapter CHAR(1),
chapter_name VARCHAR(16),
section_number INTEGER,
section_name VARCHAR(22),
subsection_name VARCHAR(21),
ledger1_account INTEGER,
ledger1_account_name VARCHAR(47),
ledger_account INTEGER NOT NULL,
ledger_account_name VARCHAR(153),
characteristic CHAR(1),
start_date DATE NOT NULL,
end_date DATE,
CONSTRAINT MD_LEDGER_ACCOUNT_S_pkey PRIMARY KEY (ledger_account,start_date)
);

CREATE TABLE IF NOT EXISTS LOGS.etl_logs (
id_logs SERIAL PRIMARY KEY,
process_name TEXT NOT NULL,
start_time TIMESTAMP,
end_time TIMESTAMP,
status VARCHAR(10) NOT NULL
);
"""

cur.execute(sql_script)
connection.commit()
cur.close()
connection.close()
