import pandas as pd
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values


def connection_to_postgres():
    return psycopg2.connect(
        dbname = 'bank_database',
        user = 'postgres',
        password = '02091997',
        host = 'localhost',
        port = 5432
    )


def log_start(process_name):
    connection = connection_to_postgres()
    cur = connection.cursor()
    start_time = datetime.now()
    cur.execute(
        ''' 
        INSERT INTO LOGS.ETL_LOGS(process_name, start_time, status)
        VALUES (%s, %s, %s) RETURNING id_logs
        ''',
        (process_name, start_time, 'STARTED')
    )
    log_id = cur.fetchone()[0]
    connection.commit()
    cur.close()
    connection.close()
    return log_id


def log_end(log_id, status = 'SUCCESS'):
    connection = connection_to_postgres()
    cur = connection.cursor()
    end_time = datetime.now()
    cur.execute(
        ''' 
        UPDATE LOGS.ETL_LOGS SET end_time = %s, status = %s WHERE id_logs = %s
        ''',
        (end_time, status, log_id)
    )
    connection.commit()
    cur.close()
    connection.close()


def load_data_to_FT_BALANCE_F():
    df = pd.read_csv('/home/olesya/Projects/project_neoflex_de/data/ft_balance_f.csv', sep = ';', parse_dates=["ON_DATE"])
    df.columns = df.columns.str.lower()
    df = df.where(pd.notnull(df), None)
    df = df.drop_duplicates(subset=["on_date", "account_rk"], keep="last")
    records = list(df[['on_date', 'account_rk', 'currency_rk', 'balance_out']].itertuples(index=False, name=None))
    connection = connection_to_postgres()
    cur = connection.cursor()
    sql = ''' 
        INSERT INTO DS.FT_BALANCE_F (on_date, account_rk, currency_rk, balance_out)
        VALUES %s
        ON CONFLICT (on_date, account_rk)
        DO UPDATE SET balance_out = EXCLUDED.balance_out,
        currency_rk = EXCLUDED.currency_rk
        '''
    execute_values(cur, sql, records)
    connection.commit()
    cur.close()
    connection.close()


def load_data_to_FT_POSTING_F():
    df = pd.read_csv('/home/olesya/Projects/project_neoflex_de/data/ft_posting_f.csv', sep = ';')
    df.columns = df.columns.str.lower()
    df = df.where(pd.notnull(df), None)
    df['oper_date'] = pd.to_datetime(df['oper_date'], format='%d-%m-%Y').dt.date
    records = list(df[['oper_date', 'credit_account_rk', 'debet_account_rk', 'credit_amount', 'debet_amount']].itertuples(index=False, name=None))
    connection = connection_to_postgres()
    cur = connection.cursor()
    cur.execute("TRUNCATE TABLE DS.FT_POSTING_F")
    sql = ''' 
        INSERT INTO DS.FT_POSTING_F (oper_date, credit_account_rk, debet_account_rk,
        credit_amount, debet_amount)
        VALUES %s
        '''
    execute_values(cur, sql, records)
    connection.commit()
    cur.close()
    connection.close()

# другой подход, где мы обновляем не все записи с одинаковым ключом, а только действительно отличающиеся
def load_data_to_MD_ACCOUNT_D():
    df = pd.read_csv('/home/olesya/Projects/project_neoflex_de/data/md_account_d.csv', sep = ';', parse_dates=["DATA_ACTUAL_DATE", "DATA_ACTUAL_END_DATE"])
    df.columns = df.columns.str.lower()
    df = df.where(pd.notnull(df), None)
    connection = connection_to_postgres()
    df_old = pd.read_sql('SELECT data_actual_date, data_actual_end_date, account_rk, account_number, char_type, currency_rk, currency_code FROM DS.MD_ACCOUNT_D', connection)
    
    df_old['data_actual_date'] = pd.to_datetime(df_old['data_actual_date'])
    df_old['data_actual_end_date'] = pd.to_datetime(df_old['data_actual_end_date'])
    df['data_actual_date'] = pd.to_datetime(df['data_actual_date'])
    df['data_actual_end_date'] = pd.to_datetime(df['data_actual_end_date'])

    merged = df.merge(df_old, on = ['data_actual_date', 'account_rk'], how = 'left', suffixes = ('', '_old'))
    
    check = (
        (merged['data_actual_end_date'] != merged['data_actual_end_date_old']) |
        (merged['account_number'] != merged['account_number_old']) |
        (merged['char_type'] != merged['char_type_old']) |
        (merged['currency_rk'] != merged['currency_rk_old']) |
        (merged['currency_code'] != merged['currency_code_old']) |
        (merged['char_type_old'].isna())
    )

    df_diff = merged.loc[check, ['data_actual_date', 'data_actual_end_date', 'account_rk', 'account_number', 'char_type', 'currency_rk', 'currency_code']]
    df_diff = df_diff.drop_duplicates(subset=['data_actual_date', 'account_rk'], keep='last')
    records = list(df_diff.itertuples(index = False, name = None))
    cur = connection.cursor()
    sql = ''' 
        INSERT INTO DS.MD_ACCOUNT_D (data_actual_date, data_actual_end_date,
        account_rk, account_number, char_type, currency_rk, currency_code)
        VALUES %s
        ON CONFLICT (data_actual_date, account_rk) DO UPDATE SET
        data_actual_end_date = EXCLUDED.data_actual_end_date,
        account_number = EXCLUDED.account_number,
        char_type = EXCLUDED.char_type,
        currency_rk = EXCLUDED.currency_rk,
        currency_code = EXCLUDED.currency_code
        '''
    execute_values(cur, sql, records)
    connection.commit()
    cur.close()
    connection.close()


def load_data_to_MD_CURRENCY_D():
    df = pd.read_csv('/home/olesya/Projects/project_neoflex_de/data/md_currency_d.csv', sep=';', parse_dates=["DATA_ACTUAL_DATE", "DATA_ACTUAL_END_DATE"])
    df.columns = df.columns.str.lower()
    df = df.drop_duplicates(subset=["currency_rk", "data_actual_date"], keep="last")
    df['currency_code_str'] = df['currency_code'].apply(lambda x: f"{int(float(x)):03d}" if pd.notna(x) else None)
    df = df.where(pd.notnull(df), None)
    records = list(df[['currency_rk', 'data_actual_date', 'data_actual_end_date', 'currency_code_str', 'code_iso_char']].itertuples(index=False, name=None))

    connection = connection_to_postgres()
    cur = connection.cursor()
    sql = '''
        INSERT INTO DS.MD_CURRENCY_D (currency_rk, data_actual_date, data_actual_end_date, currency_code, code_iso_char)
        VALUES %s
        ON CONFLICT (currency_rk, data_actual_date) DO UPDATE SET
          data_actual_end_date = EXCLUDED.data_actual_end_date,
          currency_code = EXCLUDED.currency_code,
          code_iso_char = EXCLUDED.code_iso_char
    '''
    execute_values(cur, sql, records)
    connection.commit()
    cur.close()
    connection.close()


def load_data_to_MD_EXCHANGE_RATE_D():
    df = pd.read_csv('/home/olesya/Projects/project_neoflex_de/data/md_exchange_rate_d.csv', sep = ';', parse_dates=["DATA_ACTUAL_DATE", "DATA_ACTUAL_END_DATE"])
    df.columns = df.columns.str.lower()
    df = df.where(pd.notnull(df), None)
    df = df.drop_duplicates(subset=["data_actual_date", "currency_rk"], keep="last")
    records = list(df[['data_actual_date', 'data_actual_end_date', 'currency_rk', 'reduced_cource', 'code_iso_num']].itertuples(index=False, name=None))
    connection = connection_to_postgres()
    cur = connection.cursor()
    sql = '''
        INSERT INTO DS.MD_EXCHANGE_RATE_D (data_actual_date, data_actual_end_date,
        currency_rk, reduced_cource, code_iso_num)
        VALUES %s
        ON CONFLICT (data_actual_date, currency_rk)
        DO UPDATE SET 
        data_actual_end_date = EXCLUDED.data_actual_end_date,
        reduced_cource = EXCLUDED.reduced_cource,
        code_iso_num = EXCLUDED.code_iso_num
        '''
    execute_values(cur, sql, records)
    connection.commit()
    cur.close()
    connection.close()


def load_data_to_MD_LEDGER_ACCOUNT_S():
    df = pd.read_csv('/home/olesya/Projects/project_neoflex_de/data/md_ledger_account_s.csv', sep = ';', parse_dates=["START_DATE", "END_DATE"])
    df.columns = df.columns.str.lower()
    df = df.where(pd.notnull(df), None)
    df = df.drop_duplicates(subset=["ledger_account", "start_date"], keep="last")
    records = list(df[['chapter', 'chapter_name', 'section_number', 'section_name', 'subsection_name', 'ledger1_account', 'ledger1_account_name',
    'ledger_account', 'ledger_account_name', 'characteristic', 'start_date', 'end_date']].itertuples(index=False, name=None))
    connection = connection_to_postgres()
    cur = connection.cursor()
    sql = ''' 
        INSERT INTO DS.MD_LEDGER_ACCOUNT_S (chapter, chapter_name, section_number,
        section_name, subsection_name, ledger1_account, ledger1_account_name,
        ledger_account, ledger_account_name, characteristic, start_date, end_date)
        VALUES %s
        ON CONFLICT (ledger_account, start_date)
        DO UPDATE SET
        chapter = EXCLUDED.chapter,
        chapter_name = EXCLUDED.chapter_name,
        section_number = EXCLUDED.section_number,
        section_name = EXCLUDED.section_name,
        subsection_name = EXCLUDED.subsection_name,
        ledger1_account = EXCLUDED.ledger1_account,
        ledger1_account_name = EXCLUDED.ledger1_account_name,
        ledger_account_name = EXCLUDED.ledger_account_name,
        end_date = EXCLUDED.end_date
        '''
    execute_values(cur, sql, records)
    connection.commit()
    cur.close()
    connection.close()