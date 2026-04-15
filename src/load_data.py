from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)

user = os.getenv('user')
password = os.getenv('postgres_password')
database = os.getenv('database')
#host = 'host.docker.internal'
host = "postgres"

def get_engine():
    logging.info(f"→ Conectando em {host}:5432/{database}")
    return create_engine(
        f"postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:5432/{database}"
    )
   
def create_table_if_not_exists(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cascacity_weather (
                id                SERIAL PRIMARY KEY,
                weather_id        INTEGER,
                weather_main      VARCHAR,
                weather_description VARCHAR,
                coletado_em       TIMESTAMPTZ,
                cidade_id         INTEGER,
                cidade_nome       VARCHAR,
                temperatura       FLOAT,
                sensacao_termica  FLOAT,
                temp_min          FLOAT,
                temp_max          FLOAT,
                pressao           INTEGER,
                umidade           INTEGER,
                pressao_nivel_mar INTEGER,
                pressao_solo      INTEGER,
                visibilidade      INTEGER,
                vento_velocidade  FLOAT,
                vento_direcao     INTEGER,
                vento_rajada      FLOAT,
                nuvens_cobertura  INTEGER,
                condicao_nuvens   VARCHAR,
                http_status       INTEGER,
                status_descricao  VARCHAR,
                nascer_sol        TIMESTAMPTZ,
                por_sol           TIMESTAMPTZ,
                sys_type          INTEGER,
                sys_id            INTEGER,
                pais              VARCHAR,
                fuso_horario      INTEGER,
                longitude         FLOAT,
                latitude          FLOAT,
                estacao_base      VARCHAR
            )
        """))
        conn.commit()
    logging.info("✓ Tabela verificada/criada")   


def load_weather_data(table_name: str, df):
    engine = get_engine()  # ← aqui chama a funcao criada acima 
    create_table_if_not_exists(engine)
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False
    )
    logging.info(f"✅ Dados carregados com sucesso!\n")
    
    df_check = pd.read_sql(f'SELECT * FROM {table_name}', con=engine)
    logging.info(f"Total de registros na tabela: {len(df_check)}\n")