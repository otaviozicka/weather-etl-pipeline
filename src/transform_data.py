import pandas as pd
from pathlib import Path
import json

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

##path_name = Path(__file__).parent.parent / 'data' / 'weather_data.json'
columns_names_to_drop = ['weather', 'weather_icon']
columns_names_to_rename = {
        "base":           "estacao_base",
        "visibility":     "visibilidade",
        "dt":             "coletado_em",      
        "timezone":       "fuso_horario",
        "id":             "city_id",
        "name":           "city_nome",
        "cod":            "http_status",
        "coord.lon":      "longitude",
        "coord.lat":      "latitude",
        "main.temp":      "temperatura",
        "main.feels_like":"feels_like",
        "main.temp_min":  "temp_min",
        "main.temp_max":  "temp_max",
        "main.pressure":  "pressure",
        "main.humidity":  "humidity",
        "main.sea_level": "pressao_nivel_mar",
        "main.grnd_level":"pressao_solo",
        "wind.speed":     "vento_velocidade",
        "wind.deg":       "vento_direcao",
        "wind.gust":      "vento_rajada",
        "clouds.all":     "nuvens_cobertura",
        "sys.type":       "sys_type",
        "sys.id":         "sys_id",
        "sys.country":    "pais",
        "sys.sunrise":    "nascer_sol",
        "sys.sunset":     "por_sol",
        # weather_id, weather_main, weather_description 
    }
columns_to_normalize_datetime = ['coletado_em', 'nascer_sol', 'por_sol']

def create_dataframe() -> pd.DataFrame:
    logging.info("→ Criando DataFrame do arquivo JSON...")
    path = Path('/opt/airflow/data/weather_data.json')
    
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    
    with open(path) as f:
        data = json.load(f)
        
    df = pd.json_normalize(data)
    logging.info(f"\n✓ DataFrame criado com {len(df)} linha(s)")
    return df

def normalize_weather_columns(df: pd.DataFrame) -> pd.DataFrame:
    df_weather = pd.json_normalize(df['weather'].apply(lambda x: x[0]))
    
    df_weather = df_weather.rename(columns={
        'id': 'weather_id',
        'main': 'weather_main',
        'description': 'weather_description',
        'icon': 'weather_icon'
    })
    
    df = pd.concat([df, df_weather], axis=1)
    logging.info(f"\n✓ Coluna 'weather' normalizada - {len(df.columns)} colunas")
    return df

def drop_columns(df: pd.DataFrame, columns_names:list[str]) -> pd.DataFrame:
    logging.info(f"\n→ Removendo colunas: {columns_names}")
    df = df.drop(columns=columns_names, errors='ignore')
    logging.info(f"✓ Colunas removidas - {len(df.columns)} colunas restantes")
    return df 

def rename_columns(df: pd.DataFrame, columns_names:dict[str, str]) -> pd.DataFrame:
    logging.info(f"\n→ Renomeando {len(columns_names)} colunas...")
    df = df.rename(columns=columns_names)
    logging.info("✓ Colunas renomeadas")
    return df 
    
def normalize_datetime_columns(df: pd.DataFrame, columns_names:list[str]) -> pd.DataFrame:
    logging.info(f"\n→ Convertendo colunas para datetime: {columns_names}")
    for name in columns_names:
        df[name] = pd.to_datetime(df[name], unit='s', utc=True).dt.tz_convert('America/Sao_Paulo')
    logging.info("✓ Colunas convertidas para datetime\n")    
    return df

def enrich_columns(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("\n→ Enriquecendo colunas...")

    status_map = {
        200: "sucesso",
        400: "requisição inválida",
        401: "chave de API inválida",
        404: "cidade não encontrada",
        429: "limite de requisições atingido",
        500: "erro interno da API",
        502: "erro interno da API",
        503: "API indisponível"
    }

    pos = df.columns.get_loc('http_status') + 1
    df.insert(pos, 'status_descricao', df['http_status'].map(status_map).fillna("desconhecido"))

    pos = df.columns.get_loc('nuvens_cobertura') + 1
    df.insert(pos, 'condicao_nuvens', pd.cut(
        df['nuvens_cobertura'],
        bins=[-1, 10, 25, 50, 84, 100],
        labels=["céu limpo", "poucas nuvens", "nuvens dispersas", "nublado", "totalmente nublado"]
    ).astype(str))

    logging.info("✓ Colunas enriquecidas")
    return df

def data_transformations():
    print("\n Iniciando transformações")
    df = create_dataframe()
    df = normalize_weather_columns(df)
    df = drop_columns(df, columns_names_to_drop)
    df = rename_columns(df, columns_names_to_rename)
    df = normalize_datetime_columns(df, columns_to_normalize_datetime)
    df = enrich_columns(df)
    logging.info("✓ Transformações concluídas\n")
    return df