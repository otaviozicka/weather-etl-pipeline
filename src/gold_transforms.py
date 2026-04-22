import pandas as pd
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_gold_tables(engine):
    with engine.connect() as conn:

        # ── gold_temperatura_diaria ──────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold_temperatura_diaria (
                data              DATE PRIMARY KEY,
                temp_media        FLOAT,
                temp_max          FLOAT,
                temp_min          FLOAT
            )
        """))

        # ── gold_amplitude_termica ───────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold_amplitude_termica (
                data              DATE PRIMARY KEY,
                amplitude         FLOAT,
                hora_mais_quente  INTEGER,
                temp_mais_quente  FLOAT,
                hora_mais_fria    INTEGER,
                temp_mais_fria    FLOAT
            )
        """))

        # ── gold_pressao_tendencia ───────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold_pressao_tendencia (
                coletado_em       TIMESTAMPTZ PRIMARY KEY,
                pressure          INTEGER,
                variacao          INTEGER,
                tendencia         VARCHAR,
                weather_description VARCHAR
            )
        """))

        # ── gold_padrao_climatico ────────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold_padrao_climatico (
                hora              INTEGER,
                weather_main      VARCHAR,
                weather_description VARCHAR,
                ocorrencias       INTEGER,
                percentual        FLOAT,
                PRIMARY KEY (hora, weather_main)
            )
        """))

        # ── gold_sensacao_termica ────────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold_sensacao_termica (
                data                     DATE PRIMARY KEY,
                diff_media               FLOAT,
                diff_max                 FLOAT,
                hora_maior_divergencia   INTEGER,
                condicao_maior_divergencia VARCHAR
            )
        """))

        conn.commit()
    logging.info("✓ Tabelas Gold verificadas/criadas")


def upsert_gold_temperatura_diaria(engine):
    logging.info("→ Atualizando gold_temperatura_diaria...")
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO gold_temperatura_diaria (data, temp_media, temp_max, temp_min)
            SELECT
                DATE(coletado_em)                     AS data,
                ROUND(AVG(temperatura)::numeric, 2)   AS temp_media,
                MAX(temperatura)                      AS temp_max,
                MIN(temperatura)                      AS temp_min
            FROM cascacity_weather
            GROUP BY DATE(coletado_em)
            ON CONFLICT (data) DO UPDATE SET
                temp_media = EXCLUDED.temp_media,
                temp_max   = EXCLUDED.temp_max,
                temp_min   = EXCLUDED.temp_min
        """))
        conn.commit()
    logging.info("✓ gold_temperatura_diaria atualizada")


def upsert_gold_amplitude_termica(engine):
    logging.info("→ Atualizando gold_amplitude_termica...")
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO gold_amplitude_termica (
                data, amplitude, hora_mais_quente, temp_mais_quente,
                hora_mais_fria, temp_mais_fria
            )
            WITH stats AS (
                SELECT
                    DATE(coletado_em) AS data,
                    MAX(temperatura) - MIN(temperatura) AS amplitude,
                    MAX(temperatura) AS temp_mais_quente,
                    MIN(temperatura) AS temp_mais_fria
                FROM cascacity_weather
                GROUP BY DATE(coletado_em)
            ),
            hora_quente AS (
                SELECT DISTINCT ON (DATE(coletado_em))
                    DATE(coletado_em) AS data,
                    EXTRACT(HOUR FROM coletado_em)::INTEGER AS hora
                FROM cascacity_weather
                ORDER BY DATE(coletado_em), temperatura DESC
            ),
            hora_fria AS (
                SELECT DISTINCT ON (DATE(coletado_em))
                    DATE(coletado_em) AS data,
                    EXTRACT(HOUR FROM coletado_em)::INTEGER AS hora
                FROM cascacity_weather
                ORDER BY DATE(coletado_em), temperatura ASC
            )
            SELECT
                s.data,
                s.amplitude,
                hq.hora AS hora_mais_quente,
                s.temp_mais_quente,
                hf.hora AS hora_mais_fria,
                s.temp_mais_fria
            FROM stats s
            JOIN hora_quente hq ON s.data = hq.data
            JOIN hora_fria   hf ON s.data = hf.data
            ON CONFLICT (data) DO UPDATE SET
                amplitude        = EXCLUDED.amplitude,
                hora_mais_quente = EXCLUDED.hora_mais_quente,
                temp_mais_quente = EXCLUDED.temp_mais_quente,
                hora_mais_fria   = EXCLUDED.hora_mais_fria,
                temp_mais_fria   = EXCLUDED.temp_mais_fria
        """))
        conn.commit()
    logging.info("✓ gold_amplitude_termica atualizada")


def append_gold_pressao_tendencia(engine):
    logging.info("→ Inserindo gold_pressao_tendencia...")
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO gold_pressao_tendencia (
                coletado_em, pressure, variacao, tendencia, weather_description
            )
            SELECT
                coletado_em,
                pressure,
                pressure - LAG(pressure) OVER (ORDER BY coletado_em) AS variacao,
                CASE
                    WHEN pressure - LAG(pressure) OVER (ORDER BY coletado_em) < -2
                    THEN 'alerta: pressão caindo'
                    WHEN pressure - LAG(pressure) OVER (ORDER BY coletado_em) > 2
                    THEN 'melhora: pressão subindo'
                    ELSE 'estável'
                END AS tendencia,
                weather_description
            FROM cascacity_weather
            ON CONFLICT (coletado_em) DO NOTHING
        """))
        conn.commit()
    logging.info("✓ gold_pressao_tendencia atualizada")


def upsert_gold_padrao_climatico(engine):
    logging.info("→ Atualizando gold_padrao_climatico...")
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO gold_padrao_climatico (
                hora, weather_main, weather_description, ocorrencias, percentual
            )
            WITH base AS (
                SELECT
                    EXTRACT(HOUR FROM coletado_em)::INTEGER AS hora,
                    weather_main,
                    weather_description,
                    COUNT(*) AS ocorrencias
                FROM cascacity_weather
                GROUP BY
                    EXTRACT(HOUR FROM coletado_em),
                    weather_main,
                    weather_description
            ),
            top_desc AS (
                SELECT DISTINCT ON (hora, weather_main)
                    hora,
                    weather_main,
                    weather_description
                FROM base
                ORDER BY hora, weather_main, ocorrencias DESC
            ),
            totais AS (
                SELECT
                    hora,
                    weather_main,
                    SUM(ocorrencias) AS ocorrencias,
                    SUM(SUM(ocorrencias)) OVER (PARTITION BY hora) AS total_hora
                FROM base
                GROUP BY hora, weather_main
            )
            SELECT
                t.hora,
                t.weather_main,
                d.weather_description,
                t.ocorrencias,
                ROUND((t.ocorrencias * 100.0 / t.total_hora)::numeric, 2) AS percentual
            FROM totais t
            JOIN top_desc d ON t.hora = d.hora AND t.weather_main = d.weather_main
            ON CONFLICT (hora, weather_main) DO UPDATE SET
                weather_description = EXCLUDED.weather_description,
                ocorrencias         = EXCLUDED.ocorrencias,
                percentual          = EXCLUDED.percentual
        """))
        conn.commit()
    logging.info("✓ gold_padrao_climatico atualizada")


def upsert_gold_sensacao_termica(engine):
    logging.info("→ Atualizando gold_sensacao_termica...")
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO gold_sensacao_termica (
                data, diff_media, diff_max,
                hora_maior_divergencia, condicao_maior_divergencia
            )
            WITH base AS (
                SELECT
                    DATE(coletado_em)                        AS data,
                    coletado_em,
                    ABS(temperatura - feels_like)            AS diff,
                    EXTRACT(HOUR FROM coletado_em)::INTEGER  AS hora,
                    weather_description
                FROM cascacity_weather
            ),
            stats AS (
                SELECT
                    data,
                    ROUND(AVG(diff)::numeric, 2) AS diff_media,
                    ROUND(MAX(diff)::numeric, 2) AS diff_max
                FROM base
                GROUP BY data
            ),
            hora_max AS (
                SELECT DISTINCT ON (data)
                    data,
                    hora        AS hora_maior_divergencia,
                    weather_description AS condicao_maior_divergencia
                FROM base
                ORDER BY data, diff DESC
            )
            SELECT
                s.data,
                s.diff_media,
                s.diff_max,
                h.hora_maior_divergencia,
                h.condicao_maior_divergencia
            FROM stats s
            JOIN hora_max h ON s.data = h.data
            ON CONFLICT (data) DO UPDATE SET
                diff_media                 = EXCLUDED.diff_media,
                diff_max                   = EXCLUDED.diff_max,
                hora_maior_divergencia     = EXCLUDED.hora_maior_divergencia,
                condicao_maior_divergencia = EXCLUDED.condicao_maior_divergencia
        """))
        conn.commit()
    logging.info("✓ gold_sensacao_termica atualizada")

def run_gold_pipeline(engine):
    logging.info("\n=== INICIANDO PIPELINE GOLD ===")
    create_gold_tables(engine)
    upsert_gold_temperatura_diaria(engine)
    upsert_gold_amplitude_termica(engine)
    append_gold_pressao_tendencia(engine)
    upsert_gold_padrao_climatico(engine)
    upsert_gold_sensacao_termica(engine)
    logging.info("=== PIPELINE GOLD CONCLUÍDO ===\n")