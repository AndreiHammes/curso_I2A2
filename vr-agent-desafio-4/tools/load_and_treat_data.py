import duckdb
import pandas as pd
from pathlib import Path


def load_pandas_df_into_duckdb(conn: duckdb.DuckDBPyConnection, tabela_final):

    tabela_final = tabela_final.drop(columns= ["DATA DEMISSÃO"])

    tabela_final = tabela_final.rename(
        columns = {
            "MATRICULA": "Matrícula",
            "DATA_DE_ADMISSAO": "Admissão",
            "Sindicato": "Sindicato do Colaborador",
            "DIAS_VALIDOS": "Dias",
            "VALOR_SINDICATO": "VALOR DIÁRIO VR"
        }
    )

    # Criação persistente da tabela
    conn.execute("DROP TABLE IF EXISTS tabela_compra_vr")
    conn.register("tabela_compra_vr", tabela_final)
    conn.execute("CREATE TABLE tabela_compra_vr AS SELECT * FROM tabela_compra_vr")

    print("✔️ Tabela criada com sucesso!")