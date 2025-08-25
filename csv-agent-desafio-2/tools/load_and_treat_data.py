import duckdb
import pandas as pd
from pathlib import Path


def load_csvs_into_duckdb(conn: duckdb.DuckDBPyConnection):
    # Caminho da raíz do projeto:
    abs_path = (
        Path(__file__).resolve().parent 
        if '__file__' in globals() else Path().resolve()
    )

    # Caminho global dos arquivos descompactados:
    path = abs_path.parent / "data" / "unzipped_data"

    # Lista dos caminhos de cada arquivo .csv:
    files_path = list(path.glob("*.csv"))

    # Iterando pela lista de caminhos de cada arquivo:
    for file_path in files_path:

        if "Cabecalho" in file_path.name:

            df_cabecalho = pd.read_csv(file_path, sep=',', decimal='.')

            df_cabecalho['DATA EMISSÃO'] = pd.to_datetime(
                df_cabecalho["DATA EMISSÃO"], format="mixed", errors="coerce"
            )
            df_cabecalho['DATA/HORA EVENTO MAIS RECENTE'] = pd.to_datetime(
                df_cabecalho["DATA/HORA EVENTO MAIS RECENTE"], format="mixed", errors="coerce"
            )

        elif "Itens" in file_path.name:

            df_itens = pd.read_csv(file_path, sep=',', decimal='.')
        
        else:
            raise FileNotFoundError(
                "Os arquivos encontrados não possuem as palavras chave: "
                "Cabecalho ou Itens em sua descrição"
            )

    list_of_desired_item_columns = list(
        set(
            sorted(df_itens.columns.unique().tolist())
        )
        -set(
            sorted(df_cabecalho.columns.unique().tolist())
        )
    )

    list_of_desired_item_columns.append("CHAVE DE ACESSO")

    df_final = df_cabecalho.merge(
        df_itens[list_of_desired_item_columns], 
        on =["CHAVE DE ACESSO"], how="left"
    )

    # Criação persistente da tabela
    conn.execute("DROP TABLE IF EXISTS notas_fiscais_info")
    conn.register("notas_fiscais_info", df_final)
    conn.execute("CREATE TABLE notas_fiscais_info AS SELECT * FROM notas_fiscais_info")

    # Sugestão: print de schema
    print("✔️ Tabela criada com sucesso!")