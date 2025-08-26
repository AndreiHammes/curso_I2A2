import pandas as pd 
import calendar
from pathlib import Path
from workalendar.america import Brazil


def read_treat_and_aggregate_data(
        current_month: int,
        current_year: int
    ):

    # Caminho base relativo à localização do script
    BASE_DIR = Path(__file__).resolve().parent.parent / "data"
    
    # Lendo os arquivos
    df_ativos = pd.read_excel(BASE_DIR / "ATIVOS.xlsx")
    df_ferias = pd.read_excel(BASE_DIR / "FÉRIAS.xlsx")
    df_desligados = pd.read_excel(BASE_DIR / "DESLIGADOS.xlsx")
    df_admitidos_mes = pd.read_excel(BASE_DIR / "ADMISSÃO ABRIL.xlsx")
    df_sindicato_valor = pd.read_excel(BASE_DIR / "Base sindicato x valor.xlsx")
    df_dias_uteis_colaborador = pd.read_excel(BASE_DIR / "Base dias uteis.xlsx", skiprows=1)
    df_exterior = pd.read_excel(BASE_DIR / "EXTERIOR.xlsx")
    df_afastamentos = pd.read_excel(BASE_DIR / "AFASTAMENTOS.xlsx")

    # Agregando tabela de col. ativos com tabela de férias
    df_final = df_ativos.merge(df_ferias, on= ["MATRICULA", "DESC. SITUACAO"], how="left")

    df_final["DIAS DE FÉRIAS"] = df_final["DIAS DE FÉRIAS"].fillna(0)

    # Agregando tabela com as informações de col. desligados
    df_desligados.columns = df_desligados.columns.str.strip()

    df_final = df_final.merge(df_desligados, on = "MATRICULA", how="left")

    # Adaptando e agregando tabela com informações de col. admitidos no mês
    df_admitidos_mes = df_admitidos_mes.drop(columns="Cargo")

    df_admitidos_mes = df_admitidos_mes.rename(
        columns={"Unnamed: 3": "INFO_ADMITIDOS_MES"}
    )

    df_admitidos_mes = df_admitidos_mes.rename(
        columns={"Admissão": "DATA_DE_ADMISSAO"}
    )

    df_final = df_final.merge(df_admitidos_mes, on="MATRICULA", how="left")

    # Adaptando e agregando tabela com informações de sindicato
    df_sindicato_valor = df_sindicato_valor.rename(
        columns={
            "ESTADO\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0": "ESTADO_SINDICATO"
            }
    )

    df_final["ESTADO_SINDICATO_SIGLA"] = df_final["Sindicato"].str.split().str[1]

    df_final.loc[
        df_final["ESTADO_SINDICATO_SIGLA"] == "RJ", "ESTADO_SINDICATO"
    ] = "Rio de Janeiro"
    df_final.loc[
        df_final["ESTADO_SINDICATO_SIGLA"] == "SP", "ESTADO_SINDICATO"
    ] = "São Paulo"
    df_final.loc[
        df_final["ESTADO_SINDICATO_SIGLA"] == "PR", "ESTADO_SINDICATO"
    ] = "Paraná"
    df_final.loc[
        df_final["ESTADO_SINDICATO_SIGLA"] == "RS", "ESTADO_SINDICATO"
    ] = "Rio Grande do Sul"

    df_sindicato_valor = df_sindicato_valor.rename(
        columns = {"VALOR": "VALOR_SINDICATO"}
    )

    df_final = df_final.merge(
        df_sindicato_valor, on = "ESTADO_SINDICATO", how="left"
    )

    # Adaptando e agregando tabela com informações de dias uteis por colaborador (por sindicato)
    df_dias_uteis_colaborador["ESTADO_SINDICATO_SIGLA"] = (
        df_dias_uteis_colaborador["SINDICADO"].str.split().str[1]
    )

    df_dias_uteis_colaborador.columns = df_dias_uteis_colaborador.columns.str.strip()

    df_dias_uteis_colaborador = df_dias_uteis_colaborador[
        ["ESTADO_SINDICATO_SIGLA", "DIAS UTEIS"]
    ].copy()

    df_final = df_final.merge(
        df_dias_uteis_colaborador, on="ESTADO_SINDICATO_SIGLA", how="left"
    )

    # Eliminando cargos indesejados para a tabela final
    cargos_indesejados = ["DIRETOR", "ESTAGIÁRIO", "ESTAGIARIO", "APRENDIZ"]

    df_final["CARGO_INDESEJADO"] = df_final["TITULO DO CARGO"].str.contains(
        "|".join(cargos_indesejados), case=False, na=False
    )

    df_final = df_final[df_final["CARGO_INDESEJADO"] == False].copy()

    df_final = df_final.drop(columns="CARGO_INDESEJADO")

    # Eliminando casos de afastamento (o retorno após afastamento foi detectado por regex, pode ser uma feature futura)
    # Por enquanto, todos os afastados não entram na análise de cálculo
    df_afastamentos = df_afastamentos.rename(columns = {"Unnamed: 3": "OBS_AFASTAMENTO"})

    mask = df_afastamentos['OBS_AFASTAMENTO'].str.contains(r'(?i)\bretorno\b', na=False)

    df_afastamentos.loc[mask, 'RETORNO_APÓS_AFASTAMENTO'] = (
        df_afastamentos.loc[mask, 'OBS_AFASTAMENTO'].str.extract(
            r'(\d{1,2}\s*/\s*\d{1,2})', expand=False
        ).str.replace(r'\s*\/\s*', '/', regex=True)
    )

    matriculas_para_eliminar = df_afastamentos["MATRICULA"].unique().tolist()

    df_final = df_final[~df_final["MATRICULA"].isin(matriculas_para_eliminar)].copy()

    # Eliminando colaboradores que estão no exterior
    df_exterior["COLABORADOR_RETORNOU"] = (
        df_exterior["Unnamed: 2"].str.contains("retornou", case=False, na=False)
    )

    matrículas_no_exterior =(
        df_exterior[~df_exterior["COLABORADOR_RETORNOU"]]
        ["Cadastro"].unique().tolist()
    )

    df_final = df_final[~df_final["MATRICULA"].isin(matrículas_no_exterior)].copy()

    # Definindo variáveis que devem ser input nosso, em um primeiro momento
    df_final["MES_ATUAL"] = current_month #Será um input em linguagem natural
    df_final["ANO_ATUAL"] = current_year 
    cal = Brazil()

    # Realizando cálculos para a obtenção da tabela final
    df_final["DIAS_UTEIS_MES_ATUAL"] = df_final.apply(
        lambda x: business_days(
            x["ANO_ATUAL"], x["MES_ATUAL"], cal
        ), 
        axis=1
    )

    df_final["QTD_DIAS_NO_MES"] = df_final.apply(
        lambda row: calendar.monthrange(
            row["ANO_ATUAL"], row["MES_ATUAL"]
            )[1], 
            axis=1
    )

    df_final["ADMITIDO_NO_MES_ATUAL"] = False 

    df_final.loc[
        df_final["DATA_DE_ADMISSAO"].dt.month == df_final["MES_ATUAL"], 
        "ADMITIDO_NO_MES_ATUAL"
    ] = True 

    df_final["DIAS_VALIDOS"] = df_final.apply(calcular_dias_validos, axis=1, cal=cal)

    df_final = df_final.drop(columns = "DIAS UTEIS")

    df_final["TOTAL"] = df_final["DIAS_VALIDOS"]*df_final["VALOR_SINDICATO"]

    df_final["Custo empresa"] = 0.8*df_final["TOTAL"]

    df_final["Desconto profissional"] = 0.2*df_final["TOTAL"]

    df_final["Competência"] = (
        df_final["MES_ATUAL"].astype(str).str.zfill(2) 
        + "/" + df_final["ANO_ATUAL"].astype(str)
    )

    df_final.columns.unique().tolist()

    df_final = df_final[
        [
            "MATRICULA", "Sindicato", "Competência", "DIAS_VALIDOS", 
            "VALOR_SINDICATO", "TOTAL", "Custo empresa", 
            "Desconto profissional", "ESTADO_SINDICATO_SIGLA",
            "DATA DEMISSÃO", "DATA_DE_ADMISSAO", "TITULO DO CARGO"
        ]
    ].copy()

    return df_final

def business_days(year, month, cal):
    return cal.get_working_days_delta(
        pd.Timestamp(year=year, month=month, day=1),
        pd.Timestamp(
            year=year, month=month, 
            day=pd.Period(f"{year}-{month}").days_in_month
        )
    ) + 1  # +1 porque o intervalo é exclusivo no final

def calcular_dias_validos(row, cal: Brazil):
    dias_validos = row["DIAS_UTEIS_MES_ATUAL"]

    ano = int(row["ANO_ATUAL"])
    mes = int(row["MES_ATUAL"])
    inicio_mes = pd.Timestamp(year=ano, month=mes, day=1)
    fim_mes = pd.Timestamp(
        year=ano, month=mes, day=calendar.monthrange(ano, mes)[1]
    )

    # --- Regras de Admissão ---
    if row["ADMITIDO_NO_MES_ATUAL"] and pd.notnull(row["DATA_DE_ADMISSAO"]):
        data_adm = pd.to_datetime(row["DATA_DE_ADMISSAO"])
        dias_validos = cal.get_working_days_delta(data_adm, fim_mes) + 1

    # --- Regras de Demissão ---
    if pd.notnull(row["DATA DEMISSÃO"]):
        data_dem = pd.to_datetime(row["DATA DEMISSÃO"])
        if data_dem.month == mes and data_dem.year == ano:
            # até dia 15 e comunicado OK
            if data_dem.day <= 15 and str(row["COMUNICADO DE DESLIGAMENTO"]).upper() == "OK":
                dias_validos = 0
            else:
                dias_validos = cal.get_working_days_delta(inicio_mes, data_dem) + 1

    # --- Regras de Férias (apenas PR) ---
    if row["ESTADO_SINDICATO_SIGLA"] == "PR":
        dias_validos -= int(row["DIAS DE FÉRIAS"])

    return max(dias_validos, 0)

def all_days_in_month(year, month):
    start = pd.Timestamp(year=year, month=month, day=1)
    end = start + pd.offsets.MonthEnd(0)  # último dia do mês
    return pd.date_range(start, end, freq="D")