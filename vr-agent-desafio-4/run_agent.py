import os
import json

import pandas as pd
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

from langchain_anthropic import ChatAnthropic
from sqlalchemy import create_engine

from tools.build_final_db import read_treat_and_aggregate_data
from tools.load_and_treat_data import load_pandas_df_into_duckdb


# Carregando variáveis de ambiente
load_dotenv()
anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")

# Variáveis do banco de dados
# Carrega os arquivos no banco físico .duckdb 
# (já unidos, como uma única tablea)
duckdb_file = "base_completa_vr.duckdb"

# Cria uma engine do SQLAlchemy com um conjunto (pool de conexões) para o 
# DuckDB (`duckdb_engine`)
# Evita abrir conexões diretas com o duckdb_engine com conexões reutilizáveis
engine = create_engine(f"duckdb:///{duckdb_file}")

# Integração com o LangChain 
# (utilizando o modelo Claude Haiku 3.5 e a chave de api privada)
llm = ChatAnthropic(
    model="claude-3-5-haiku-latest", temperature=0, 
    anthropic_api_key=anthropic_api_key
)

# --- PROMPT 1---
decision_prompt = ChatPromptTemplate.from_messages([
    ("system",
     "Você é um assistente que interpreta pedidos de competência (mês/ano) em linguagem natural "
     "e decide se deve ou não rodar a agregação.\n\n"
     "Sua resposta deve ser SEMPRE em JSON no seguinte formato:\n"
     "{{\n"
     "  \"trigger\": true/false,  # true se o usuário quer rodar a tabela, false caso contrário\n"
     "  \"mes\": <inteiro entre 1 e 12 ou null>,\n"
     "  \"ano\": <inteiro de 4 dígitos ou null>\n"
     "}}\n\n"
     "Regras:\n"
     "1. Se o usuário não quiser rodar, retorne trigger=false e mes/ano como null.\n"
     "2. Se o usuário quiser rodar, retorne trigger=true e os números corretos de mês/ano.\n"
     "3. Não aceite 'mês atual'.\n"
     "4. Para 'mês passado' ou 'próximo mês', calcule corretamente com base na data de hoje.\n"
     "5. Interprete que o usuário quer rodar quando ele usar frases como 'gere', 'crie', etc.\n"
     "6. Retorne apenas o JSON, sem explicações extras."
    ),
    ("human", "{input}")
])

decision_chain = decision_prompt | llm | StrOutputParser()

# --- Função para invocar e interpretar a saída ---
def interpretar_decisao(texto: str) -> dict:
    raw = decision_chain.invoke({"input": texto})
    # garante que é JSON válido
    data = json.loads(raw)
    return data


# --- PROMPT 2 ---
confirm_prompt = ChatPromptTemplate.from_messages([
    ("system",
     "Você é um assistente que interpreta a resposta do usuário quando perguntado "
     "se deseja enviar a tabela agregada para o banco de dados.\n\n"
     "Sua resposta deve ser SEMPRE em JSON no formato:\n"
     "{{\n"
     "  \"confirmar\": true/false  # true se o usuário quer enviar, false caso contrário\n"
     "}}\n\n"
     "Regra: apenas analise se o usuário aceitou ou recusou, não explique nada."
    ),
    ("human", "{input}")
])

confirm_chain = confirm_prompt | llm | StrOutputParser()

def interpretar_confirmacao(texto: str) -> dict:
    raw = confirm_chain.invoke({"input": texto})
    return json.loads(raw)

# --- PROMPT 3: geração de query SQL ---
sql_prompt = ChatPromptTemplate.from_messages([
    ("system",
     "Você é um assistente que gera queries SQL para DuckDB.\n"
     "A tabela disponível no banco se chama 'tabela_compra_vr'.\n"
     "As colunas disponíveis são:\n"
     "1. Matrícula: Valor numérico único que corresponde a matrícula do colaborador.(INT)\n"
     "2. Admissão: Data de admissão do colaborador (DATE)\n"
     "3. Sindicato do Colaborador: Sindicado que representa o colaborador. (TEXT)\n"
     "4. Competência: Mês e ano de análise (TEXT no formato 'MM/YYYY')\n"
     "5. Dias: Quantidade de dias que o colaborador recebe o vale. (INT)\n"
     "6. VALOR DIÁRIO VR: Valor diário estipulado para o colaborador em reais. (FLOAT)\n"
     "7. TOTAL: Valor total do benefício para o colaborador no mês vigente. (FLOAT)\n"
     "8. Custo empresa: Valor total pago pela empresa no mês vigente. (FLOAT)\n"
     "9. Desconto profissional: Valor total pago pelo colaborador no mês vigente. (FLOAT)\n"
     "10. ESTADO_SINDICATO_SIGLA: Sigla que corresponde ao estado brasileiro em que o colaborador trabalha. (TEXT)\n"
     "11. TITULO DO CARGO: Título oficial do cargo do colaborador (TEXT)\n\n"
     "Responda SEMPRE apenas com a query SQL completa e válida.\n"
     "Nunca invente colunas fora da lista acima.\n"
     "Nunca adicione explicações fora do código SQL.\n"
     "Todas as queries devem gerar uma tabela sem as seguintes colunas:\n"
     "ESTADO_SINDICATO_SIGLA\n"
     "TITULO DO CARGO\n"
     "Uma coluna vazia deve ser gerada no final de todas, com o nome OBS GERAL"
     "Lembre-se: no **DuckDB/Postgres**, se sua coluna tem espaços ou acentos, você precisa **usar aspas duplas** para a coluna na query, lembrar que matrícula vai sempre com as aspas\n"
     "Se solicitarem a tabela de um profissional específico, não filtrar a coluna TÍTULO DO CARGO diretamente, mas ver se a coluna contém a palavra chave da profissão descrita em caixa alta\n"
     "Gere a query SQL crua, sem usar ```sql"
    ),
    ("human", "{input}")
])

sql_chain = sql_prompt | llm | StrOutputParser()

def gerar_query(texto: str) -> str:
    raw = sql_chain.invoke({"input": texto})
    return raw.strip()

# --- Execução do sistema ---
print("Bem-vindo ao sistema gerador do arquivo de compra do Vale Refeição!")
entrada = input(
    "Como deseja proceder? (ex.: 'gere o arquivo para agosto 2024', 'não quero rodar agora'):\n"
    "(obs: Evite especificar o mês de análise como 'mês atual', 'mês passado' etc. O modelo vai puxar a data mais recente de seu último treinamento).\n"
    ">"
)

decisao = interpretar_decisao(entrada)

print(f"{decisao}")

if decisao["trigger"]:
    mes, ano = decisao["mes"], decisao["ano"]
    print(f"-> Gerando tabela para competência {mes}/{ano}")
    tabela_final = read_treat_and_aggregate_data(mes, ano)

    # extrair meses/anos distintos
    admissao_meses = (
        tabela_final["DATA_DE_ADMISSAO"].dropna().dt.strftime("%m/%Y").unique()
    )
    demissao_meses = (
        tabela_final["DATA DEMISSÃO"].dropna().dt.strftime("%m/%Y").unique()
    )

    # ordenar para melhor leitura
    admissao_meses = sorted(admissao_meses)
    demissao_meses = sorted(demissao_meses)
    mes_de_análise = f"{decisao['mes']}/{decisao['ano']}"

    mensagem = f"""
        Após unir a tabela, foi observado que:

        - A coluna DATA_DE_ADMISSAO contém registros no mes/ano: {admissao_meses}
        - A coluna DATA DEMISSAO contém registros no mes/ano: {demissao_meses}
        - O mês/ano de análise é {mes_de_análise}

        Deseja proceder com o envio da tabela agregada para o banco de dados? (sim/não)
        """
    print(mensagem)

    resposta_usuario = input("> ")
    confirmacao = interpretar_confirmacao(resposta_usuario)

    if confirmacao["confirmar"]:
        print("✅ Enviando a tabela agregada para o banco de dados...")
        # Utiliza uma das conexões da pool de conexões para carregar os arquivos 
        with engine.connect() as connection:
            # Obtém a conexão DBAPI (duckdb.DuckDBPyConnection) esperada pela função
            raw_duckdb_conn = connection.connection 
            # Carrega os arquivos .csv da pasta unzipped_data no banco
            load_pandas_df_into_duckdb(raw_duckdb_conn, tabela_final)

            pergunta = input(
                "\nVocê quer a tabela completa ou quer filtrar por algum estado/cargo profissional?\n"
                ">"
            )
            query = gerar_query(pergunta)
            print(f"\n📜 Query gerada:\n{query}\n")

            try:
                df_result = pd.read_sql(query, engine)

                # Caminho para salvar
                output_path = "VR MENSAL 05.2025.xlsx"
                df_result.to_excel(output_path, index=False)

                # Abrir automaticamente o Excel no sistema
                if os.name == "nt":  # Windows
                    os.startfile(output_path)
                elif os.name == "posix":  # Linux/Mac
                    os.system(f"open {output_path}")

            except Exception as e:
                print("⚠️ Erro ao executar a query:", e)
    else:
        print("❌ Ok, não vamos enviar agora, por favor faça as correções necessárias.")

else:
    print("Ok, não vamos gerar a tabela agora. Lembre-se que é necessário informar o mês e ano de análise")