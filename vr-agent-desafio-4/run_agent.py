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
     "2. Sindicato do Colaborador: Sindicado que representa o colaborador. (TEXT)\n"
     "3. Competência: Mês e ano de análise (TEXT no formato 'MM/YYYY')\n"
     "4. Dias: Quantidade de dias que o colaborador recebe o vale. (INT)\n"
     "5. VALOR DIÁRIO VR: Valor diário estipulado para o colaborador em reais. (FLOAT)\n"
     "6. TOTAL: Valor total do benefício para o colaborador no mês vigente. (FLOAT)\n"
     "7. Custo empresa: Valor total pago pela empresa no mês vigente. (FLOAT)\n"
     "8. Desconto profissional: Valor total pago pelo colaborador no mês vigente. (FLOAT)\n"
     "9. ESTADO_SINDICATO_SIGLA: Sigla que corresponde ao estado brasileiro em que o colaborador trabalha. (TEXT)\n"
     "10. Admissão: Data de admissão do colaborador (DATE)\n"
     "11. TITULO DO CARGO: Título oficial do cargo do colaborador (TEXT)\n\n"
     "Responda SEMPRE apenas com a query SQL completa e válida.\n"
     "Nunca invente colunas fora da lista acima.\n"
     "Nunca adicione explicações fora do código SQL.\n"
     "Todas as queries devem gerar uma tabela sem as seguintes colunas:\n"
     "ESTADO_SINDICATO_SIGLA\n"
     "Admissão\n"
     "TITULO DO CARGO\n"
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
                output_path = "tabela_compra_vr.xlsx"
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































# # Cria uma ferramenta de execução de queries pelo agente do LangChain no banco
# @tool
# def execute_sql_query(query: str) -> str:
#     """Executes a SQL query against the DuckDB database via SQLAlchemy and 
#     returns the results.
#     Input should be a well-formed SQL query string.
#     Example: SELECT COUNT(*) FROM "notas_fiscais_info";
#     """
#     try:
#         # Utiliza uma das conexões da pool para executar a query 
#         with engine.connect() as connection:
#             result = connection.execute(text(query)).fetchall()
#             if not result:
#                 return "No results found for the query."
#             return str(result)
#     except Exception as e:
#         return f"Error executing SQL query: {e}"

# # Cria uma função para exibir a query executada e também a passar para o
# # modelo
# def print_query(sql_query: str) -> str:
#     """Helper function to print the generated SQL query."""
#     print(f"\nGenerated SQL Query:\n```sql\n{sql_query}\n```\n")
#     return sql_query



# # --- ENGENHARIA DE PROMPT - ENRIQUECENDO O CONTEXTO ---
# # Aqui são descritas informações da tabela, como o nome das colunas e suas 
# # respectivas descrições e um próprio descritivo da tabela. 
# db_info = SQLDatabase(engine=engine)
# table_name = "notas_fiscais_info"
# full_table_info = db_info.get_table_info(table_names=[table_name])

# schema_description = f"""Table Name: "{table_name}"
# Purpose: This table contains detailed information about electronic invoices (Notas Fiscais), including header data and individual line items. Use it to answer all questions related to invoices, their items, values, dates, and parties involved.

# Available Columns and their Descriptions:
# - "CFOP": Código Fiscal de Operações e Prestações (Tax Code for Operations and Services) - Indicates the nature of the transaction.
# - "CHAVE DE ACESSO": Chave de Acesso (Access Key) - The unique 44-digit identifier for the electronic invoice; acts as the primary key.
# - "CNPJ DESTINATÁRIO": CNPJ do Destinatário (Recipient's CNPJ) - Brazilian corporate taxpayer ID of the invoice recipient.
# - "CONSUMIDOR FINAL": Consumidor Final (Final Consumer) - Indicates if the recipient is the end consumer (Yes/No).
# - "CPF/CNPJ Emitente": CPF/CNPJ do Emitente (Issuer's CPF/CNPJ) - Brazilian individual or corporate taxpayer ID of the invoice issuer.
# - "CÓDIGO NCM/SH": Código NCM/SH (NCM/SH Code) - Mercosur Common Nomenclature / Harmonized System code for products/services.
# - "DATA EMISSÃO": Data de Emissão (Issuance Date) - The date the invoice was created. Format: YYYY-MM-DD.
# - "DATA/HORA EVENTO MAIS RECENTE": Data/Hora Evento Mais Recente (Most Recent Event Date/Time) - Timestamp of the last significant event related to the invoice (e.g., cancellation, correction).
# - "DESCRIÇÃO DO PRODUTO/SERVIÇO": Descrição do Produto/Serviço (Product/Service Description) - Detailed description of the item or service on the invoice line.
# - "DESTINO DA OPERAÇÃO": Destino da Operação (Operation Destination) - Indicates if the transaction is internal (within state), inter-state, or external.
# - "EVENTO MAIS RECENTE": Evento Mais Recente (Most Recent Event) - Describes the type of the last significant event related to the invoice.
# - "INDICADOR IE DESTINATÁRIO": Indicador IE Destinatário (Recipient's State Registration Indicator) - Shows if the recipient contributes ICMS and has a State Registration.
# - "INSCRIÇÃO ESTADUAL EMITENTE": Inscrição Estadual Emitente (Issuer's State Registration Number) - Unique tax ID within the state for ICMS purposes.
# - "MODELO": Modelo (Invoice Model) - Identifies the type of tax document (e.g., 55 for NF-e).
# - "MUNICÍPIO EMITENTE": Município Emitente (Issuer's Municipality) - The city where the invoice was issued.
# - "NATUREZA DA OPERAÇÃO": Natureza da Operação (Nature of Operation) - Descriptive text indicating the purpose of the transaction (e.g., "Venda de mercadoria").
# - "NCM/SH (TIPO DE PRODUTO)": NCM/SH (Tipo de Produto) - Another reference to the product classification code.
# - "NOME DESTINATÁRIO": Nome Destinatário (Recipient's Name) - The name of the individual or company receiving the invoice.
# - "NÚMERO": Número (Invoice Number) - The sequential fiscal number of the invoice.
# - "NÚMERO PRODUTO": Número Produto (Product Number) - An internal code or identifier for the specific product/service item.
# - "PRESENÇA DO COMPRADOR": Presença do Comprador (Buyer Presence Indicator) - Describes the buyer's presence during the transaction (e.g., in-person, internet).
# - "QUANTIDADE": Quantidade (Quantity) - The quantity of the specific product/service item sold.
# - "RAZÃO SOCIAL EMITENTE": Razão Social Emitente (Issuer's Corporate Name) - The full legal name of the company that issued the invoice.
# - "SÉRIE": Série (Invoice Series) - A sub-identifier for the invoice, allowing for multiple numbering sequences.
# - "UF DESTINATÁRIO": UF Destinatário (Recipient's State) - The Brazilian state (e.g., 'SP', 'RJ') of the recipient.
# - "UF EMITENTE": UF Emitente (Issuer's State) - The Brazilian state (e.g., 'SC', 'PR') where the invoice was issued.
# - "UNIDADE": Unidade (Unit of Measure) - The unit in which the product/service quantity is measured (e.g., "UN" for unit, "KG" for kilogram).
# - "VALOR NOTA FISCAL": Valor Nota Fiscal (Invoice Total Value) - The overall total monetary value of the entire invoice.
# - "VALOR TOTAL": Valor Total (Item Total Value) - The total monetary value for a specific line item (Quantity * Unit Value).
# - "VALOR UNITÁRIO": Valor Unitário (Unit Value) - The monetary value per unit of the specific product/service item.
# """

# table_description = (
#     "The 'notas_fiscais_info' table contains comprehensive data for electronic invoices (Notas Fiscais). "
#     "It combines information from invoice headers and individual line items. "
#     "This includes financial amounts, issuance dates, event timestamps, "
#     "customer details, and specific product descriptions along with their values. "
#     "You should use this table to answer all questions related to invoices and their items."
# )

# # Cria o prompt final estruturado para gerar queries SQL a partir de 
# # perguntas em linguagem natural, seguindo regras rígidas para consultar 
# # apenas a tabela notas_fiscais_info.
# sql_query_generation_prompt = ChatPromptTemplate.from_messages([
#     ("system",
#      "You are an expert SQL query generator for a DuckDB database. "
#      "Your task is to convert natural language questions into accurate SQL queries for the 'notas_fiscais_info' table. "
#      "Strictly adhere to the following rules:\n"
#      "1. Enclose column names with spaces or special characters in DOUBLE QUOTES (\"). Example: `\"CHAVE DE ACESSO\"`.\n"
#      "2. Use `COUNT(DISTINCT column_name)` for counting unique items.\n"
#      "3. Do NOT include any explanations or extra text, just the SQL query.\n"
#      "4. Only generate queries for the 'notas_fiscais_info' table.\n"
#      "5. Most importantly: only use the columns present in Database Schema to generate the queries and respect number 1.\n\n"
#      "##\n\n"
#      f"Database Schema:\n{schema_description}\n"
#      "##\n\n"
#      f"Table Context:\n{table_description}\n"
#     ),
#      ("human", "{question}\nSQL Query:")
# ])

# # Constrói uma sequência (RunnableSequence) que recebe uma pergunta, gera um 
# # prompt formatado, o envia para a LLM e retorna a query SQL como texto puro 
# # com o StrOutputParser.
# sql_query_generator_chain = sql_query_generation_prompt | llm | StrOutputParser()

# # --- DIRECIONAMENTO DOS RESULTADOS FINAIS (RESPOSTA FINAL ESPERADA) ---
# result_analysis_prompt = ChatPromptTemplate.from_messages([
#     ("system",
#      "You are a helpful financial data analyst. Your task is to analyze SQL query results "
#      "and explain them in clear, concise natural language in Brazilian Portuguese. "
#      "Strictly adhere to the following rules:\n"
#      "1. Do NOT include any SQL queries or code in your final answer.\n"
#      "2. Always state the answer clearly and directly in Portuguese Brazil.\n"
#      "3. For numerical answers, provide the exact number and, if applicable, specify the currency (e.g., 'R$').\n"
#      "4. If the result indicates no data, state that no information was found for the given criteria.\n"
#      "5. Consider the original question and the query result to provide a complete and relevant answer."
#     ),
#     ("human", "Original Question: {question}\nSQL Query Result: {query_result}\n\nAnalysis:")
# ])

# # Define um prompt para o modelo atuar como analista financeiro, explicando 
# # os resultados das queries SQL em português claro e sem incluir código.
# # Cria uma chain que envia essas instruções e o resultado da query ao LLM e 
# # usa um parser para extrair apenas o texto da análise final.
# # O resultado é uma explicação direta e formatada em texto do resultado da 
# # execução da query no banco.
# result_analyst_chain = result_analysis_prompt | llm | StrOutputParser()

# # --- CONSTRUINDO A SEQUÊNCIA (CHAIN) FINAL DO AGENTE UTILIZANDO LCEL (LangChain Expression Language) ---
# full_chain = (
#     RunnablePassthrough.assign(
#         # Gera a query com a LLM (1a chamada):
#         sql_query=sql_query_generator_chain
#     )
#     .assign(
#         # Isola a query gerada pelo modelo em um formato viável para execução
#         # no banco
#         sql_query_to_execute=lambda x: print_query(x["sql_query"])
#     )
#     .assign(
#         # Executa a query no banco e salva o resultado em query_result.
#         query_result=lambda x: execute_sql_query.invoke(
#             x["sql_query_to_execute"]
#         )
#     )
#     .assign(
#         # Interpreta o resultado final e traz a resposta em linguagem natural
#         # (2a chamada da LLM):
#         final_answer=result_analyst_chain
#     )
#     # Corrected: Use itemgetter to retrieve the desired key from the final dictionary output
#     | itemgetter("final_answer")
# )

# # Rodando o sistema com o script (idealizado para funcionar em linha de comando)
# print("Bem-vindo ao assistente de análise de Notas Fiscais!")
# print("Por favor, faça suas perguntas sobre os dados das notas fiscais.")

# while True:
#     pergunta = input("Faça uma pergunta (ou 'sair' para encerrar): ")
#     if pergunta.lower() in ['sair', 'exit', 'quit']:
#         # Garante que a engine seja desabalitada para derrubar todas as
#         # conexões
#         engine.dispose()
#         break
#     try:
#         response = full_chain.invoke({"question": pergunta})
#         print(f"Resposta: {response}\n")
#     except Exception as e:
#         print(f"Ocorreu um erro: {e}")
#         print("Por favor, tente novamente ou reformule sua pergunta.")