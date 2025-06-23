import zipfile
from pathlib import Path


def unzip_all_files_from_data_and_export_csvs():
    """
    Descompacta todos os arquivos .zip da pasta data e exporta todos os
    arquivos para a pasta unzipped data. Mas somente caso necessário.  
    """

    # 1) Detectando caminho de leitura dos .zip's e definindo caminho de 
    # exportação dos .csv's. 
     
    # Caminho da raíz do projeto:
    abs_path = (
        Path(__file__).resolve().parent 
        if '__file__' in globals() else Path().resolve()
    )

    # Caminho dos .zip's:
    zip_path = abs_path.parent / "data"

    # Caminho para os arquivos do .zip:
    output_path = zip_path / "unzipped_data"

    # 2) Verificando se já existem arquivos .csv na pasta
    existing_csvs = list(output_path.glob("*.csv"))

    if existing_csvs:
        print(f"Arquivos CSV já existem em {output_path}, pulando descompactação.")

    else:
    # 3) Caso não hajam arquivos .csv na pasta:
    # Detectando os arquivos zip (seus caminhos) na pasta dos .zip's.
    # E descompactando esses arquivos caso existam na pasta dos .zip's.
        zip_files = list(zip_path.glob("*.zip"))
        if not zip_files:
            print("⚠️ Nenhum arquivo ZIP encontrado.")
        else:
            for zip_file in zip_files:
                print(f"🔓 Descompactando {zip_file.name}")
                unzip_file(zip_file, output_path)



def unzip_file(file_path: str, output_dir: str = "unzipped"):
    """
    Descompacta um arquivo .zip e encaminha os seus arquivos para uma pasta
    desejada.

    Args:
    file_path(str): Caminho do arquivo .zip. 
    output_dir(str): Pasta onde se deseja salvar os arquivos. 
    
    """
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)

