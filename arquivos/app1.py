import pandas as pd
import os
import subprocess

# Função para ajustar latitudes inserindo o ponto após a segunda casa decimal
def ajustar_latitude(lat):
    try:
        lat_str = str(lat).replace('.', '').replace(',', '')  # Remove qualquer ponto ou vírgula
        if len(lat_str) > 2:
            lat_str = lat_str[:2] + '.' + lat_str[2:]  # Insere o ponto após a segunda casa
        #return float(lat_str)  # Converte de volta para float
        return lat_str  # Converte de volta para float
    except (ValueError, TypeError):
        return lat  # Retorna o valor original em caso de erro

# Função para ajustar longitudes inserindo o ponto após a terceira casa decimal
def ajustar_longitude(lon):
    try:
        lon_str = str(lon).replace('.', '').replace(',', '')  # Remove qualquer ponto ou vírgula
        if len(lon_str) > 3:
            lon_str = lon_str[:3] + '.' + lon_str[3:]  # Insere o ponto após a terceira casa
        #return float(lon_str)  # Converte de volta para float
        return lon_str  # Converte de volta para float
    except (ValueError, TypeError):
        return lon  # Retorna o valor original em caso de erro

# Caminho do arquivo de entrada e saída
input_file_path = r"/SAD_Completo.xlsx"
temp_file_path = r"/Users/user/dev/Python/BuscaBairro/dados_temp_normalizado.xlsx"

# Verificar se o arquivo de entrada existe
if os.path.exists(input_file_path):
    # Carregar o arquivo original SAD_completo.xlsx
    df_ruas = pd.read_excel(input_file_path)
    print("Arquivo SAD_completo.xlsx carregado com sucesso!")

    # Salvar o arquivo temporário com os dados ajustados e normalizados
    try:
        df_ruas.to_excel(temp_file_path, index=False)
        print(f"Arquivo temporário salvo com sucesso em {temp_file_path}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo: {e}")
else:
    print(f"Arquivo de entrada não encontrado: {input_file_path}")

# Chamar o próximo script (app2.py) com subprocess
try:
    subprocess.run(['python', 'app2.py'], check=True)
except Exception as e:
    print(f"Erro ao executar app2.py: {e}")
