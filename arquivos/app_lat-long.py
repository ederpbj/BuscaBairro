import pandas as pd
import asyncio
import aiohttp
import time
import os


# Função para ajustar latitudes substituindo vírgula por ponto
def ajustar_latitude(lat):
    try:
        lat_str = str(lat).replace(',', '.')  # Substitui vírgula por ponto
        return float(lat_str)  # Converte de volta para float
    except (ValueError, TypeError):
        return lat  # Retorna o valor original em caso de erro


# Função para ajustar longitudes substituindo vírgula por ponto
def ajustar_longitude(lon):
    try:
        lon_str = str(lon).replace(',', '.')  # Substitui vírgula por ponto
        return float(lon_str)  # Converte de volta para float
    except (ValueError, TypeError):
        return lon  # Retorna o valor original em caso de erro


# Função para normalizar dados de latitude e longitude após o processamento
def normalizar_coordenadas(df):
    # Substitui vírgula por ponto nas colunas de latitude e longitude, garantindo que são strings primeiro
    df['latitude'] = df['latitude'].apply(lambda x: str(x).replace(',', '.') if pd.notna(x) else x)
    df['longitude'] = df['longitude'].apply(lambda x: str(x).replace(',', '.') if pd.notna(x) else x)

    # Tenta converter os dados de volta para float
    try:
        df['latitude'] = df['latitude'].astype(float)
        df['longitude'] = df['longitude'].astype(float)
    except ValueError as e:
        print(f"Erro ao converter coordenadas: {e}")

    return df


# Função assíncrona para buscar o nome da rua e o bairro com base em latitude e longitude
async def buscar_endereco_bairro(session, lat, lon):
    url = f'https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&addressdetails=1'
    headers = {'User-Agent': 'SeuNome/SeuEmail'}
    try:
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status == 200:
                dados = await response.json()
                if 'address' in dados:
                    endereco = dados['address']
                    rua = endereco.get('road', 'Rua não encontrada')
                    bairro = endereco.get('suburb', endereco.get('neighbourhood', 'Bairro não encontrado'))
                    return rua, bairro
            return "Rua não encontrada", "Bairro não encontrado"
    except Exception as e:
        print(f"Erro ao buscar dados: {e}")
        return "Rua não encontrada", "Bairro não encontrado"


# Função assíncrona que retorna valores padrão quando as coordenadas são inválidas
async def coordenadas_invalidas():
    return "Rua não encontrada", "Bairro não encontrado"


# Função principal para gerenciar as requisições assíncronas
async def processar_enderecos(df):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _, row in df.iterrows():
            lat = row['latitude']
            lon = row['longitude']
            if pd.notna(lat) and pd.notna(lon):
                tasks.append(buscar_endereco_bairro(session, lat, lon))
            else:
                # Adiciona a corrotina que retorna valores padrão em caso de coordenadas inválidas
                tasks.append(coordenadas_invalidas())
        results = await asyncio.gather(*tasks)
        return results


# Caminho do arquivo de entrada
input_file_path = r"/SAD_Completo.xlsx"
output_file_path = r"/Users/user/dev/Python/BuscaBairro/Dados_Completos_Bairro.xlsx"

# Verificar se o arquivo de entrada existe
if os.path.exists(input_file_path):
    # Carregar o arquivo original SAD_completo.xlsx
    df_ruas = pd.read_excel(input_file_path)
    print("Arquivo SAD_completo.xlsx carregado com sucesso!")

    # Passar os dados pelas funções ajustar_latitude e ajustar_longitude
    df_ruas['latitude'] = df_ruas['latitude'].apply(ajustar_latitude)
    df_ruas['longitude'] = df_ruas['longitude'].apply(ajustar_longitude)

    # Processar endereços assíncronamente
    start_time = time.time()
    results = asyncio.run(processar_enderecos(df_ruas))

    # Verificar se o tamanho dos resultados corresponde ao tamanho do DataFrame
    if len(results) == len(df_ruas):
        df_ruas['rua'], df_ruas['bairro'] = zip(*results)
    else:
        print(
            f"Erro: Tamanho dos resultados ({len(results)}) não corresponde ao tamanho do DataFrame ({len(df_ruas)}).")

    # Normalizar as coordenadas (substituir vírgula por ponto) após o processamento
    df_ruas = normalizar_coordenadas(df_ruas)

    # Salvar o novo DataFrame com os dados ajustados
    print(f"Processo concluído em {time.time() - start_time:.2f} segundos")
    try:
        df_ruas.to_excel(output_file_path, index=False)
        print(f"Arquivo salvo com sucesso em {output_file_path}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo: {e}")
else:
    print(f"Arquivo de entrada não encontrado: {input_file_path}")
