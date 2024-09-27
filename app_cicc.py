import pandas as pd
import asyncio
import aiohttp
import time
import os


# Função para ajustar latitudes inserindo o ponto após a segunda casa decimal
def ajustar_latitude(lat):
    try:
        # Verifica se é uma string e limpa possíveis valores como 'na.n'
        if isinstance(lat, str) and (lat.lower() in ['na.n', 'nan', '', None]):
            raise ValueError("Valor inválido de latitude: 'na.n'")

        lat_str = str(lat).replace('.', '').replace(',', '')  # Remove qualquer ponto ou vírgula
        if len(lat_str) > 2:
            lat_str = lat_str[:2] + '.' + lat_str[2:]  # Insere o ponto após a segunda casa
            latitude = converter_latitude(lat_str)
        else:
            raise ValueError("Latitude muito curta para ajustar.")
        return latitude if latitude is not None else lat  # Retorna o valor original em caso de falha na conversão
    except (ValueError, TypeError) as e:
        print(f"Erro ao ajustar latitude: {e}")
        return None  # Retorna None se houver erro na conversão


# Função para ajustar longitudes inserindo o ponto após a terceira casa decimal
def ajustar_longitude(lon):
    try:
        # Verifica se é uma string e limpa possíveis valores como 'na.n'
        if isinstance(lon, str) and (lon.lower() in ['na.n', 'nan', '', None]):
            raise ValueError("Valor inválido de longitude: 'na.n'")

        lon_str = str(lon).replace('.', '').replace(',', '')  # Remove qualquer ponto ou vírgula
        if len(lon_str) > 3:
            lon_str = lon_str[:3] + '.' + lon_str[3:]  # Insere o ponto após a terceira casa
            longitude = converter_longitude(lon_str)
        else:
            raise ValueError("Longitude muito curta para ajustar.")
        return longitude if longitude is not None else lon  # Retorna o valor original em caso de falha na conversão
    except (ValueError, TypeError) as e:
        print(f"Erro ao ajustar longitude: {e}")
        return None  # Retorna None se houver erro na conversão


# Funções de conversão para latitudes e longitudes
def converter_latitude(valor_str):
    try:
        valor_str = valor_str.strip().replace(',', '.')  # Limpar string
        lat = float(valor_str)  # Tentar converter para float
        if -90 <= lat <= 90:
            return lat
        else:
            raise ValueError(f"Valor de latitude fora do intervalo: {lat}")
    except ValueError as e:
        print(f"Erro de conversão para latitude: {e}")
        return None


def converter_longitude(valor_str):
    try:
        valor_str = valor_str.strip().replace(',', '.')  # Limpar string
        lon = float(valor_str)  # Tentar converter para float
        if -180 <= lon <= 180:
            return lon
        else:
            raise ValueError(f"Valor de longitude fora do intervalo: {lon}")
    except ValueError as e:
        print(f"Erro de conversão para longitude: {e}")
        return None


# Função assíncrona para buscar o nome da rua e o bairro com base em latitude e longitude
async def buscar_endereco_bairro(session, lat, lon):
    url = f'https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&addressdetails=1'
    headers = {'User-Agent': 'MyApp/1.0 (meuemail@example.com)'}

    try:
        async with session.get(url, headers=headers, timeout=30) as response:  # Aumentei o timeout para 20 segundos
            if response.status == 200:
                dados = await response.json()
                if 'address' in dados:
                    endereco = dados['address']
                    rua = endereco.get('road', 'Rua não encontrada')
                    bairro = endereco.get('suburb', endereco.get('neighbourhood', 'Bairro não encontrado'))
                    return rua, bairro
            return "Rua não encontrada", "Bairro não encontrado"
    except aiohttp.ClientError as e:
        print(f"Erro de conexão ao buscar dados para lat={lat}, lon={lon}: {e}")
        return "Rua não encontrada", "Bairro não encontrado"
    except asyncio.TimeoutError:
        print(f"Timeout ao buscar dados para lat={lat}, lon={lon}")
        return "Rua não encontrada", "Bairro não encontrado"
    except Exception as e:
        print(f"Erro inesperado ao buscar dados para lat={lat}, lon={lon}: {e}")
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
                tasks.append(coordenadas_invalidas())  # Coordenadas inválidas retornam um valor padrão
        results = await asyncio.gather(*tasks)
        return results


# Caminho do arquivo de entrada
input_file_path = r"/Users/user/dev/Python/BuscaBairro/SAD_Completo.xlsx"
output_file_path = r"/Users/user/dev/Python/BuscaBairro/Dados_Completos_Bairro.xlsx"

# Verificar se o arquivo de entrada existe
if os.path.exists(input_file_path):
    df_ruas = pd.read_excel(input_file_path)
    print("Arquivo SAD_completo.xlsx carregado com sucesso!")

    # Remover entradas inválidas antes de ajustar latitudes e longitudes
    df_ruas = df_ruas.replace({'latitude': ['na.n', 'nan', None, ''], 'longitude': ['na.n', 'nan', None, '']}, None)

    # Ajustar latitude e longitude
    df_ruas['latitude'] = df_ruas['latitude'].apply(ajustar_latitude)
    df_ruas['longitude'] = df_ruas['longitude'].apply(ajustar_longitude)

    # Remover entradas com latitudes ou longitudes inválidas após a conversão
    df_ruas = df_ruas.dropna(subset=['latitude', 'longitude'])

    # Processar endereços assíncronamente
    start_time = time.time()
    results = asyncio.run(processar_enderecos(df_ruas))

    if len(results) == len(df_ruas):
        df_ruas['rua'], df_ruas['bairro'] = zip(*results)
    else:
        print(
            f"Erro: Tamanho dos resultados ({len(results)}) não corresponde ao tamanho do DataFrame ({len(df_ruas)}).")

    print(f"Processo concluído em {time.time() - start_time:.2f} segundos")

    try:
        df_ruas.to_excel(output_file_path, index=False)
        print(f"Arquivo salvo com sucesso em {output_file_path}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo: {e}")
else:
    print(f"Arquivo de entrada não encontrado: {input_file_path}")
