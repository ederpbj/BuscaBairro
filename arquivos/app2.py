import pandas as pd
import asyncio
import aiohttp
import os
import logging

# Configurando o logging para melhor controle dos erros e progresso
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Função assíncrona para buscar o nome da rua e o bairro com base em latitude e longitude
async def buscar_endereco_bairro(session, lat, lon):
    url = f'https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&addressdetails=1'
    headers = {'User-Agent': 'MyApp/1.0 (myemail@example.com)'}
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
    except aiohttp.ClientError as e:
        logging.error(f"Erro de cliente ao buscar dados: {e}")
        return "Rua não encontrada", "Bairro não encontrado"
    except asyncio.TimeoutError:
        logging.error(f"Timeout ao buscar dados para lat: {lat}, lon: {lon}")
        return "Rua não encontrada", "Bairro não encontrado"
    except Exception as e:
        logging.error(f"Erro inesperado ao buscar dados: {e}")
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
                # Validação simples de coordenadas geográficas
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    tasks.append(buscar_endereco_bairro(session, lat, lon))
                else:
                    logging.warning(f"Coordenadas inválidas: lat={lat}, lon={lon}")
                    tasks.append(coordenadas_invalidas())
            else:
                tasks.append(coordenadas_invalidas())
        results = await asyncio.gather(*tasks)
        return results

# Caminho do arquivo temporário normalizado e o arquivo final
normalized_temp_file_path = r"/Users/user/dev/Python/BuscaBairro/dados_temp_normalizado.xlsx"
final_file_path = r"/Users/user/dev/Python/BuscaBairro/Dados_Completos_Bairro.xlsx"

# Verificar se o arquivo temporário normalizado existe
if os.path.exists(normalized_temp_file_path):
    # Carregar o arquivo temporário normalizado
    df_ruas = pd.read_excel(normalized_temp_file_path)
    logging.info("Arquivo temporário normalizado carregado com sucesso!")

    # Processar endereços assíncronamente
    results = asyncio.run(processar_enderecos(df_ruas))

    # Verificar se o tamanho dos resultados corresponde ao tamanho do DataFrame
    if len(results) == len(df_ruas):
        df_ruas['rua'], df_ruas['bairro'] = zip(*results)
    else:
        logging.error(f"Erro: Tamanho dos resultados ({len(results)}) não corresponde ao tamanho do DataFrame ({len(df_ruas)}).")

    # Salvar o novo DataFrame com os bairros ajustados
    try:
        df_ruas.to_excel(final_file_path, index=False)
        logging.info(f"Arquivo final salvo com sucesso em {final_file_path}")
    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo: {e}")
else:
    logging.error(f"Arquivo temporário normalizado não encontrado: {normalized_temp_file_path}")
