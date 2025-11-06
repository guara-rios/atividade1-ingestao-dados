#Atividade de Enganharia de Dados
#Aluno: Guaraci Rios

import requests
import json
import os
import pandas as pd
import time

# Caminhos das pastas
path_raw = "dataset/raw"
path_bronze = "dataset/bronze"
os.makedirs(path_raw, exist_ok=True)  #aqui é pra se certificar de que se a pasta não existir ele vai criar
os.makedirs(path_bronze, exist_ok=True)  #aqui a mesma coisa acima

# URL e Token
url = "https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data/"

headers = {"Authorization": "Token 7fdf1b2ec1f2e58876430d8813068ca95aafd4a9"}

# Lista para armazenar todos os resultados
all_results = []

# Loop para contar as paginas que vão ser salvas
page_count = 0
while url and page_count < 1000:
    response = requests.get(url, headers=headers)
    if response.status_code != 200: #aqui é pra dizer quando tiver erro e parar o codigo para não ficar dando erro pra sempre
        print(f"❌ Erro na página {page_count + 1}: {response.status_code}")
        break

    data = response.json()

    # 👉 Interrompe o loop se não houver dados
    if not data.get("results"):
        print("⚠️ Nenhum dado encontrado, encerrando...")
        break

    all_results.extend(data["results"])  # adiciona dados da página
    print(f"✅ Página {page_count + 1} baixada ({len(data['results'])} registros)")

    # 👉 Salva cada página separadamente em raw
    page_file = os.path.join(path_raw, f"gastos_diretos_p{page_count + 1}.json")
    with open(page_file, "w", encoding="utf-8") as f:
        json.dump(data["results"], f, ensure_ascii=False, indent=4)

    # próxima página
    url = data["next"]
    page_count += 1

    time.sleep(2)  # pausa curta para não sobrecarregar o servidor. baixa um arquivo a cada 2 segundos

print(f"\n📊 Total de páginas baixadas: {page_count}")
print(f"📦 Total de registros: {len(all_results)}")

# Salvar o JSON completo (raw)
raw_file = os.path.join(path_raw, "gastos_diretos.json")
with open(raw_file, "w", encoding="utf-8") as f:
    json.dump(all_results, f, ensure_ascii=False, indent=4)
print(f"✅ Dados salvos em: {raw_file}")

# Converter para DataFrame
df = pd.DataFrame(all_results)

# Adicionar colunas de ano e mês
if "data" in df.columns:
    df["ano"] = pd.to_datetime(df["data"]).dt.year
    df["mes"] = pd.to_datetime(df["data"]).dt.month

# Salvar em Parquet (bronze)
bronze_file = os.path.join(path_bronze, "gastos_diretos.parquet")
df.to_parquet(bronze_file, index=False)
print(f"✅ Arquivo Parquet salvo em: {bronze_file}")
