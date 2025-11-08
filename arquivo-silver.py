import pandas as pd
import os

# Caminhos das pastas
path_bronze = "dataset/bronze"
path_silver = "dataset/silver"
os.makedirs(path_silver, exist_ok=True) #aqui é pra se certificar de que se a pasta não existir ele vai criar

# Leitura do arquivo parquet da pasta Bronze
arquivo_bronze = os.path.join(path_bronze, "gastos_diretos.parquet") #transformando o arquivo parquet em variável
df = pd.read_parquet(arquivo_bronze) #transformando a variável em dataframe

print(df.columns)  # mostra os nomes das colunas do arquivo parquet

# limpando valores nulos
df = df.dropna(subset=["data_pagamento", "nome_orgao", "valor"])
if "nome_favorecido" in df.columns:
    df["nome_favorecido"] = df["nome_favorecido"].fillna("NÃO INFORMADO")

# padronizando os textos
if "nome_orgao" in df.columns:
    df["nome_orgao"] = df["nome_orgao"].str.strip().str.title()
if "nome_favorecido" in df.columns:
    df["nome_favorecido"] = df["nome_favorecido"].fillna("NÃO INFORMADO")
    df["nome_favorecido"] = df["nome_favorecido"].str.strip().str.upper()

# convertendo os tipos
df["data_pagamento"] = pd.to_datetime(df["data_pagamento"], errors="coerce") # converte em formato de data
df["valor"] = pd.to_numeric(df["valor"], errors="coerce") # aqui converte em formato de numero

# colocando colunas derivadas
if "data_pagamento" in df.columns:
    df["ano"] = pd.to_datetime(df["data_pagamento"]).dt.year #cria a coluna ano
    df["mes"] = pd.to_datetime(df["data_pagamento"]).dt.month #cria a coluna mes

# verificando se os dados não tem valor nulo
if df["valor"].isnull().any():
    print("⚠️ Existem valores nulos na coluna 'valor'!")
if (df["valor"] < 0).any():
    print("⚠️ Existem valores negativos na coluna 'valor'!")

# analisando os dados
print("\n📊 Estatísticas básicas do valor gasto:")
print(df["valor"].describe())

print("\n🏢 Top 5 órgãos com mais gastos:")
print(df.groupby("nome_orgao")["valor"].sum().sort_values(ascending=False).head())

# caminho de salvar os resultados
path_silver = "dataset/silver"
os.makedirs(path_silver, exist_ok=True)

# salva em arquivo parquet
silver_file = os.path.join(path_silver, "gastos_diretos_silver.parquet")
df.to_parquet(silver_file, index=False)
print(f"✅ Arquivo Silver salvo em: {silver_file}")