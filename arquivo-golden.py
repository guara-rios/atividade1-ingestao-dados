import pandas as pd
import os

# Caminhos das pastas
path_silver = "dataset/silver"
path_gold = "dataset/gold"
os.makedirs(path_gold, exist_ok=True)

# Leitura do arquivo parquet da pasta Silver
arquivo_silver = os.path.join(path_silver, "gastos_diretos_silver.parquet") #tranforma o arquivo em variavel
df = pd.read_parquet(arquivo_silver) #transforma a variavel em dataframe

print("✅ Dados lidos da camada Silver com sucesso!")
print(df.head())

# criando agregações

# Total de gastos por órgão
gastos_por_orgao = (
    df.groupby("nome_orgao")["valor"] # agrupa os valores por nome_orgao
    .sum() #faz a soma desses valores
    .reset_index() #transforma em dataframe de novo
    .sort_values(by="valor", ascending=False) #ordena os valores do maior para o menor
)

print("\n🏛️ Top 10 órgãos com maiores gastos:")
print(gastos_por_orgao.head(10))

# Total de gastos por mês e ano
gastos_por_mes = (
    df.groupby(["ano", "mes"])["valor"] #agurpa os valores por mes e ano
    .sum() # soma esses valores
    .reset_index() #faz a transformacao em dataframe novamente
    .sort_values(by=["ano", "mes"]) #ordena os valores por ordem cronologica
)

print("\n📆 Total de gastos por mês e ano:")
print(gastos_por_mes.head())

# Total de gastos por favorecido
gastos_por_favorecido = (
    df.groupby("nome_favorecido")["valor"] #agrupa os valores por nome_favorecido
    .sum() #soma esses valores
    .reset_index() #transforma de volta em dataframe
    .sort_values(by="valor", ascending=False) #ordena os valores do maior pro menor
)

print("\n💰 Top 10 favorecidos:")
print(gastos_por_favorecido.head(10))

# Salvando os resultados (camada Gold)

gastos_por_orgao.to_parquet(os.path.join(path_gold, "gastos_por_orgao.parquet"), index=False)
gastos_por_mes.to_parquet(os.path.join(path_gold, "gastos_por_mes_ano.parquet"), index=False)
gastos_por_favorecido.to_parquet(os.path.join(path_gold, "gastos_por_favorecido.parquet"), index=False)

print("\n✅ Arquivos Gold salvos com sucesso em:")
print(f"   📁 {path_gold}")