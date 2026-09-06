# Databricks notebook source
# Lê os CSVs diretamente do Volume para ver os nomes de colunas exatos
CATALOG = "lakehouse_rotaperfume"

for sistema, arquivo in [
    ("erp", "produtos.csv"),
    ("erp", "pedidos.csv"),
    ("erp", "itens_pedido.csv"),
    ("erp", "pagamentos.csv"),
    ("erp", "estoque.csv"),
    ("crm", "clientes.csv"),
    ("crm", "vendedores.csv"),
    ("crm", "carteira.csv"),
    ("crm", "oportunidades.csv"),
    ("crm", "visitas.csv"),
]:
    path = f"/Volumes/{CATALOG}/bronze/raw/{sistema}/{arquivo}"
    # Lê só o header (1 linha)
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "false").load(path)
    cols = df.columns
    print(f"{arquivo}: {cols}")
