# Databricks notebook source
# ─────────────────────────────────────────────────────────────────────────────
# VERIFICAÇÃO — Bronze Feature 02
# ─────────────────────────────────────────────────────────────────────────────
# Confere se as 10 tabelas foram criadas com a contagem certa e se a sujeira
# foi preservada conforme o prompt_02 exige.

# COMMAND ----------
dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------
print("="*70)
print("1️⃣  SHOW TABLES IN bronze")
print("="*70)
spark.sql(f"SHOW TABLES IN {CATALOG}.bronze").show(truncate=False)

# COMMAND ----------
print("\n" + "="*70)
print("2️⃣  CONTAGEM vs _raw_arquivos (coluna 'bate' tem que ser true em todas)")
print("="*70)

query = f"""
WITH contagem AS (
  SELECT 'produtos'     AS tabela, COUNT(*) AS linhas FROM {CATALOG}.bronze.produtos
  UNION ALL SELECT 'pedidos',      COUNT(*) FROM {CATALOG}.bronze.pedidos
  UNION ALL SELECT 'itens_pedido', COUNT(*) FROM {CATALOG}.bronze.itens_pedido
  UNION ALL SELECT 'pagamentos',   COUNT(*) FROM {CATALOG}.bronze.pagamentos
  UNION ALL SELECT 'estoque',      COUNT(*) FROM {CATALOG}.bronze.estoque
  UNION ALL SELECT 'clientes',     COUNT(*) FROM {CATALOG}.bronze.clientes
  UNION ALL SELECT 'vendedores',   COUNT(*) FROM {CATALOG}.bronze.vendedores
  UNION ALL SELECT 'carteira',     COUNT(*) FROM {CATALOG}.bronze.carteira
  UNION ALL SELECT 'oportunidades',COUNT(*) FROM {CATALOG}.bronze.oportunidades
  UNION ALL SELECT 'visitas',      COUNT(*) FROM {CATALOG}.bronze.visitas
)
SELECT c.tabela, c.linhas AS na_tabela, r.linhas AS no_arquivo,
       c.linhas = r.linhas AS bate
FROM contagem c
JOIN {CATALOG}.bronze._raw_arquivos r ON r.arquivo = c.tabela || '.csv'
ORDER BY c.linhas DESC
"""
spark.sql(query).show(truncate=False)

# COMMAND ----------
print("\n" + "="*70)
print("3️⃣  DESCRIBE TABLE pedidos — colunas de negócio em STRING")
print("="*70)
spark.sql(f"DESCRIBE TABLE {CATALOG}.bronze.pedidos").show(truncate=False)

# COMMAND ----------
print("\n" + "="*70)
print("4️⃣  METADADOS — _arquivo_origem e _ingerido_em em itens_pedido")
print("="*70)
spark.sql(f"""
SELECT _arquivo_origem, MIN(_ingerido_em) AS ingerido_em, COUNT(*) AS linhas
FROM {CATALOG}.bronze.itens_pedido
GROUP BY _arquivo_origem
""").show(truncate=False)

# COMMAND ----------
print("\n" + "="*70)
print("5️⃣  SUJEIRA PRESERVADA — contadores de CNPJ, datas, razão social")
print("="*70)
spark.sql(f"""
SELECT
  COUNT(*)                                                          AS clientes,
  COUNT(*) FILTER (WHERE cnpj LIKE '%.%')                           AS cnpj_pontuado,
  COUNT(*) FILTER (WHERE cnpj <> trim(cnpj))                        AS cnpj_com_espaco,
  COUNT(*) FILTER (WHERE regexp_replace(trim(cnpj),'[^0-9]','') LIKE '0%') AS cnpj_zero_a_esquerda,
  COUNT(*) FILTER (WHERE data_cadastro LIKE '%/%')                  AS data_formato_br,
  COUNT(*) FILTER (WHERE razao_social = upper(razao_social))        AS razao_caixa_alta
FROM {CATALOG}.bronze.clientes
""").show(truncate=False)

# COMMAND ----------
print("\n" + "="*70)
print("6️⃣  EXEMPLO DA SUJEIRA — três formatos de CNPJ na mesma tabela")
print("="*70)
spark.sql(f"""
SELECT cliente_id, cnpj, razao_social, data_cadastro
FROM {CATALOG}.bronze.clientes
WHERE cnpj LIKE '%.%' OR cnpj <> trim(cnpj)
LIMIT 10
""").show(truncate=False)

# COMMAND ----------
print("\n" + "="*70)
print("7️⃣  PROVA: bronze não converte nada — ORDER BY texto vs CAST")
print("="*70)
print("\n7a. Os '5 maiores' pedidos com valor_total como texto (ERRADO):")
spark.sql(f"""
SELECT pedido_id, valor_total
FROM {CATALOG}.bronze.pedidos
ORDER BY valor_total DESC
LIMIT 5
""").show(truncate=False)

print("\n7b. Os 5 maiores pedidos com CAST (correto):")
spark.sql(f"""
SELECT pedido_id, CAST(valor_total AS DECIMAL(18,2)) AS valor
FROM {CATALOG}.bronze.pedidos
ORDER BY valor DESC
LIMIT 5
""").show(truncate=False)

# COMMAND ----------
print("\n" + "="*70)
print("8️⃣  TOTAL FINAL")
print("="*70)
spark.sql(f"""
SELECT SUM(linhas) AS total_bronze
FROM (
  SELECT 'produtos'      AS t, COUNT(*) AS linhas FROM {CATALOG}.bronze.produtos
  UNION ALL SELECT 'pedidos',       COUNT(*) FROM {CATALOG}.bronze.pedidos
  UNION ALL SELECT 'itens_pedido',  COUNT(*) FROM {CATALOG}.bronze.itens_pedido
  UNION ALL SELECT 'pagamentos',    COUNT(*) FROM {CATALOG}.bronze.pagamentos
  UNION ALL SELECT 'estoque',       COUNT(*) FROM {CATALOG}.bronze.estoque
  UNION ALL SELECT 'clientes',      COUNT(*) FROM {CATALOG}.bronze.clientes
  UNION ALL SELECT 'vendedores',    COUNT(*) FROM {CATALOG}.bronze.vendedores
  UNION ALL SELECT 'carteira',      COUNT(*) FROM {CATALOG}.bronze.carteira
  UNION ALL SELECT 'oportunidades', COUNT(*) FROM {CATALOG}.bronze.oportunidades
  UNION ALL SELECT 'visitas',       COUNT(*) FROM {CATALOG}.bronze.visitas
) todas
""").show(truncate=False)

print("\n✅ Verificação concluída — total esperado: 313.551")
