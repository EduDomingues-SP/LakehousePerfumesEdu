# Databricks notebook source
# ─────────────────────────────────────────────────────────────────────────────
# VERIFICAÇÃO — Schema das tabelas bronze (antes de criar silver)
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------
for tabela in ["produtos", "pedidos", "itens_pedido", "pagamentos", "estoque",
              "clientes", "vendedores", "carteira", "oportunidades", "visitas"]:
    print(f"\n{'='*60}")
    print(f"📋 {CATALOG}.bronze.{tabela}")
    print('='*60)
    spark.sql(f"DESCRIBE TABLE {CATALOG}.bronze.{tabela}").show(truncate=False)

# COMMAND ----------
# Verificar valores distintos de status e etapa
for tabela, col in [("pedidos", "status"), ("oportunidades", "etapa"),
                     ("pagamentos", "status"), ("carteira", "status")]:
    print(f"\n{col} em {tabela}:")
    spark.sql(f"SELECT DISTINCT {col} FROM {CATALOG}.bronze.{tabela} ORDER BY 1").show(truncate=False)
