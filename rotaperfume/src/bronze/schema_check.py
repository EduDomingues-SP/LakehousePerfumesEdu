# Databricks notebook source
dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

tabelas = ["produtos","pedidos","itens_pedido","pagamentos","estoque",
          "clientes","vendedores","carteira","oportunidades","visitas"]

results = []
for t in tabelas:
    cols = spark.sql(f"DESCRIBE TABLE {CATALOG}.bronze.{t}").collect()
    cols_str = ",".join([c.col_name for c in cols])
    results.append((t, cols_str))

import json
out = json.dumps(results, indent=2, ensure_ascii=False)
dbutils.notebook.exit(out)
