# Databricks notebook source
# Apenas listar schemas — saída vai para o log do job

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

out = {}
for t in ["produtos","pedidos","itens_pedido","pagamentos","estoque",
          "clientes","vendedores","carteira","oportunidades","visitas"]:
    cols = spark.sql(f"DESCRIBE TABLE {CATALOG}.bronze.{t}").collect()
    out[t] = [{"name": c.col_name, "type": c.data_type} for c in cols]

# Imprime cada tabela (vai para notebook_output)
for t, cols in out.items():
    print(f"\n=== {t} ===")
    for c in cols:
        print(f"  {c['name']:<25} {c['type']}")

# Valores distintos que importam
print("\n\n=== DISTINCT VALUES ===")
for t, c in [("pedidos","status"),("oportunidades","etapa"),("pagamentos","status")]:
    try:
        print(f"\n{t}.{c}:")
        rows = spark.sql(f"SELECT DISTINCT {c} FROM {CATALOG}.bronze.{t}").collect()
        for r in rows:
            print(f"  {r[0]}")
    except Exception as e:
        print(f"  ERRO: {e}")
