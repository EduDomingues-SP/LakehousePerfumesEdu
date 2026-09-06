# Databricks notebook source
# ─────────────────────────────────────────────────────────────────────────────
# BRONZE INGESTÃO — Camada Bronze: dados brutos preservados
# ─────────────────────────────────────────────────────────────────────────────
#
# Este notebook ingere os 10 CSVs do Volume como tabelas Delta na camada bronze.
#
# REGRAS DA BRONZE:
#   - Tudo como STRING. Nada de inferSchema.
#   - Adiciona _ingerido_em (timestamp) e _arquivo_origem.
#   - Valida contagem contra bronze._raw_arquivos. Se divergir, falha.
#
# CONTAGENS ESPERADAS (seed 42):
#   produtos 292 · pedidos 28.729 · itens_pedido 197.724 · pagamentos 27.772
#   estoque 8.400 · clientes 3.040 · vendedores 42 · carteira 3.637
#   oportunidades 5.979 · visitas 37.936     TOTAL: 313.551

# COMMAND ----------
# Parâmetros do pipeline
dbutils.widgets.text("catalog", "lakehouse_rotaperfume")

CATALOG = dbutils.widgets.get("catalog")

print(f"🏗️  Ingestão Bronze — Catálogo: {CATALOG}")
print(f"📍 Volume: /Volumes/{CATALOG}/bronze/raw/")
print("-" * 60)

# COMMAND ----------
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

spark = SparkSession.getActiveSession()

# COMMAND ----------
# Definição das 10 tabelas: (tabela, sistema, arquivo_csv, comentario)
# A COMMENT é aplicada em cada tabela via SQL após a escrita.
TABELAS = [
    ("produtos",      "erp", "produtos.csv",
     "Tabela de produtos do ERP — dados brutos, sem tratamento"),
    ("pedidos",       "erp", "pedidos.csv",
     "Pedidos registrados no ERP — dados brutos, sem tratamento"),
    ("itens_pedido",  "erp", "itens_pedido.csv",
     "Itens que compõe cada pedido — dados brutos, sem tratamento"),
    ("pagamentos",    "erp", "pagamentos.csv",
     "Registros de pagamento — dados brutos, sem tratamento"),
    ("estoque",       "erp", "estoque.csv",
     "Controle de estoque — dados brutos, sem tratamento"),
    ("clientes",      "crm", "clientes.csv",
     "Base de clientes do CRM — dados brutos, sem tratamento"),
    ("vendedores",    "crm", "vendedores.csv",
     "Equipe de vendas — dados brutos, sem tratamento"),
    ("carteira",      "crm", "carteira.csv",
     "Carteira de clientes por vendedor — dados brutos, sem tratamento"),
    ("oportunidades", "crm", "oportunidades.csv",
     "Oportunidades comerciais — dados brutos, sem tratamento"),
    ("visitas",       "crm", "visitas.csv",
     "Registros de visitas — dados brutos, sem tratamento"),
]

# COMMAND ----------
# Carrega registro de controle — linhas que o raw_conferencia gravou
df_raw = spark.table(f"{CATALOG}.bronze._raw_arquivos")

# Cria dict: arquivo_csv → linhas registradas (sem header)
linhas_registradas = {
    row.arquivo: row.linhas
    for row in df_raw.collect()
}

print(f"📋 Registro de controle carregado: {len(linhas_registradas)} arquivos")
for arq, lin in sorted(linhas_registradas.items()):
    print(f"   {arq:<20} → {lin:>7,} linhas (sem header)")

# COMMAND ----------
def ingestar_tabela(catalog, tabela, sistema, arquivo_csv, comentario):
    """Lê CSV como string, adiciona metadados, escreve em Delta e valida.

    Regras:
    - Lê TUDO como string (inferSchema=False).
    - CSV é CRLF com header — não usa multiLine.
    - Adiciona _ingerido_em (timestamp) e _arquivo_origem (string).
    - Valida: linhas_lidas == linhas_registradas. Se divergir, falha.
    """
    caminho = f"/Volumes/{catalog}/bronze/raw/{sistema}/{arquivo_csv}"
    destino = f"{catalog}.bronze.{tabela}"

    print(f"\n{'='*60}")
    print(f"📥 {tabela} ({arquivo_csv})")

    # ── 1. Leitura como texto — sem inferência de tipo ──────────────────────
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .load(caminho)

    # Remove a coluna _rescued_data se o Spark a criar ( artefacto do reader )
    if "_rescued_data" in df.columns:
        df = df.select([c for c in df.columns if c != "_rescued_data"])
        print(f"   ⚠️  _rescued_data removida (dados que não couberam no schema)")

    linhas_lidas = df.count()
    agora = datetime.now()

    print(f"   📄 Linhas lidas: {linhas_lidas:,}")

    # ── 2. Adiciona metadados técnicos ─────────────────────────────────────
    df_com_meta = df \
        .withColumn("_ingerido_em", lit(agora)) \
        .withColumn("_arquivo_origem", lit(arquivo_csv))

    # ── 3. Escrita em Delta (overwrite) ───────────────────────────────────
    df_com_meta.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(destino)

    # ── 4. COMMENT — de qual sistema veio ──────────────────────────────────
    spark.sql(f"COMMENT ON TABLE {destino} IS '{comentario}'")

    # ── 5. Validação contra controle ───────────────────────────────────────
    linhas_esperadas = linhas_registradas.get(arquivo_csv)

    if linhas_esperadas is None:
        raise Exception(
            f"❌ ARQUIVO NÃO REGISTRADO: {arquivo_csv} não foi encontrado em "
            f"{CATALOG}.bronze._raw_arquivos. "
            f"Execute raw_conferencia primeiro."
        )

    if linhas_lidas != linhas_esperadas:
        raise Exception(
            f"❌ CONTAGEM DIVERGE em {tabela}:\n"
            f"   Linhas lidas:     {linhas_lidas:,}\n"
            f"   Linhas esperadas: {linhas_esperadas:,}\n"
            f"   Divergência:      {abs(linhas_lidas - linhas_esperadas):,}\n"
            f"   Possível causa: multiLine=true ou separador errado"
        )

    print(f"   ✅ Validado: {linhas_lidas:,} linhas = {linhas_esperadas:,} no controle")
    print(f"   💾 Tabela: {destino}")
    print(f"   🕐 Ingerido em: {agora.isoformat()}")

    return {
        "tabela": tabela,
        "arquivo": arquivo_csv,
        "sistema": sistema,
        "linhas_lidas": linhas_lidas,
        "linhas_arquivo": linhas_esperadas,
        "bate": linhas_lidas == linhas_esperadas,
    }

# COMMAND ----------
# ── Execução: uma função, iterada sobre a lista ────────────────────────────
print("\n🚀 Iniciando ingestão das 10 tabelas bronze...\n")

resultados = []
erros = []

for tabela, sistema, arquivo, comentario in TABELAS:
    try:
        resultado = ingestar_tabela(CATALOG, tabela, sistema, arquivo, comentario)
        resultados.append(resultado)
    except Exception as e:
        erros.append({"tabela": tabela, "erro": str(e)})
        print(f"   ❌ ERRO: {e}")

# COMMAND ----------
# ── Falha rápida se alguma tabela divergiu ──────────────────────────────────
if erros:
    print("\n" + "=" * 60)
    print("🚨 INGESTÃO FALHOU — tabelas com divergência:")
    for e in erros:
        print(f"   - {e['tabela']}: {e['erro']}")
    print("=" * 60)
    raise Exception(f"{len(erros)} tabela(s) com problema. Corrija e reexecute.")

# COMMAND ----------
# ── Resumo final ───────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("📊 RESUMO DA INGESTÃO BRONZE")
print("=" * 60)

df_resultado = spark.createDataFrame(resultados)
df_resultado = df_resultado.select(
    "tabela", "sistema", "linhas_lidas", "linhas_arquivo", "bate"
)
df_resultado = df_resultado.sort("linhas_lidas", ascending=False)
df_resultado.show(truncate=False)

total_linhas = sum(r["linhas_lidas"] for r in resultados)
total_arquivo = sum(r["linhas_arquivo"] for r in resultados)

print(f"\n📦 Tabelas ingeridas:  {len(resultados)}/10")
print(f"📝 Total linhas lidas:  {total_linhas:,}")
print(f"📝 Total esperado:      {total_arquivo:,}")
print(f"✅ Bate:                {total_linhas == total_arquivo}")

print("\n" + "=" * 60)
print("✅ INGESTÃO BRONZE CONCLUÍDA COM SUCESSO!")
print("=" * 60)
