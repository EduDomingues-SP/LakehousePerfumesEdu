# Databricks notebook source
# ─────────────────────────────────────────────────────────────
# CONFERÊNCIA DE CHEGADA - Raw Archive Verification
# ─────────────────────────────────────────────────────────────
# Esta tarefa verifica se todos os arquivos esperados chegaram
# ao Volume do Unity Catalog e registra o controle de qualidade.

# COMMAND ----------
# Parâmetros do pipeline
dbutils.widgets.text("catalog", "lakehouse_rotaperfume")

CATALOG = dbutils.widgets.get("catalog")

print(f"🔍 Conferindo dados do catálogo: {CATALOG}")
print(f"📍 Volume: dbfs:/Volumes/{CATALOG}/bronze/raw/")
print("-" * 60)

# COMMAND ----------
import os
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

# Arquivos esperados por sistema
ARQUIVOS_ESPERADOS = {
    "erp": ["produtos.csv", "pedidos.csv", "itens_pedido.csv", "pagamentos.csv", "estoque.csv"],
    "crm": ["clientes.csv", "vendedores.csv", "carteira.csv", "oportunidades.csv", "visitas.csv"]
}

# COMMAND ----------
def get_file_info(path):
    """Retorna (exists, size_bytes, num_lines) para um arquivo."""
    try:
        # Tenta ler via Spark (para arquivos no Volume/DBFS)
        df = spark.read.format("csv").option("header", "true").load(path)
        num_lines = df.count()

        # Obtém tamanho via dbutils
        file_info = dbutils.fs.ls(path)
        size_bytes = file_info[0].size if file_info else 0

        return True, size_bytes, num_lines
    except Exception as e:
        return False, 0, 0

# COMMAND ----------
def verificar_sistema(sistema, volume_path):
    """Verifica todos os arquivos de um sistema."""
    resultados = []

    for arquivo in ARQUIVOS_ESPERADOS[sistema]:
        path = f"{volume_path}/{sistema}/{arquivo}"

        print(f"  📄 {arquivo}...", end=" ")

        exists, size_bytes, num_lines = get_file_info(path)

        if exists and num_lines > 0:
            print(f"✅ {num_lines:,} linhas ({size_bytes:,} bytes)")
            resultados.append({
                "sistema": sistema,
                "arquivo": arquivo,
                "bytes": size_bytes,
                "linhas": num_lines,
                "status": "OK"
            })
        elif exists and num_lines == 0:
            print(f"⚠️  VAZIO (0 linhas)")
            resultados.append({
                "sistema": sistema,
                "arquivo": arquivo,
                "bytes": size_bytes,
                "linhas": 0,
                "status": "VAZIO"
            })
        else:
            print(f"❌ NÃO ENCONTRADO")
            resultados.append({
                "sistema": sistema,
                "arquivo": arquivo,
                "bytes": 0,
                "linhas": 0,
                "status": "FALTA"
            })

    return resultados

# COMMAND ----------
# Execução da conferência
print("\n📂 Iniciando conferência de chegada...\n")

VOLUME_PATH = f"dbfs:/Volumes/{CATALOG}/bronze/raw"
resultados = []

for sistema in ["erp", "crm"]:
    print(f"\n🏢 Sistema {sistema.upper()}:")
    resultados.extend(verificar_sistema(sistema, VOLUME_PATH))

# COMMAND ----------
# Verifica se há falhas
falhas = [r for r in resultados if r["status"] != "OK"]

if falhas:
    print("\n" + "=" * 60)
    print("🚨 ERRO: Arquivos faltando ou vazios!")
    print("=" * 60)
    for f in falhas:
        print(f"  - {f['sistema']}/{f['arquivo']}: {f['status']}")
    print("\nPipeline INTERROMPIDO até que todos os arquivos cheguem.")

    # Levanta exceção para falhar o job
    raise Exception(f"Conferência de chegada FALHOU: {len(falhas)} arquivo(s) com problema(s).")

# COMMAND ----------
# Registra controle no Unity Catalog
print("\n📝 Registrando controle no Unity Catalog...")

conferido_em = datetime.now().isoformat()

# Converte resultados para DataFrame
dados_controle = []
for r in resultados:
    dados_controle.append({
        "sistema": r["sistema"],
        "arquivo": r["arquivo"],
        "bytes": r["bytes"],
        "linhas": r["linhas"],
        "conferido_em": conferido_em
    })

df_controle = spark.createDataFrame(dados_controle)

# Salva no Unity Catalog (cria a tabela se não existir)
df_controle.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.bronze._raw_arquivos")

print(f"✅ Tabela de controle atualizada: {CATALOG}.bronze._raw_arquivos")

# COMMAND ----------
# Resumo final
print("\n" + "=" * 60)
print("📊 RESUMO DA CONFERÊNCIA DE CHEGADA")
print("=" * 60)

total_arquivos = len(resultados)
total_linhas = sum(r["linhas"] for r in resultados)
total_bytes = sum(r["bytes"] for r in resultados)

print(f"\n📦 Arquivos conferidos: {total_arquivos}")
print(f"📝 Total de linhas de dado: {total_linhas:,}")
print(f"💾 Tamanho total: {total_bytes / 1024 / 1024:.1f} MB")
print(f"🕐 Conferido em: {conferido_em}")

# COMMAND ----------
# Tabela legível
print("\n📋 Detalhamento por arquivo:\n")
print(f"{'Sistema':<6} {'Arquivo':<20} {'Linhas':>12} {'Tamanho':>12}")
print("-" * 55)

df_resumo = spark.createDataFrame(resultados)
df_resumo.sort("linhas", ascending=False).show(truncate=False)

# COMMAND ----------
print("\n✅ CONFERÊNCIA DE CHEGADA CONCLUÍDA COM SUCESSO!")
print("=" * 60)
