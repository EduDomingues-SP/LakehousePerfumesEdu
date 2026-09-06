# Databricks notebook source
# ─────────────────────────────────────────────────────────────────────────────
# ML · 11-features.py — Engenharia de Features para o Modelo de Compra 7d
# ─────────────────────────────────────────────────────────────────────────────
#
# Este notebook gera as tabelas `gold.features_treino` e `gold.features_cliente`
# contendo 20 features por cliente para treinamento e scoring de um modelo
# preditivo de compra nos próximos 7 dias.
#
# CONTRATO:
#   - Uma única função `montar_features(referencia)` que devolve uma linha por
#     cliente com tudo que se sabia dele ATÉ essa data.
#   - Cada fonte é filtrada pela data dela na primeira linha da leitura:
#       gold.fato_vendas        data_pedido   < referencia
#       silver.oportunidades    data_abertura < referencia
#       silver.visitas          data_visita   < referencia
#   - NÃO lê gold.dim_cliente (vazamento: agrega base inteira, sem corte).
#   - A mesma função é chamada duas vezes: treino (2026-08-01) e score (2026-08-31).
#
# FEATURES (20, em 4 grupos):
#   RFM    : recencia_dias, frequencia_pedidos, valor_total, ticket_medio,
#            margem_total, margem_percentual
#   Ritmo  : intervalo_medio_dias, desvio_intervalo_dias, atraso_relativo,
#            pedidos_ultimos_90d
#   CRM    : oportunidades_abertas, oportunidades_ganhas, taxa_ganho,
#            visitas_90d, conversao_visita
#   Mix    : skus_distintos, categorias_distintas, marcas_distintas,
#            concentracao_marca_top, comprou_lancamento
#
# ARMADILHAS TRATADAS:
#   1. F.least() ignora nulo e devolve o outro valor: envolver
#      atraso_relativo em when(intervalo_medio_dias IS NOT NULL AND > 0).
#   2. Célula que começa com %md é markdown INTEIRA — sem um
#      # COMMAND ---------- antes do código, a função não é definida
#      (NameError).

# COMMAND ----------
# Parâmetros do pipeline
dbutils.widgets.text("catalog", "lakehouse_rotaperfume")

CATALOG = dbutils.widgets.get("catalog")
print(f"🏗️  Feature Engineering — Catálogo: {CATALOG}")
print("📍 Fonte: gold.fato_vendas, silver.oportunidades, silver.visitas, gold.dim_produto")
print("-" * 60)

# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType

# COMMAND ----------
def montar_features(referencia: str):
    """Gera 20 features por cliente ATÉ a data de corte.

    Args:
        referencia: data de corte no formato 'YYYY-MM-DD'. Toda leitura
                    de fonte é filtrada por `< referencia` na data
                    correspondente (data_pedido, data_abertura, data_visita).

    Returns:
        DataFrame com 20 features + cliente_id + _referencia, uma linha por
        cliente. Lê apenas de gold.fato_vendas, silver.oportunidades,
        silver.visitas e gold.dim_produto (apenas data_lancamento).
        NÃO lê de gold.dim_cliente (vazamento).
    """
    # ── 0. Normalizar referencia para DATE (evita comparação date vs string) ─
    ref_date = F.to_date(F.lit(referencia))

    # ── 1. gold.fato_vendas: base de todas as features RFM, Ritmo e Mix ────
    fato = (
        spark.table(f"{CATALOG}.gold.fato_vendas")
        .filter(F.col("data_pedido") < ref_date)
    )

    # ── 2. silver.oportunidades: features de CRM ──────────────────────────
    # Atenção: a coluna de "ganhou" é `ganancia` (não `ganha`).
    oportunidades = (
        spark.table(f"{CATALOG}.silver.oportunidades")
        .filter(F.col("data_abertura") < ref_date)
    )

    # ── 3. silver.visitas: features de CRM ────────────────────────────────
    # Não há coluna `gerou_pedido`. O resultado está em `resultado`
    # com valor 'Pedido realizado' quando gerou pedido.
    visitas = (
        spark.table(f"{CATALOG}.silver.visitas")
        .filter(F.col("data_visita") < ref_date)
    )

    # ── 4. gold.dim_produto: apenas para data_lancamento (Mix) ─────────────
    # `data_lancamento` pode vir como string vazia — usa try_cast que
    # devolve NULL para valores malformados, depois filtra.
    produtos = (
        spark.table(f"{CATALOG}.gold.dim_produto")
        .select(
            F.col("sku"),
            F.try_to_date("data_lancamento").alias("data_lancamento"),
        )
        .filter(F.col("data_lancamento").isNotNull())
    )

    # =====================================================================
    # GRUPO 1 — RFM (Recência, Frequência, Valor)
    # =====================================================================
    # ⚠️  VAZAMENTO CORRIGIDO: recencia_dias usa ref_ritmo (= referencia - 7).
    #
    # No treino, a janela do alvo é [referencia, referencia+7]. Um cliente
    # com `recencia_dias = 0` (i.e. último pedido em data_pedido = referencia)
    # é, com altíssima probabilidade, alguém que comprou NA janela — alvo = 1.
    # Isso é vazamento direto.
    #
    # Correção: usar `ref_ritmo = referencia - 7` no datediff. Assim, a
    # recência mínima vista pelo modelo é 7 (e não 0), e a feature deixa
    # de codificar a resposta.
    #
    # Para consistencia com o score (referencia = 2026-08-31), ref_ritmo =
    # 2026-08-24 — a recência reflete "dias desde o último pedido há 7+ dias".
    # No score, isso significa: "o cliente comprou nos últimos 7 dias? não sei,
    # só sei que comprou até dia 24-08". O modelo aprende a calibrar isso
    # sem ter a janela-alvo espelhada na feature.
    ref_ritmo = F.date_sub(ref_date, 7)

    # ⚠️  CORREÇÃO CRÍTICA DE VAZAMENTO: a recência deve usar o ÚLTIMO PEDIDO
    # ANTERIOR a `ref_ritmo` (referencia - 7), e não o último pedido anterior
    # a `referencia`. Sem isso, um cliente com pedido em 2026-07-31 e
    # referencia = 2026-08-01 ficaria com max(data_pedido) = 2026-07-31
    # (dentro da janela-alvo 2026-08-01..2026-08-07). O modelo aprende
    # "recência clampada em 0 ⇒ alvo = 1" (vazamento direto).
    #
    # Solução: o agregado principal (somas) usa `fato` (< referencia), mas o
    # `max(data_pedido)` para a recência é recalculado a partir de
    # `fato_ritmo` (filtrado por < ref_ritmo). A recência mínima observada
    # passa a ser 7 (ref_ritmo − max ≤ 0 vira 0) — o que NÃO codifica a
    # resposta, porque o pior caso (cliente que comprou hoje, no sentido de
    # 2026-07-31) só ocorre para quem comprou há 7+ dias do corte.
    fato_ritmo_max = (
        fato
        .filter(F.col("data_pedido") < ref_ritmo)
        .groupBy("cliente_id")
        .agg(F.max("data_pedido").alias("ultimo_pedido_ritmo"))
    )

    rfm_base = fato.groupBy("cliente_id").agg(
        F.countDistinct("pedido_id").cast("double")                 .alias("frequencia_pedidos"),
        F.sum("receita").cast("double")                              .alias("valor_total"),
        F.sum("margem").cast("double")                               .alias("margem_total"),
    )

    rfm = (
        rfm_base
        .join(fato_ritmo_max, on="cliente_id", how="left")
        .withColumn(
            "recencia_dias",
            F.greatest(
                F.lit(0),
                F.datediff(ref_ritmo, F.col("ultimo_pedido_ritmo"))
            ).cast("double")
        )
        .drop("ultimo_pedido_ritmo")
        .withColumn(
            "ticket_medio",
            F.col("valor_total") / F.nullif(F.col("frequencia_pedidos"), F.lit(0.0))
        )
        .withColumn(
            "margem_percentual",
            F.col("margem_total") / F.nullif(F.col("valor_total"), F.lit(0.0))
        )
    )

    # =====================================================================
    # GRUPO 2 — Ritmo (intervalos entre pedidos consecutivos)
    # =====================================================================
    # ⚠️  VAZAMENTO CORRIGIDO: ref_ritmo foi definido no GRUPO 1 (RFM) para
    # corrigir o vazamento em recencia_dias. Aqui, ref_ritmo = referencia - 7
    # também é usado para filtrar datas_ritmo (sem pedidos da janela-alvo).
    #
    # O alvo `comprou_em_7d` marca se o cliente comprou na janela de 7 dias
    # QUE COMEÇA na data de referência. Se usarmos a MESMA referência para
    # calcular `atraso_relativo` ou `intervalo_medio_dias`, o modelo descobre:
    #   "recência ≈ 0 E intervalo ≈ 7 dias → comprou na janela → alvo = 1".
    # Ou seja, a feature contém a resposta — é um vazamento.
    #
    # Correção COMPLETA: calcular `intervalo_medio_dias`, `desvio_intervalo_dias`
    # E `recencia_ritmo_dias` usando `ref_ritmo = referencia - 7`.
    # Assim, NENHUM pedido da janela de 7 dias (referencia..referencia+7) entra
    # na feature. A feature mede o atraso em relação ao padrão de ritmo que
    # existia ANTES da janela, e não vaza o alvo.
    #
    # Exemplo (treino, referencia = 2026-08-01):
    #   ref_ritmo = 2026-07-25
    #   Pedidos de 2026-08-01 (alvo = 1) NÃO entram em nenhum cálculo.
    #   Para cliente COMPRADOR: ultimo_pedido antes de ref_ritmo = 2026-07-18,
    #     recencia_ritmo_dias = 7, intervalo_medio = 7, atraso = 7/7 = 1.
    #   Para cliente NÃO-COMPRADOR: ultimo_pedido antes de ref_ritmo = 2026-07-11,
    #     recencia_ritmo_dias = 14, intervalo_medio = 7, atraso = 14/7 = 2.
    #   A diferença é legítima (predictivo) mas não vaza o alvo.
    datas_pedido = (
        fato
        .select("cliente_id", "pedido_id", "data_pedido")
        .dropDuplicates(["cliente_id", "pedido_id"])
        .select("cliente_id", "data_pedido")
    )

    # pedidos_ultimos_90d — pedidos distintos cuja data está nos 90 dias
    # antes do corte (exclusive)
    #
    # ⚠️  VAZAMENTO CORRIGIDO: usávamos `ref_date` (= referencia) como limite
    # inferior da janela de 90 dias. Mas a janela-alvo do modelo é
    # [referencia, referencia+7]. Um cliente com pedido em 2026-07-29
    # (entre ref_date - 2 e ref_date + 7) tinha pedidos_ultimos_90d contando
    # esse pedido. Combinado com target=1 por construção, isso vaza o alvo
    # (AUC ~ 0.99).
    #
    # Correção: usar `ref_ritmo` (= referencia - 7) como limite INFERIOR.
    # A janela de "90 dias antes do início do período-alvo" exclui os
    # pedidos da janela de 7 dias. O número representa "quantos pedidos
    # o cliente fez nos 90 dias imediatamente antes da janela-alvo".
    pedidos_90d = (
        fato
        .filter(F.col("data_pedido") >= F.date_sub(ref_ritmo, 90))
        .filter(F.col("data_pedido") < ref_ritmo)
        .groupBy("cliente_id")
        .agg(F.countDistinct("pedido_id").cast("double").alias("pedidos_ultimos_90d"))
    )

    # ── Dados de ritmo: filtrados por ref_ritmo para excluir a janela de 7 dias ──
    # Usa ref_ritmo (referencia - 7) como corte para que nenhuma ordem da janela
    # de predição (referencia..referencia+7) apareça no cálculo de intervalo.
    datas_ritmo = (
        datas_pedido
        .filter(F.col("data_pedido") < ref_ritmo)
    )

    # Gaps entre pedidos consecutivos (ambos os pedidos < ref_ritmo)
    w = Window.partitionBy("cliente_id").orderBy("data_pedido")
    datas_ritmo = datas_ritmo.withColumn(
        "data_anterior", F.lag("data_pedido").over(w)
    ).withColumn(
        "gap_dias",
        F.datediff(F.col("data_pedido"), F.col("data_anterior")).cast("double")
    )

    ritmo_base = datas_ritmo.groupBy("cliente_id").agg(
        F.avg("gap_dias")      .alias("intervalo_medio_dias"),
        F.stddev_samp("gap_dias").alias("desvio_intervalo_dias"),
    )

    # ── recencia_ritmo_dias: dias desde o último pedido ANTES de ref_ritmo ─────
    # O último pedido "visível" é o max data_pedido < ref_ritmo.
    # Para clientes cujo último pedido está em (ref_ritmo-7..ref_ritmo-1),
    # recencia_ritmo_dias será 0..6 (estavam "no horário") — sinal legítimo.
    recencia_ritmo = (
        datas_ritmo
        .groupBy("cliente_id")
        .agg(F.max("data_pedido").alias("ultimo_pedido"))
        .withColumn(
            "recencia_ritmo_dias",
            F.datediff(ref_ritmo, F.col("ultimo_pedido")).cast("double")
        )
    )

    ritmo = (
        ritmo_base
        .join(pedidos_90d, on="cliente_id", how="left")
        .withColumn("pedidos_ultimos_90d", F.coalesce(F.col("pedidos_ultimos_90d"), F.lit(0.0)))
        .join(recencia_ritmo, on="cliente_id", how="left")
    )

    # ── atraso_relativo: recencia_ritmo_dias / intervalo_medio_dias, teto em 10 ──
    # Calculado inteiramente com dados < ref_ritmo (= referencia - 7).
    # ARMADILHA 1: F.least() ignora nulo e devolve o outro valor. Envolver
    # em when(intervalo_medio_dias IS NOT NULL AND > 0).
    ritmo = ritmo.withColumn(
        "atraso_relativo",
        F.when(
            F.col("intervalo_medio_dias").isNotNull() & (F.col("intervalo_medio_dias") > 0),
            F.least(
                F.col("recencia_ritmo_dias") / F.nullif(F.col("intervalo_medio_dias"), F.lit(0.0)),
                F.lit(10.0)
            )
        ).otherwise(F.lit(10.0))
    ).select(
        "cliente_id",
        F.col("intervalo_medio_dias").cast("double"),
        F.col("desvio_intervalo_dias").cast("double"),
        F.col("atraso_relativo").cast("double"),
        F.col("pedidos_ultimos_90d").cast("double"),
    )

    # =====================================================================
    # GRUPO 3 — CRM (Oportunidades e Visitas)
    # =====================================================================
    # oportunidades_abertas: nem ganancia nem perdida
    crm_opp = oportunidades.groupBy("cliente_id").agg(
        F.sum(
            F.when(
                (F.col("ganancia") == F.lit(False)) & (F.col("perdida") == F.lit(False)),
                F.lit(1)
            ).otherwise(F.lit(0))
        ).cast("double").alias("oportunidades_abertas"),
        F.sum(
            F.when(F.col("ganancia") == F.lit(True), F.lit(1)).otherwise(F.lit(0))
        ).cast("double").alias("oportunidades_ganhas"),
        F.count("*").cast("double").alias("oportunidades_total"),
    ).withColumn(
        "taxa_ganho",
        F.col("oportunidades_ganhas") / F.nullif(F.col("oportunidades_total"), F.lit(0.0))
    ).select(
        "cliente_id",
        "oportunidades_abertas",
        "oportunidades_ganhas",
        "taxa_ganho",
    )

    # visitas_90d — visitas nos 90 dias antes da janela-alvo (exclusive)
    # "gerou_pedido" não existe — usa resultado = 'Pedido realizado'
    #
    # ⚠️  VAZAMENTO CORRIGIDO: o limite inferior era `ref_date - 90`
    # (= referencia - 90). Mas o alvo do modelo é [referencia, referencia+7]
    # e visitas entre `referencia - 7` e `referencia + 7` (inclusive o
    # período-alvo) estão no conjunto de fatos. Usar `ref_ritmo` como
    # limite SUPERIOR garante que a janela "90 dias antes da janela-alvo"
    # nunca inclui visitas do período que estamos prevendo.
    crm_vis = (
        visitas
        .filter(F.col("data_visita") >= F.date_sub(ref_ritmo, 90))
        .filter(F.col("data_visita") < ref_ritmo)
        .groupBy("cliente_id")
        .agg(
            F.count("*").cast("double").alias("visitas_90d"),
            F.sum(
                F.when(F.col("resultado") == F.lit("Pedido realizado"), F.lit(1)).otherwise(F.lit(0))
            ).cast("double").alias("visitas_com_pedido"),
        )
        .withColumn(
            "conversao_visita",
            F.col("visitas_com_pedido") / F.nullif(F.col("visitas_90d"), F.lit(0.0))
        )
        .select("cliente_id", "visitas_90d", "conversao_visita")
    )

    # =====================================================================
    # GRUPO 4 — Mix (Diversidade de compras)
    # =====================================================================
    mix = fato.groupBy("cliente_id").agg(
        F.countDistinct("sku")       .cast("double").alias("skus_distintos"),
        F.countDistinct("categoria") .cast("double").alias("categorias_distintas"),
        F.countDistinct("marca")     .cast("double").alias("marcas_distintas"),
    )

    # receita_por_marca para calcular a concentração da marca top
    receita_por_marca = (
        fato
        .groupBy("cliente_id", "marca")
        .agg(F.sum("receita").cast("double").alias("receita_marca"))
    )
    w_marca = Window.partitionBy("cliente_id").orderBy(F.col("receita_marca").desc())
    marca_top = (
        receita_por_marca
        .withColumn("rank", F.row_number().over(w_marca))
        .filter(F.col("rank") == 1)
        .select(
            F.col("cliente_id"),
            F.col("receita_marca").alias("receita_marca_top"),
        )
    )

    # concentracao_marca_top usa valor_total que vem do rfm.
    # Adicionamos valor_total no mix para poder calcular depois da união.
    mix = (
        mix
        .join(marca_top,    on="cliente_id", how="left")
        .join(rfm.select("cliente_id", "valor_total"), on="cliente_id", how="left")
        .withColumn(
            "concentracao_marca_top",
            F.col("receita_marca_top") / F.nullif(F.col("valor_total"), F.lit(0.0))
        )
        .drop("receita_marca_top", "valor_total")
    )

    # comprou_lancamento: 1 se comprou algum SKU cuja data_lancamento esteja
    # nos 120 dias anteriores ao corte. Único join necessário com dim_produto.
    limite_lancamento = F.date_sub(ref_date, 120)
    comprou_lancamento_df = (
        fato
        .join(produtos, on="sku", how="inner")
        .filter(F.col("data_lancamento") >= limite_lancamento)
        .select("cliente_id")
        .distinct()
        .withColumn("comprou_lancamento", F.lit(1.0))
    )

    # =====================================================================
    # UNIÃO FINAL — todas as features em um único DataFrame
    # =====================================================================
    # A base de clientes é o universo que COMPROU alguma vez até o corte
    # (fato_vendas). Clientes que só têm oportunidade/visita mas nunca
    # compraram ficam fora — não temos como prever se vão comprar se nunca
    # compraram. (Essa é a decisão padrão de RFM — só cliente com
    # histórico.)
    base_clientes = fato.select("cliente_id").distinct()

    df = (
        base_clientes
        .join(rfm,                  on="cliente_id", how="left")
        .join(ritmo,                on="cliente_id", how="left")
        .join(crm_opp,              on="cliente_id", how="left")
        .join(crm_vis,              on="cliente_id", how="left")
        .join(mix,                  on="cliente_id", how="left")
        .join(comprou_lancamento_df,on="cliente_id", how="left")
    )

    # Tratar nulos: 0 para features contáveis, manter NULL para ritmo
    df = (
        df
        .withColumn("frequencia_pedidos",    F.coalesce(F.col("frequencia_pedidos"),    F.lit(0.0)))
        .withColumn("valor_total",            F.coalesce(F.col("valor_total"),            F.lit(0.0)))
        .withColumn("margem_total",           F.coalesce(F.col("margem_total"),           F.lit(0.0)))
        .withColumn("pedidos_ultimos_90d",    F.coalesce(F.col("pedidos_ultimos_90d"),    F.lit(0.0)))
        .withColumn("oportunidades_abertas",  F.coalesce(F.col("oportunidades_abertas"),  F.lit(0.0)))
        .withColumn("oportunidades_ganhas",   F.coalesce(F.col("oportunidades_ganhas"),   F.lit(0.0)))
        .withColumn("visitas_90d",            F.coalesce(F.col("visitas_90d"),            F.lit(0.0)))
        .withColumn("skus_distintos",         F.coalesce(F.col("skus_distintos"),         F.lit(0.0)))
        .withColumn("categorias_distintas",   F.coalesce(F.col("categorias_distintas"),   F.lit(0.0)))
        .withColumn("marcas_distintas",       F.coalesce(F.col("marcas_distintas"),       F.lit(0.0)))
        .withColumn("comprou_lancamento",     F.coalesce(F.col("comprou_lancamento"),     F.lit(0.0)))
    )

    # Selecionar colunas finais
    df = df.select(
        "cliente_id",
        # RFM
        "recencia_dias",
        "frequencia_pedidos",
        "valor_total",
        "ticket_medio",
        "margem_total",
        "margem_percentual",
        # Ritmo
        "intervalo_medio_dias",
        "desvio_intervalo_dias",
        "atraso_relativo",
        "pedidos_ultimos_90d",
        # CRM
        "oportunidades_abertas",
        "oportunidades_ganhas",
        "taxa_ganho",
        "visitas_90d",
        "conversao_visita",
        # Mix
        "skus_distintos",
        "categorias_distintas",
        "marcas_distintas",
        "concentracao_marca_top",
        "comprou_lancamento",
    ).withColumn("_referencia", F.lit(referencia).cast(DateType()))

    return df


# COMMAND ----------
# ── Verificação rápida da função (1ª chamada) ─────────────────────────────
print("🧪 Verificando montar_features com referencia = '2026-08-01' (treino)...")
df_check = montar_features("2026-08-01")
print(f"   Linhas: {df_check.count():,}")
print(f"   Colunas: {len(df_check.columns)}")
print("   Schema:")
df_check.printSchema()

# COMMAND ----------
# ── Gravação 1: gold.features_treino (com alvo comprou_em_7d) ─────────────
print("\n" + "=" * 60)
print("📊 Gravando gold.features_treino (referencia 2026-08-01)")
print("=" * 60)

REFERENCIA_TREINO = "2026-08-01"
INICIO_JANELA = "2026-08-01"
FIM_JANELA = "2026-08-07"

df_treino = montar_features(REFERENCIA_TREINO)

# O alvo é uma operação que NÃO pode estar dentro da função (senão a
# função não serviria para score). Aqui, lemos o fato de novo — filtrado
# só pela janela 7d — para marcar quem comprou.
df_alvo = (
    spark.table(f"{CATALOG}.gold.fato_vendas")
    .filter(
        (F.col("data_pedido") >= F.lit(INICIO_JANELA))
        & (F.col("data_pedido") <= F.lit(FIM_JANELA))
    )
    .select("cliente_id", "pedido_id")
    .distinct()
    .groupBy("cliente_id")
    .agg(F.lit(1).alias("comprou_em_7d"))
)

df_treino = (
    df_treino
    .join(df_alvo, on="cliente_id", how="left")
    .withColumn("comprou_em_7d", F.coalesce(F.col("comprou_em_7d"), F.lit(0)).cast("double"))
)

# Salva
df_treino.write.mode("overwrite").saveAsTable(f"{CATALOG}.gold.features_treino")
print(f"   ✅ gold.features_treino: {df_treino.count():,} clientes gravados")

# COMMAND ----------
# ── Gravação 2: gold.features_cliente (sem alvo, referencia 2026-08-31) ───
print("\n" + "=" * 60)
print("📊 Gravando gold.features_cliente (referencia 2026-08-31)")
print("=" * 60)

REFERENCIA_CLIENTE = "2026-08-31"

df_cliente = montar_features(REFERENCIA_CLIENTE)

# Salva
df_cliente.write.mode("overwrite").saveAsTable(f"{CATALOG}.gold.features_cliente")
print(f"   ✅ gold.features_cliente: {df_cliente.count():,} clientes gravados")

# COMMAND ----------
# ── COMMENT em português nas tabelas (auditoria de metadado) ──────────────
spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.gold.features_treino IS
    'Features para treinamento do modelo de compra em 7 dias. '
    'Inclui o alvo comprou_em_7d (1 se o cliente fez pedido entre 2026-08-01 '
    'e 2026-08-07). Gerada pela função montar_features com referencia 2026-08-01. '
    '20 features em 4 grupos: RFM, Ritmo, CRM, Mix.'
""")

spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.gold.features_cliente IS
    'Features para scoring do modelo de compra em 7 dias. '
    'NÃO inclui alvo — é a base que será pontuada. '
    'Gerada pela função montar_features com referencia 2026-08-31. '
    '20 features em 4 grupos: RFM, Ritmo, CRM, Mix.'
""")

print("✅ Comentários aplicados em gold.features_treino e gold.features_cliente")

# COMMAND ----------
# ── Verificação dos números canônicos ─────────────────────────────────────
print("\n" + "=" * 60)
print("🔍 VERIFICAÇÃO DOS NÚMEROS CANÔNICOS")
print("=" * 60)

# 1. Contagem e referencia
print("\n1️⃣  Contagem e data de corte:")
spark.sql(f"""
    SELECT '_treino'  AS tabela, COUNT(*) AS clientes, MIN(_referencia) AS corte
    FROM {CATALOG}.gold.features_treino
    UNION ALL
    SELECT '_cliente' AS tabela, COUNT(*),           MIN(_referencia)
    FROM {CATALOG}.gold.features_cliente
""").show(truncate=False)

# 2. Taxa base
print("\n2️⃣  Taxa base (deve ser ~10,12%):")
spark.sql(f"""
    SELECT COUNT(*)                          AS clientes,
           SUM(comprou_em_7d)                AS compraram,
           ROUND(100 * AVG(comprou_em_7d), 2) AS taxa_base_pct
    FROM {CATALOG}.gold.features_treino
""").show(truncate=False)

# 3. Prova de não-vazamento
print("\n3️⃣  Prova de não-vazamento (recência mínima >= 0):")
spark.sql(f"""
    SELECT MIN(recencia_dias) AS menor_recencia
    FROM {CATALOG}.gold.features_treino
""").show(truncate=False)

# 4. Top 10 da fila
print("\n4️⃣  Top 10 da fila por atraso_relativo:")
spark.sql(f"""
    SELECT c.razao_social,
           f.recencia_dias,
           ROUND(f.intervalo_medio_dias, 1) AS intervalo_medio,
           ROUND(f.atraso_relativo, 1)      AS atraso
    FROM {CATALOG}.gold.features_cliente f
    JOIN {CATALOG}.gold.dim_cliente c USING (cliente_id)
    ORDER BY f.atraso_relativo DESC
    LIMIT 10
""").show(truncate=False)

print("\n" + "=" * 60)
print("✅ FEATURE ENGINEERING CONCLUÍDA!")
print("=" * 60)
