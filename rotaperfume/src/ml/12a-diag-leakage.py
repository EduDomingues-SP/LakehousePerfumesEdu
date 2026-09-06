# Databricks notebook source
# ─────────────────────────────────────────────────────────────────────────────
# ML · 12a-diag-leakage.py — Diagnóstico de Data Leakage no Modelo
# ─────────────────────────────────────────────────────────────────────────────
# Este notebook:
#   1. Treina o modelo
#   2. Calcula AUC por feature (individual)
#   3. Examina a distribuição de features por classe
#   4. Identifica combinações de features que causam separação perfeita

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")
print(f"🏗️  Diagnóstico de Leakage — Catálogo: {CATALOG}")

# COMMAND ----------
import pandas as pd
import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score, roc_curve
from sklearn.model_selection import StratifiedKFold

df_treino = spark.table(f"{CATALOG}.gold.features_treino").toPandas()
ALVO = "comprou_em_7d"
FEATURE_COLS = [c for c in df_treino.columns if c not in ("cliente_id", "_referencia", ALVO)]

print(f"\n📊 Dataset: {len(df_treino):,} linhas, {len(FEATURE_COLS)} features")
print(f"   Positivos: {int(df_treino[ALVO].sum())} ({100*df_treino[ALVO].mean():.2f}%)")

# Garantir tipo numérico
for c in FEATURE_COLS:
    df_treino[c] = pd.to_numeric(df_treino[c], errors='coerce')

X = df_treino[FEATURE_COLS].values
y = df_treino[ALVO].values

# COMMAND ----------
# 1. AUC univariado de TODAS as features
print("\n" + "=" * 60)
print("1️⃣  AUC UNIVARIADO — cada feature sozinha")
print("=" * 60)
for f in FEATURE_COLS:
    vals = df_treino[f].fillna(-999).values
    try:
        auc = roc_auc_score(y, vals)
        direction = "↑" if auc > 0.5 else "↓ (invertendo)"
        auc_clean = auc if auc >= 0.5 else 1 - auc
        print(f"  {f:30s}  AUC={auc_clean:.4f} {direction}")
    except Exception as e:
        print(f"  {f:30s}  ERRO: {e}")

# COMMAND ----------
# 2. Treinar modelo e ver AUC
print("\n" + "=" * 60)
print("2️⃣  MODELO COMPLETO — holdout AUC")
print("=" * 60)

from sklearn.model_selection import train_test_split
X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

m = HistGradientBoostingClassifier(random_state=42, max_iter=200, learning_rate=0.1, max_depth=5)
m.fit(X_tr, y_tr)
proba = m.predict_proba(X_te)[:, 1]
AUC = roc_auc_score(y_te, proba)
print(f"  AUC no holdout: {AUC:.6f}")
print(f"  Positivos no holdout: {int(y_te.sum())} de {len(y_te)}")

# COMMAND ----------
# 3. Verificar se há valores especiais (ID-like)
print("\n" + "=" * 60)
print("3️⃣  VERIFICAÇÃO DE VALORES ESPECIAIS")
print("=" * 60)

# Para cada feature, verificar se há valores únicos próximos ao número de linhas
for f in FEATURE_COLS:
    nunique = df_treino[f].nunique()
    total = len(df_treino)
    if nunique > total * 0.8:
        print(f"  ⚠️  {f}: {nunique} valores únicos em {total} linhas (muito alto!)")
    elif df_treino[f].isnull().mean() > 0.5:
        print(f"  ⚠️  {f}: {100*df_treino[f].isnull().mean():.1f}% NaN")
    else:
        print(f"  OK   {f}: {nunique} valores únicos, {100*df_treino[f].isnull().mean():.1f}% NaN")

# COMMAND ----------
# 4. Análise: separar por score do modelo
print("\n" + "=" * 60)
print("4️⃣  DISTRIBUIÇÃO DE FEATURES POR FAIXA DE SCORE")
print("=" * 60)

df_te = pd.DataFrame(X_te, columns=FEATURE_COLS)
df_te[ALVO] = y_te
df_te["proba"] = proba

for f in FEATURE_COLS:
    q_low = df_te["proba"].quantile(0.1)
    q_high = df_te["proba"].quantile(0.9)
    low = df_te[df_te["proba"] <= q_low][f]
    high = df_te[df_te["proba"] >= q_high][f]
    if len(low) > 0 and len(high) > 0:
        diff = high.mean() - low.mean()
        if abs(diff) > 1.0:  # só imprimir diferenças grandes
            print(f"  {f:30s}  Low={low.mean():.2f}  High={high.mean():.2f}  Δ={diff:+.2f}")

# COMMAND ----------
# 5. Cross-validation para ver se AUC=0.9999 é consistente
print("\n" + "=" * 60)
print("5️⃣  CROSS-VALIDATION (5 folds) — consistência do AUC")
print("=" * 60)
skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
for fold, (tr_idx, val_idx) in enumerate(skf.split(X, y)):
    clf = HistGradientBoostingClassifier(random_state=42, max_iter=200, learning_rate=0.1, max_depth=5)
    clf.fit(X[tr_idx], y[tr_idx])
    p = clf.predict_proba(X[val_idx])[:, 1]
    a = roc_auc_score(y[val_idx], p)
    print(f"  Fold {fold+1}: AUC={a:.6f}  (n_pos={int(y[val_idx].sum())})")

print("\n✅ Diagnóstico completo!")
