#!/bin/bash
# Sobe os CSVs de dados/erp e dados/crm para o Volume Unity Catalog.
#
# O comando databricks fs cp exige o esquema "dbfs:" no destino, mesmo sendo
# um Volume do UC.

set -e

PROFILE="${1:-projeto-dados-ia}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DADOS_DIR="$REPO_ROOT/dados"
CATALOG="${CATALOG:-lakehouse_rotaperfume}"

# Verifica se dados/ existe. Se não, gera o dataset.
if [ ! -d "$DADOS_DIR" ]; then
  echo "dados/ não existe. Gerando dataset..."
  if [ -f "$REPO_ROOT/material/gerar_dataset.py" ]; then
    python3 "$REPO_ROOT/material/gerar_dataset.py" --saida "$DADOS_DIR" --seed 42
  else
    echo "ERRO: $DADOS_DIR não existe e gerar_dataset.py não foi encontrado."
    exit 1
  fi
fi

echo "Subindo dados do ERP para o Volume..."
databricks fs cp --recursive --overwrite \
  "$DADOS_DIR/erp" "dbfs:/Volumes/${CATALOG}/bronze/raw/erp" \
  --profile "$PROFILE"

echo "Subindo dados do CRM para o Volume..."
databricks fs cp --recursive --overwrite \
  "$DADOS_DIR/crm" "dbfs:/Volumes/${CATALOG}/bronze/raw/crm" \
  --profile "$PROFILE"

echo "Upload concluído."
echo "Listando arquivos no Volume:"
databricks fs ls "dbfs:/Volumes/${CATALOG}/bronze/raw/erp" --profile "$PROFILE"
databricks fs ls "dbfs:/Volumes/${CATALOG}/bronze/raw/crm" --profile "$PROFILE"
