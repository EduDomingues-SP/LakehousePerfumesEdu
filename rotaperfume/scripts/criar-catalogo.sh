#!/bin/bash
# Cria o catálogo lakehouse_rotaperfume via SQL query
#
# NOTA: No Databricks Free Edition, o Default Storage está ligado, e nessa
# configuração a API do Unity Catalog RECUSA criar catálogo pela API do bundle —
# ela exige um MANAGED LOCATION que a conta gratuita não tem:
#
#   Error: Metastore storage root URL does not exist.
#          Default Storage is enabled in your account. (400 INVALID_STATE)
#
# Por isso este script usa SQL via databricks experimental aitools tools query
# em vez de criar o catálogo no bundle.

PROFILE="${1:-projeto-dados-ia}"

echo "Criando catálogo lakehouse_rotaperfume..."

databricks experimental aitools tools query \
  --query "CREATE CATALOG IF NOT EXISTS lakehouse_rotaperfume COMMENT 'Catálogo principal do projeto Rota do Perfume - Engenharia de Dados com Databricks'" \
  --profile "$PROFILE"

echo "Catálogo criado (ou já existia)."
echo "Verificando:"
databricks catalogs list --profile "$PROFILE" | grep lakehouse_rotaperfume
