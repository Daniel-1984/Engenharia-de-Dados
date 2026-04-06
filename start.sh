#!/bin/bash
# ATLAS — Script de inicializacao no Render
# 1. Roda o pipeline ETL para popular o banco
# 2. Sobe a API FastAPI

echo "=== ATLAS ERP Pipeline ==="
echo "Etapa 1: Rodando pipeline ETL..."
python main.py --skip-analytics

echo "Etapa 2: Subindo API FastAPI..."
uvicorn api.main:app --host 0.0.0.0 --port $PORT
