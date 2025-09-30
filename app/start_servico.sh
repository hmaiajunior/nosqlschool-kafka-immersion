#!/bin/bash

# Nome do serviço (ex: api_pagamento.py)
SERVICO=$1

# Cria ambiente virtual se não existir
if [ ! -d "venv" ]; then
  echo "🔧 Criando ambiente virtual..."
  python3 -m venv venv
fi

# Ativa o ambiente virtual
source venv/bin/activate

# Instala dependências
pip install -r requirements.txt

# Executa o serviço
echo "🚀 Iniciando serviço: $SERVICO"
python $SERVICO
