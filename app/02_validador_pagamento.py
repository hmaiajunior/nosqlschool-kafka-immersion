import json
import re
from confluent_kafka import Consumer, Producer
from datetime import datetime

# Configuração Kafka
bootstrap_servers = 'localhost:9091,localhost:9092,localhost:9093'

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'validador-pagamento',
    'auto.offset.reset': 'earliest'
}

producer_config = {
    'bootstrap.servers': bootstrap_servers
}

consumer = Consumer(consumer_config)
producer = Producer(producer_config)

consumer.subscribe(['pagamento.solicitado'])

# Função de validação
def validar_pagamento(evento):
    dados = evento.get("dados_pagamento", {})
    numero_cartao = dados.get("numero_cartao", "")
    cvv = dados.get("cvv", "")
    validade = dados.get("validade", "")

    # Valida número do cartão
    cartao_valido = re.fullmatch(r"4\d{15}", numero_cartao) is not None

    # Valida CVV
    cvv_valido = re.fullmatch(r"\d{3}", cvv) is not None

    # Valida validade
    validade_valida = re.fullmatch(r"(0[1-9]|1[0-2])\/\d{2}", validade) is not None

    return cartao_valido and cvv_valido and validade_valida

# Loop de consumo
print("🔎 Iniciando validador de pagamentos...")
while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue

    try:
        evento = json.loads(msg.value().decode("utf-8"))
        pagamento_id = evento.get("pagamento_id")
        valido = validar_pagamento(evento)

        novo_topico = "pagamento.validado" if valido else "pagamento.rejeitado"
        evento["timestamp_validacao"] = datetime.utcnow().isoformat()
        evento["status_validacao"] = "valido" if valido else "invalido"

        producer.produce(
            topic=novo_topico,
            key=pagamento_id.encode("utf-8"),
            value=json.dumps(evento).encode("utf-8")
        )
        producer.flush()
        print(f"📤 Pagamento {pagamento_id} → {novo_topico}")

    except Exception as e:
        print(f"❌ Erro ao processar mensagem: {e}")
