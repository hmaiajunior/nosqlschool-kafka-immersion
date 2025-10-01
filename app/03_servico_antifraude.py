import json
from confluent_kafka import Consumer, Producer
from datetime import datetime

# Configuração Kafka
bootstrap_servers = 'localhost:9091,localhost:9092,localhost:9093'

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'servico-antifraude',
    'auto.offset.reset': 'earliest'
}

producer_config = {
    'bootstrap.servers': bootstrap_servers
}

consumer = Consumer(consumer_config)
producer = Producer(producer_config)

consumer.subscribe(['pagamento.validado'])

# Função de avaliação de risco
def avaliar_risco(evento):
    valor = evento.get("valor", 0)
    numero_cartao = evento.get("dados_pagamento", {}).get("numero_cartao", "")
    cvv = evento.get("dados_pagamento", {}).get("cvv", "")

    if valor > 7000:
        return "alto"
    elif numero_cartao.endswith("9999") or cvv == "999":
        return "medio"
    else:
        return "baixo"

# Loop de consumo
print("🛡️ Iniciando serviço de antifraude...")
while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue

    try:
        evento = json.loads(msg.value().decode("utf-8"))
        pagamento_id = evento.get("pagamento_id")
        risco = avaliar_risco(evento)

        evento["risco"] = risco
        evento["timestamp_antifraude"] = datetime.utcnow().isoformat()

        producer.produce(
            topic="antifraude.verificado",
            key=pagamento_id.encode("utf-8"),
            value=json.dumps(evento).encode("utf-8")
        )
        producer.flush()
        print(f"🔍 Pagamento {pagamento_id} → risco {risco}")

    except Exception as e:
        print(f"❌ Erro ao processar mensagem: {e}")
