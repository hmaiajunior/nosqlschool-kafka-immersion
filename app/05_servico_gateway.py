import json
from confluent_kafka import Consumer, Producer
from datetime import datetime

# Configuração Kafka
bootstrap_servers = 'localhost:9091,localhost:9092,localhost:9093'

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'servico-gateway',
    'auto.offset.reset': 'earliest'
}

producer_config = {
    'bootstrap.servers': bootstrap_servers
}

consumer = Consumer(consumer_config)
producer = Producer(producer_config)

consumer.subscribe(['pagamento.autorizado'])

# Simulação de resposta do gateway
def simular_gateway(evento):
    valor = evento.get("valor", 0)
    numero_cartao = evento.get("dados_pagamento", {}).get("numero_cartao", "")

    if valor > 4000:
        return "falhou", "limite insuficiente"
    elif numero_cartao.endswith("1234"):
        return "falhou", "cartão vencido"
    else:
        return "confirmado", "pagamento aprovado"

# Loop de consumo
print("🔗 Iniciando serviço de gateway de pagamento...")
while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue

    try:
        evento = json.loads(msg.value().decode("utf-8"))
        pagamento_id = evento.get("pagamento_id")
        status, motivo = simular_gateway(evento)

        evento["status_gateway"] = status
        evento["motivo_gateway"] = motivo
        evento["timestamp_gateway"] = datetime.utcnow().isoformat()

        topico_destino = "pagamento.confirmado" if status == "confirmado" else "pagamento.falhou"

        producer.produce(
            topic=topico_destino,
            key=pagamento_id.encode("utf-8"),
            value=json.dumps(evento).encode("utf-8")
        )
        producer.flush()
        print(f"📡 Pagamento {pagamento_id} → {topico_destino} ({motivo})")

    except Exception as e:
        print(f"❌ Erro ao processar mensagem: {e}")
