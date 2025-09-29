import json
from confluent_kafka import Consumer, Producer
from datetime import datetime

# Configuração Kafka
bootstrap_servers = 'localhost:9091,localhost:9092,localhost:9093'

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'processador-pagamento',
    'auto.offset.reset': 'earliest'
}

producer_config = {
    'bootstrap.servers': bootstrap_servers
}

consumer = Consumer(consumer_config)
producer = Producer(producer_config)

consumer.subscribe(['antifraude.verificado'])

# Função de decisão
def decidir_topico(evento):
    risco = evento.get("risco", "baixo")
    if risco == "alto":
        return "pagamento.recusado"
    else:
        return "pagamento.aprovado"

# Loop de consumo
print("💳 Iniciando processador de pagamentos...")
while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue

    try:
        evento = json.loads(msg.value().decode("utf-8"))
        pagamento_id = evento.get("pagamento_id")
        topico_destino = decidir_topico(evento)

        evento["timestamp_processamento"] = datetime.utcnow().isoformat()

        producer.produce(
            topic=topico_destino,
            key=pagamento_id.encode("utf-8"),
            value=json.dumps(evento).encode("utf-8")
        )
        producer.flush()
        print(f"📤 Pagamento {pagamento_id} → {topico_destino}")

    except Exception as e:
        print(f"❌ Erro ao processar mensagem: {e}")
