import json
import uuid
import random
import time
from datetime import datetime
from confluent_kafka import Producer

# Configuração do Kafka
producer_config = {
    'bootstrap.servers': 'localhost:9091,localhost:9092,localhost:9093'
}
producer = Producer(producer_config)

# Callback de entrega
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Erro ao enviar mensagem: {err}")
    else:
        print(f"✅ Evento enviado para {msg.topic()} [partição {msg.partition()}] com chave {msg.key().decode()}")

# Gerador de pagamento aleatório
def gerar_pagamento_aleatorio():
    pagamento_id = str(uuid.uuid4())
    valor = round(random.uniform(10, 6000), 2)  # valores entre R$10 e R$5000
    metodo = random.choice(["cartao_credito", "pix", "boleto"])
    
    # Simular dados inválidos
    numero_cartao = random.choice([
        "4111111111111111",  # válido
        "9999999999999999",  # suspeito
        "1234567890123456"   # inválido
    ])
    cvv = random.choice(["123", "999", "abc"])  # "abc" é inválido
    validade = random.choice(["12/26", "01/20", "13/99"])  # "13/99" é inválido

    evento = {
        "pagamento_id": pagamento_id,
        "id_pedido": f"pedido-{random.randint(1000, 9999)}",
        "id_cliente": f"cliente-{random.randint(100, 999)}",
        "valor": valor,
        "moeda": "BRL",
        "metodo_pagamento": metodo,
        "dados_pagamento": {
            "numero_cartao": numero_cartao,
            "cvv": cvv,
            "validade": validade
        },
        "timestamp": datetime.utcnow().isoformat()
    }

    return pagamento_id, evento

# Loop contínuo de envio
def iniciar_envio_continuo():
    while True:
        pagamento_id, evento = gerar_pagamento_aleatorio()
        producer.produce(
            topic="pagamento.solicitado",
            key=pagamento_id.encode("utf-8"),
            value=json.dumps(evento).encode("utf-8"),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(0.5)

if __name__ == "__main__":
    print("🚀 Iniciando geração contínua de pagamentos...")
    iniciar_envio_continuo()
