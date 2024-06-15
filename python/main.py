import random
import time
from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from datetime import datetime, timezone
import json

# Criando uma instância para nossa variável fake
fake = Faker()

# Definir uma função para gerar as nossas transações de venda de maneira aleatória
def generate_sales_transaction():
    user = fake.simple_profile()

    # Gerando uma transação de venda com dados aleatórios
    return {
        "transactionID": fake.uuid4(),
        "productID": random.choice(['product 1', 'product 2', 'product 3', 'product 4', 'product 5', 'product 6', 'product 7']),
        "productName": random.choice(['notebook', 'celular', 'tablet', 'relogio', 'headphone', 'speaker', 'camisa']),
        "productCategory": random.choice(['eletronicos', 'roupas', 'alimentação', 'casa', 'beleza', 'esportes']),
        "productPrice": round(random.uniform(100, 1000), 2),
        "productQuantity": random.randint(1, 10),
        "productBrand": random.choice(['apple', 'samsung', 'xiaomi', 'microsoft', 'sony']),
        "currency": random.choice(['BRL', 'USD', 'EUR']),
        "customerId": user['username'],  # O simple profile do faker gera um usuário com username e outras informações
        "transactionDate": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": random.choice(['cartao de credito', 'cartao de debito', 'paypal', 'pix', 'dinheiro'])
    }

# Função principal para gerar e enviar transações de vendas para o Kafka
def main():
    topic = 'sales-transactions'  # Definindo o tópico do Kafka
    producer = SerializingProducer({
        'bootstrap.servers': '172.17.0.1:9092',  # Configuração do Kafka Producer
        'key.serializer': StringSerializer('utf_8'),  # Serialização da chave
        'value.serializer': StringSerializer('utf_8')  # Serialização do valor
    })

    while True:
        try:
            # Gerando uma transação de venda aleatória
            transaction = generate_sales_transaction()
            # Calculando o valor total da transação. Para esse case preferi calcular no Spark
            # transaction['totalAmount'] = transaction['productPrice'] * transaction['productQuantity']

            # Enviando a transação para o tópico do Kafka
            producer.produce(
                topic=topic,
                key=transaction['transactionID'],
                value=json.dumps(transaction),
                on_delivery=delivery_report  # Função de callback para relatório de entrega
            )
            producer.poll(0)  # Processando mensagens pendentes

            time.sleep(0.5)  # Pausando por meio segundo antes de gerar a próxima transação

        except BufferError:
            print('Buffer está cheio. Espere alguns instantes...')
            time.sleep(1)

        except Exception as e:
            print(f'Erro: {e}')

# Função para relatório de entrega de mensagens
def delivery_report(err, msg):
    if err is not None:
        print(f"Não foi possível entregar o payload para a gravação {msg.key()}: {err}")
    else:
        print(f'Mensagem entregue com sucesso no tópico {msg.topic()} [{msg.partition()}]')

if __name__ == '__main__':
    main()