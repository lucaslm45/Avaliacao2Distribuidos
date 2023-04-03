from confluent_kafka import Producer
import time
import random

p = Producer({'bootstrap.servers':'localhost:9092'})

print('Kafka Producer 1 has been initiated...')
topic1 = "esporte"
topic2 = "clima"
esporte = ["Esporte 1", "Esporte 2", "Esporte 3", "Esporte 4"]
clima = ["Clima 1", "Clima 2", "Clima 3", "Clima 4"]

def main():
    while(1):
        mensagemEsporte = random.choice(esporte)
        mensagemClima = random.choice(clima)

        p.poll(1)
        p.produce(topic1, mensagemEsporte.encode('utf-8'))
        p.flush()

        time.sleep(1/2)
        
        p.poll(1)
        p.produce(topic1, mensagemClima.encode('utf-8'))
        p.flush()

        time.sleep(3)
        
if __name__ == '__main__':
    main()