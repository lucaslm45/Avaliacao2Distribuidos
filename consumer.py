# Cosumidor 1: consome saude e financas (topico 1 e 2, producer 1)

from confluent_kafka import Consumer

#def commit_completed(err, partitions):
#    if err:
#        print(str(err))
#    else:
#        print("Committed partition offsets: " + str(partitions))

################
config = {'bootstrap.servers':'localhost:9092','group.id':'consumer','auto.offset.reset':'earliest'}
# {'bootstrap.servers': 'localhost:9092',
#         'group.id': "python-consumer1",
#         'enable.auto.commit': True,
#         'auto.offset.reset': 'earliest'}
# #        ,        'on_commit': commit_completed}
        
c = Consumer(config)
print('Tópicos disponíveis para consumo: ', c.list_topics().topics, "\n")

c.subscribe(['saude', 'financas'])

################

def main():
    while True:
        msg=c.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            #print('Error: {}'.format(msg.error()))
            continue
        data=msg.value().decode('utf-8')
        print(data)
    c.close()
        
if __name__ == '__main__':
    main()